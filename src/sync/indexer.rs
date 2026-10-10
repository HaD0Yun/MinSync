//! Batch indexing: read, normalize, chunk and derive doc IDs for a group of
//! files, embed every missing chunk of the group in one call, and write the
//! whole group to the store with one commit per kind of write.
//!
//! Per batch of N files the store sees exactly: one `existing_ids`, one
//! `update` (reused chunks), one `insert_new` (new chunks), one stale-chunk
//! sweep, and one delete for removed files. The cost of a LanceDB write is
//! dominated by the commit, not the rows, so N files cost about what one did.

use crate::chunker::Chunker;
use crate::config::Config;
use crate::embedder::Embedder;
use crate::error::{MinSyncError, Result};
use crate::id::doc_ids_for_chunks;
use crate::normalize::normalize_text;
use crate::types::{Chunk, SyncResult};
use crate::vectorstore::{Document, DocumentUpdate, Filter, VectorStore};
use std::collections::HashSet;
use std::path::Path;

pub(super) struct SyncFileContext<'a> {
    pub config: &'a Config,
    pub chunker: &'a dyn Chunker,
    pub embedder: &'a dyn Embedder,
    pub store: &'a mut dyn VectorStore,
    pub sync_token: &'a str,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum BatchKind {
    /// Added or modified file: (re)index its chunks.
    Index,
    /// File removed from the tree: drop all of its chunks.
    Delete,
}

pub(super) struct BatchItem {
    pub path: String,
    pub kind: BatchKind,
    /// A resumed run already committed this path with older content. Its
    /// rows carry the reused sync token, so the stale sweep cannot tell them
    /// from fresh ones: clear the path before indexing.
    pub purge_first: bool,
}

struct PreparedFile<'a> {
    path: &'a str,
    chunks: Vec<(Chunk, String, String)>,
}

fn path_filter(source_id: &str, paths: Vec<String>) -> Filter {
    Filter::And(vec![
        Filter::Eq("source_id".to_string(), source_id.to_string()),
        Filter::In("path".to_string(), paths),
    ])
}

/// Index/delete `items` as one unit. On success every item is fully
/// committed; on error nothing is recorded as processed in `result`.
pub(super) async fn index_batch(
    root: &Path,
    items: &[BatchItem],
    context: SyncFileContext<'_>,
    result: &mut SyncResult,
) -> Result<()> {
    let source_id = context.config.source_id.as_str();
    let schema_id = context.chunker.schema_id();

    let purge: Vec<String> = items
        .iter()
        .filter(|item| item.purge_first)
        .map(|item| item.path.clone())
        .collect();
    let mut chunks_deleted = 0;
    if !purge.is_empty() {
        chunks_deleted += context
            .store
            .delete_by_filter(&path_filter(source_id, purge))?;
    }

    // Read + chunk + id every file; files that vanished since the scan are
    // handled like deletions.
    let mut delete_paths: Vec<String> = Vec::new();
    let mut prepared: Vec<PreparedFile<'_>> = Vec::new();
    for item in items {
        if item.kind == BatchKind::Delete {
            delete_paths.push(item.path.clone());
            continue;
        }
        let raw_text = match std::fs::read_to_string(root.join(&item.path)) {
            Ok(text) => text,
            Err(error) if error.kind() == std::io::ErrorKind::InvalidData => String::new(),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                delete_paths.push(item.path.clone());
                continue;
            }
            Err(error) => return Err(MinSyncError::Io(error)),
        };
        let text = normalize_text(&raw_text, &context.config.normalize);
        let chunks = context.chunker.chunk(&text, &item.path)?;
        let ids = doc_ids_for_chunks(source_id, &item.path, schema_id, &chunks);
        prepared.push(PreparedFile {
            path: &item.path,
            chunks: chunks
                .into_iter()
                .zip(ids)
                .map(|(chunk, (doc_id, content_hash))| (chunk, doc_id, content_hash))
                .collect(),
        });
    }

    // One id-only existence check for the whole batch.
    let all_ids: Vec<String> = prepared
        .iter()
        .flat_map(|file| file.chunks.iter().map(|(_, doc_id, _)| doc_id.clone()))
        .collect();
    let existing: HashSet<String> = context.store.existing_ids(&all_ids)?;

    let mut reused: Vec<DocumentUpdate> = Vec::new();
    let mut docs_to_embed: Vec<Document> = Vec::new();
    // Position of each new chunk within its own file, for error messages.
    let mut chunk_in_file: Vec<usize> = Vec::new();
    for file in prepared {
        let mut new_in_file = 0;
        for (chunk, doc_id, content_hash) in file.chunks {
            if existing.contains(&doc_id) {
                reused.push(DocumentUpdate {
                    id: doc_id,
                    seen_token: context.sync_token.to_string(),
                    path: file.path.to_string(),
                    heading_path: chunk.heading_path,
                });
            } else {
                docs_to_embed.push(Document {
                    id: doc_id,
                    embedding: Vec::new(),
                    text: chunk.text,
                    source_id: source_id.to_string(),
                    path: file.path.to_string(),
                    chunk_schema_id: schema_id.to_string(),
                    chunk_type: chunk.chunk_type,
                    heading_path: chunk.heading_path,
                    content_hash,
                    seen_token: context.sync_token.to_string(),
                });
                chunk_in_file.push(new_in_file);
                new_in_file += 1;
            }
        }
    }

    // Embed first: the slow, fallible step runs before any write, so a
    // failure leaves the store untouched by this batch.
    //
    // Identical chunk text (same content hash) embeds once, and vectors the
    // store already holds for that hash are reused. The rest go to the
    // embedder longest first so consecutive batches pad far less.
    if !docs_to_embed.is_empty() {
        let mut seen_hashes: HashSet<&str> = HashSet::new();
        let mut unique: Vec<usize> = Vec::new();
        for (index, doc) in docs_to_embed.iter().enumerate() {
            if seen_hashes.insert(doc.content_hash.as_str()) {
                unique.push(index);
            }
        }
        let hashes: Vec<String> = unique
            .iter()
            .map(|&index| docs_to_embed[index].content_hash.clone())
            .collect();
        let mut vectors = context.store.fetch_embeddings_by_content_hash(&hashes)?;

        let mut to_embed: Vec<usize> = unique
            .into_iter()
            .filter(|&index| !vectors.contains_key(&docs_to_embed[index].content_hash))
            .collect();
        to_embed.sort_by_key(|&index| std::cmp::Reverse(docs_to_embed[index].text.len()));

        if !to_embed.is_empty() {
            let texts: Vec<String> = to_embed
                .iter()
                .map(|&index| docs_to_embed[index].text.clone())
                .collect();
            let mut embeddings = context.embedder.embed(&texts).await?;
            if embeddings.len() != texts.len() {
                return Err(MinSyncError::Embedding(format!(
                    "expected {} embeddings, got {}",
                    texts.len(),
                    embeddings.len()
                )));
            }
            let located: Vec<(&Document, usize)> = to_embed
                .iter()
                .map(|&index| (&docs_to_embed[index], chunk_in_file[index]))
                .collect();
            repair_non_finite_embeddings(
                context.embedder,
                &texts,
                &mut embeddings,
                context.config.embedder.max_retries,
                &located,
            )
            .await?;

            result.embedding_api_calls += 1;
            result.embedded_texts += texts.len();
            result.estimated_tokens += texts
                .iter()
                .map(|text| estimate_tokens(text))
                .sum::<usize>();
            for (&index, embedding) in to_embed.iter().zip(embeddings) {
                vectors.insert(docs_to_embed[index].content_hash.clone(), embedding);
            }
        }

        result.embeddings_reused += docs_to_embed.len() - to_embed.len();
        for (index, doc) in docs_to_embed.iter_mut().enumerate() {
            doc.embedding = vectors.get(&doc.content_hash).cloned().ok_or_else(|| {
                MinSyncError::Embedding(format!(
                    "missing embedding for {} (chunk {})",
                    doc.path, chunk_in_file[index]
                ))
            })?;
        }
    }

    if !reused.is_empty() {
        context.store.update(&reused)?;
        result.chunks_updated += reused.len();
    }
    if !docs_to_embed.is_empty() {
        context.store.insert_new(&docs_to_embed)?;
        result.chunks_added += docs_to_embed.len();
    }

    // Stale chunks of every indexed file (content changed: old ids no longer
    // carry this run's token) in one sweep, then removed files in one delete.
    let indexed_paths: Vec<String> = items
        .iter()
        .filter(|item| item.kind == BatchKind::Index && !delete_paths.contains(&item.path))
        .map(|item| item.path.clone())
        .collect();
    if !indexed_paths.is_empty() {
        chunks_deleted += context.store.delete_by_filter(&Filter::And(vec![
            Filter::Eq("source_id".to_string(), source_id.to_string()),
            Filter::In("path".to_string(), indexed_paths),
            Filter::Neq("seen_token".to_string(), context.sync_token.to_string()),
        ]))?;
    }
    if !delete_paths.is_empty() {
        chunks_deleted += context
            .store
            .delete_by_filter(&path_filter(source_id, delete_paths))?;
    }

    result.chunks_deleted += chunks_deleted;
    result.files_processed += items.len();
    result
        .files_processed_paths
        .extend(items.iter().map(|item| item.path.clone()));
    Ok(())
}

fn is_finite_embedding(embedding: &[f32]) -> bool {
    embedding.iter().all(|value| value.is_finite())
}

/// Re-embed chunks whose vectors contain NaN/inf, one text per request.
///
/// Local GPU backends can emit non-finite values for a single chunk inside a
/// large batch without the input itself being invalid, so re-embedding it
/// alone usually yields a valid vector. Without this, one bad vector aborts
/// the whole sync at write time. Chunks that stay non-finite after
/// `max_retries` single-text attempts fail with the file path and chunk index
/// so the offending file can be inspected or excluded.
async fn repair_non_finite_embeddings(
    embedder: &dyn Embedder,
    texts: &[String],
    embeddings: &mut [Vec<f32>],
    max_retries: usize,
    located: &[(&Document, usize)],
) -> Result<()> {
    for index in 0..embeddings.len() {
        if is_finite_embedding(&embeddings[index]) {
            continue;
        }
        let (doc, chunk) = located[index];
        let path = doc.path.as_str();
        let mut attempts = 0;
        while attempts < max_retries.max(1) && !is_finite_embedding(&embeddings[index]) {
            attempts += 1;
            tracing::warn!(
                path,
                chunk_index = chunk,
                attempt = attempts,
                "embedding contains non-finite values; re-embedding chunk alone"
            );
            embeddings[index] = embedder.embed_single(&texts[index]).await?;
        }
        if !is_finite_embedding(&embeddings[index]) {
            return Err(MinSyncError::Embedding(format!(
                "embedding for {path} (chunk {chunk}, document {}) contains non-finite values after {attempts} single-chunk retries",
                doc.id
            )));
        }
    }
    Ok(())
}

fn estimate_tokens(text: &str) -> usize {
    let ascii_count = text.chars().filter(|char| char.is_ascii()).count();
    let non_ascii_count = text.chars().count() - ascii_count;
    ascii_count / 4 + non_ascii_count
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_estimate_tokens_ascii() {
        assert_eq!(estimate_tokens("abcdefgh"), 2);
    }

    #[test]
    fn test_estimate_tokens_korean() {
        let text = "안녕하세요";
        assert_eq!(estimate_tokens(text), text.chars().count());
    }

    #[test]
    fn test_estimate_tokens_mixed_ascii_and_non_ascii() {
        assert_eq!(estimate_tokens("abcdefgh한글"), 4);
    }
}
