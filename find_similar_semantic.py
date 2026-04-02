#!/usr/bin/env python3
"""
find_similar_semantic.py

Find semantically similar passages between two text files using
sentence-transformer embeddings and FAISS cosine search.
Pure semantic matching — no character-level diffing.

Requirements:
    pip install sentence-transformers faiss-cpu numpy tqdm
"""

import argparse
import json
import re
import sys
from typing import List, Tuple

import numpy as np
from tqdm import tqdm

__version__ = "0.1.0"

DEFAULT_MODEL = "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2"


# ---------------------------------------------------------------------------
# Chunking
# ---------------------------------------------------------------------------

def make_chunks(text: str, chunk_size: int, overlap: int, min_chunk: int) -> List[Tuple[int, int]]:
    """
    Split text into overlapping chunks aligned to word boundaries.

    Returns a list of (char_start, char_end) ranges.
    chunk_size and overlap are in characters.
    """
    # Find all word positions
    word_ends = [m.end() for m in re.finditer(r'\S+\s*', text)]
    if not word_ends:
        return []

    # Map character positions to word indices for fast lookup
    # Build chunks by accumulating words until chunk_size is reached
    chunks: List[Tuple[int, int]] = []
    n = len(word_ends)
    i = 0

    while i < n:
        start = word_ends[i - 1] if i > 0 else 0
        # Walk forward until we've accumulated chunk_size characters
        j = i
        while j < n and word_ends[j] - start < chunk_size:
            j += 1
        end = word_ends[j - 1] if j > i else word_ends[min(j, n - 1)]

        if end - start >= min_chunk:
            chunks.append((start, end))

        # Advance by (chunk_size - overlap) characters
        step = max(1, chunk_size - overlap)
        target = start + step
        while i < n and (word_ends[i - 1] if i > 0 else 0) < target:
            i += 1

    return chunks


# ---------------------------------------------------------------------------
# Embedding + search
# ---------------------------------------------------------------------------

def embed(texts: List[str], model, quiet: bool) -> np.ndarray:
    return model.encode(
        texts,
        normalize_embeddings=True,
        show_progress_bar=not quiet,
        convert_to_numpy=True,
    ).astype(np.float32)


def find_candidates(
    embs1: np.ndarray,
    embs2: np.ndarray,
    top_k: int,
    threshold: float,
) -> List[Tuple[int, int, float]]:
    """
    Return (i, j, score) triples where score >= threshold.
    Uses FAISS IndexFlatIP (inner product = cosine on unit vectors).
    """
    import faiss

    dim = embs2.shape[1]
    index = faiss.IndexFlatIP(dim)
    index.add(embs2)

    k = min(top_k, len(embs2))
    D, I = index.search(embs1, k)

    results = []
    for i in range(len(embs1)):
        for rank in range(k):
            score = float(D[i, rank])
            j = int(I[i, rank])
            if j >= 0 and score >= threshold:
                results.append((i, j, score))
    return results


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description=(
            "Find semantically similar passages between two text files "
            "using sentence-transformer embeddings and FAISS ANN search."
        )
    )
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    parser.add_argument("file1", help="First input file")
    parser.add_argument("file2", help="Second input file")
    parser.add_argument("-o", "--output", default=None, help="Output JSONL file (default: stdout)")
    parser.add_argument(
        "-t", "--threshold", type=float, default=0.7,
        help="Cosine similarity threshold 0–1 (default: 0.7).",
    )
    parser.add_argument(
        "--top-k", type=int, default=5,
        help="Nearest neighbors to retrieve per file1 chunk (default: 5).",
    )
    parser.add_argument(
        "--chunk-size", type=int, default=1000,
        help="Target chunk size in characters (default: 1000).",
    )
    parser.add_argument(
        "--overlap", type=int, default=200,
        help="Overlap between consecutive chunks in characters (default: 200).",
    )
    parser.add_argument(
        "--min-chunk", type=int, default=50,
        help="Minimum chunk size in characters; shorter chunks are dropped (default: 50).",
    )
    parser.add_argument(
        "--model", default=DEFAULT_MODEL,
        help=f"Sentence-transformers model name (default: {DEFAULT_MODEL}).",
    )
    parser.add_argument(
        "--max-results", type=int, default=0,
        help="Maximum number of results to output, 0 = no limit (default: 0).",
    )
    parser.add_argument("-q", "--quiet", action="store_true", help="Suppress progress output.")

    if len(sys.argv) == 1:
        parser.print_help(sys.stderr)
        sys.exit(1)

    args = parser.parse_args()

    if not (0.0 < args.threshold <= 1.0):
        parser.error("--threshold must be between 0 (exclusive) and 1 (inclusive).")
    if args.chunk_size <= 0:
        parser.error("--chunk-size must be positive.")
    if args.overlap < 0:
        parser.error("--overlap must be >= 0.")
    if args.overlap >= args.chunk_size:
        parser.error("--overlap must be less than --chunk-size.")

    # Read files
    try:
        text1 = open(args.file1, encoding="utf-8", errors="replace").read()
        text2 = open(args.file2, encoding="utf-8", errors="replace").read()
    except OSError as e:
        parser.error(str(e))

    # Chunk
    chunks1 = make_chunks(text1, args.chunk_size, args.overlap, args.min_chunk)
    chunks2 = make_chunks(text2, args.chunk_size, args.overlap, args.min_chunk)

    if not chunks1 or not chunks2:
        if not args.quiet:
            print("Warning: one or both files produced no chunks.", file=sys.stderr)
        sys.exit(0)

    if not args.quiet:
        print(
            f"Chunks: {len(chunks1)} from file1, {len(chunks2)} from file2.",
            file=sys.stderr,
        )

    # Prevent PyTorch semaphore leaks that cause a segfault at shutdown on macOS.
    import os
    os.environ["TOKENIZERS_PARALLELISM"] = "false"
    os.environ.setdefault("OMP_NUM_THREADS", "1")

    # Load model
    try:
        from sentence_transformers import SentenceTransformer
    except ImportError:
        print("ERROR: pip install sentence-transformers", file=sys.stderr)
        sys.exit(1)

    try:
        import torch
        torch.multiprocessing.set_sharing_strategy("file_system")
    except (ImportError, RuntimeError):
        pass

    if not args.quiet:
        print(f"Loading model '{args.model}' (may download on first use)...", file=sys.stderr)
    model = SentenceTransformer(args.model)

    # Embed
    if not args.quiet:
        print("Embedding file1 chunks...", file=sys.stderr)
    embs1 = embed([text1[s:e] for s, e in chunks1], model, args.quiet)

    if not args.quiet:
        print("Embedding file2 chunks...", file=sys.stderr)
    embs2 = embed([text2[s:e] for s, e in chunks2], model, args.quiet)

    # Search
    if not args.quiet:
        print("Searching for similar pairs...", file=sys.stderr)
    try:
        import faiss  # noqa: F401
    except ImportError:
        print("ERROR: pip install faiss-cpu", file=sys.stderr)
        sys.exit(1)

    candidates = find_candidates(embs1, embs2, args.top_k, args.threshold)

    # Deduplicate by (i, j), keep highest score
    best: dict = {}
    for i, j, score in candidates:
        if (i, j) not in best or score > best[(i, j)]:
            best[(i, j)] = score

    results = sorted(best.items(), key=lambda kv: -kv[1])  # sort by score desc

    if args.max_results > 0:
        results = results[:args.max_results]

    # Write output
    out = open(args.output, "w", encoding="utf-8") if args.output else sys.stdout
    try:
        for (i, j), score in results:
            s1, e1 = chunks1[i]
            s2, e2 = chunks2[j]
            record = {
                "file1": args.file1, "start1": s1, "end1": e1,
                "file2": args.file2, "start2": s2, "end2": e2,
                "similarity": round(score, 5),
                "text1": text1[s1:e1],
                "text2": text2[s2:e2],
            }
            out.write(json.dumps(record, ensure_ascii=False) + "\n")
    finally:
        if args.output:
            out.close()

    if not args.quiet:
        dest = repr(args.output) if args.output else "stdout"
        print(f"Done. {len(results)} pair(s) written to {dest}.", file=sys.stderr)


if __name__ == "__main__":
    main()
