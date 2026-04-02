# find_similar_strings

Find parallel (similar) passages between two large text files.

Uses chunked multiprocess [`difflib`](https://docs.python.org/3/library/difflib.html) matching over a Unicode letter-only view. Outputs results as JSONL, JSON, SQLite, or CSV.

## Features

- Parallel processing across CPU cores for large files
- Optional letter-only matching (ignores punctuation, spaces, digits)
- Optional [RapidFuzz](https://github.com/rapidfuzz/RapidFuzz) re-scoring for higher-precision filtering
- Chunked comparison with overlap to avoid missing cross-boundary matches
- **Pre-filtering with MinHash LSH** (`--use-lsh`) to skip dissimilar chunk pairs before diffing
- **Pre-filtering with semantic embeddings** (`--use-embeddings`) via sentence-transformers + FAISS to find candidate pairs by meaning, not just character overlap
- Progress bar (via tqdm) on stderr, clean output on stdout or a file
- HTML command builder (`gui.html`) — compose commands in a browser, no typing required
- Browser-native implementation (`app.html`) — runs the full matching algorithm in JavaScript, no Python required
- **Pure semantic comparison** (`find_similar_semantic.py`) — a companion script that matches passages by meaning using embeddings only, no character-level diffing

## Requirements

- Python 3.8+
- `tqdm`

Optional dependencies (install only what you need):

```bash
pip install tqdm

pip install rapidfuzz          # --use-rapidfuzz
pip install datasketch         # --use-lsh
pip install sentence-transformers faiss-cpu   # --use-embeddings, or find_similar_semantic.py
```

## Usage — find_similar_strings.py

```
usage: find_similar_strings.py [-h] [--version] [-o OUTPUT] [--format {jsonl,json,sqlite,csv}]
                               [-n MIN_LEN] [--max-len MAX_LEN] [-t THRESHOLD] [-j JOBS]
                               [--chunk-size CHUNK_SIZE] [--overlap OVERLAP]
                               [--use-rapidfuzz] [--no-ignore-non-alpha]
                               [--fast] [--max-results MAX_RESULTS] [-q]
                               [--use-lsh] [--lsh-threshold N] [--lsh-num-perm N] [--lsh-ngram-size N]
                               [--use-embeddings] [--emb-model NAME] [--emb-top-k N]
                               file1 file2
```

### Arguments

| Argument | Default | Description |
|---|---|---|
| `file1`, `file2` | — | Input files to compare |
| `-o`, `--output` | stdout | Write output to this file (required for `--format sqlite`) |
| `--format` | `jsonl` | Output format: `jsonl`, `json`, `sqlite`, or `csv` |
| `-n`, `--min-len` | 100 | Minimum region length in alphabetic characters |
| `--max-len` | 0 (no limit) | Maximum region length in alphabetic characters |
| `-t`, `--threshold` | 0.8 | Similarity threshold (0–1) |
| `-j`, `--jobs` | CPU count | Number of worker processes |
| `--chunk-size` | 500000 | Chunk size in alphabetic characters |
| `--overlap` | max(2×min_len, 100) | Overlap between chunks |
| `--use-rapidfuzz` | off | Re-score with RapidFuzz; reject if below threshold |
| `--no-ignore-non-alpha` | off | Use full text instead of letters only |
| `--fast` | off | Enable difflib autojunk heuristic for faster matching (may miss some matches) |
| `--max-results` | 0 (no limit) | Maximum number of results to output |
| `-q`, `--quiet` | off | Suppress progress bar and summary output on stderr |

#### Pre-filtering flags

Pre-filtering reduces the number of chunk pairs passed to `SequenceMatcher`. Both methods can be combined; their candidate sets are unioned.

| Argument | Default | Description |
|---|---|---|
| `--use-lsh` | off | Pre-filter with MinHash + LSH (requires `datasketch`) |
| `--lsh-threshold` | 0.3 | Jaccard similarity threshold for LSH candidate selection |
| `--lsh-num-perm` | 128 | Number of MinHash permutations (higher = more accurate, slower) |
| `--lsh-ngram-size` | 3 | Character n-gram size for MinHash shingles |
| `--use-embeddings` | off | Pre-filter with sentence-transformers + FAISS (requires `sentence-transformers faiss-cpu`) |
| `--emb-model` | `paraphrase-multilingual-MiniLM-L12-v2` | Sentence-transformers model name |
| `--emb-top-k` | 10 | Nearest file2 chunks to retrieve per file1 chunk |

### Examples

```bash
# Basic usage — JSONL to stdout
python3 find_similar_strings.py doc1.txt doc2.txt

# Filter results with jq
python3 find_similar_strings.py doc1.txt doc2.txt | jq 'select(.similarity_filtered > 0.9)'

# Save as JSONL, lower threshold, higher minimum length
python3 find_similar_strings.py doc1.txt doc2.txt -o matches.jsonl -t 0.7 -n 200

# Save as a SQLite database (queryable with SQL)
python3 find_similar_strings.py doc1.txt doc2.txt --format sqlite -o matches.db

# Use RapidFuzz for a second pass, 4 workers
python3 find_similar_strings.py doc1.txt doc2.txt --use-rapidfuzz -j 4

# Pre-filter with LSH (fast, lexical — good for large files with sparse matches)
python3 find_similar_strings.py doc1.txt doc2.txt --use-lsh

# Pre-filter with embeddings (semantic — finds paraphrases and translations)
python3 find_similar_strings.py doc1.txt doc2.txt --use-embeddings

# Combine both pre-filters
python3 find_similar_strings.py doc1.txt doc2.txt --use-lsh --use-embeddings

# Quiet mode (no progress bar or summary on stderr)
python3 find_similar_strings.py doc1.txt doc2.txt -q

# Limit output to top 50 results
python3 find_similar_strings.py doc1.txt doc2.txt --max-results 50
```

## Usage — find_similar_semantic.py

A companion script that matches passages **purely by meaning** using sentence-transformer embeddings and FAISS cosine search. No character-level diffing — finds paraphrases and semantic parallels that `find_similar_strings.py` would miss.

Requires: `pip install sentence-transformers faiss-cpu`

```
usage: find_similar_semantic.py [-h] [--version] [-o OUTPUT]
                                [-t THRESHOLD] [--top-k N]
                                [--chunk-size N] [--overlap N] [--min-chunk N]
                                [--model NAME] [--max-results N] [-q]
                                file1 file2
```

| Argument | Default | Description |
|---|---|---|
| `file1`, `file2` | — | Input files to compare |
| `-o`, `--output` | stdout | Output JSONL file |
| `-t`, `--threshold` | 0.7 | Cosine similarity threshold (0–1) |
| `--top-k` | 5 | Nearest file2 chunks to retrieve per file1 chunk |
| `--chunk-size` | 1000 | Target chunk size in characters |
| `--overlap` | 200 | Overlap between chunks in characters |
| `--min-chunk` | 50 | Minimum chunk size; shorter chunks are dropped |
| `--model` | `paraphrase-multilingual-MiniLM-L12-v2` | Sentence-transformers model |
| `--max-results` | 0 (no limit) | Maximum number of results to output |
| `-q`, `--quiet` | off | Suppress progress output |

Output fields: `file1`, `start1`, `end1`, `file2`, `start2`, `end2`, `similarity` (cosine), `text1`, `text2`.

```bash
# Basic usage
python3 find_similar_semantic.py doc1.txt doc2.txt

# Lower threshold to catch more paraphrases
python3 find_similar_semantic.py doc1.txt doc2.txt -t 0.6 --top-k 10

# Save to file
python3 find_similar_semantic.py doc1.txt doc2.txt -o matches.jsonl
```

## Output formats (find_similar_strings.py)

| Format | Description |
|---|---|
| `jsonl` *(default)* | One JSON object per line. Handles multi-line text natively. Pipe-friendly; easy to filter with `jq`. |
| `json` | Pretty-printed JSON array. Good for small result sets or web tooling. |
| `sqlite` | SQLite database with a `matches` table. Queryable with SQL; requires `-o`. |
| `csv` | Comma-separated values. Multi-line text is quoted per RFC 4180. |

### Output fields (find_similar_strings.py)

| Field | Description |
|---|---|
| `file1`, `file2` | Input file paths |
| `start1`, `end1` | Byte offsets of the match in file1 |
| `start2`, `end2` | Byte offsets of the match in file2 |
| `span_length_bytes` | Length of the longer match in bytes |
| `similarity_filtered` | Similarity score in the filtered (letter-only) view |
| `similarity_rf` | RapidFuzz similarity score (`null` / empty if not used) |
| `text1`, `text2` | Matched passage text |

## GUI

Open `gui.html` in any browser to compose the command interactively. Includes controls for all flags including the new pre-filtering options. The command preview updates live as you fill in the form.

## Browser app

Open `app.html` in any browser to run the matching algorithm entirely in JavaScript — no Python or server needed. Drag and drop two files, adjust parameters, and view results inline.

## How it works — find_similar_strings.py

1. Both files are decoded as UTF-8 and a character→byte offset map is built.
2. By default, a letter-only view is created (non-alpha characters removed).
3. File1's filtered view is split into overlapping chunks; same for file2.
4. *(Optional)* Candidate chunk pairs are pre-filtered via MinHash LSH and/or embedding cosine search, reducing the number of pairs from O(chunks₁ × chunks₂) to only likely matches.
5. Each candidate chunk pair is compared using `difflib.SequenceMatcher`.
6. Matching blocks are merged into regions while the running similarity stays above the threshold.
7. Regions are mapped back from filtered indices → char indices → byte offsets.
8. Optionally re-scored and filtered with RapidFuzz on the raw UTF-8 text.
9. Duplicate regions from overlapping chunks are deduplicated.
10. Results are written in the chosen format, sorted by position in file1.
