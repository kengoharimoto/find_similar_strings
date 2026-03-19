# find_similar_strings

Find parallel (similar) passages between two large text files.

Uses chunked multiprocess [`difflib`](https://docs.python.org/3/library/difflib.html) matching over a Unicode letter-only view. Outputs results as JSONL, JSON, SQLite, or CSV.

## Features

- Parallel processing across CPU cores for large files
- Optional letter-only matching (ignores punctuation, spaces, digits)
- Optional [RapidFuzz](https://github.com/rapidfuzz/RapidFuzz) re-scoring for higher-precision filtering
- Chunked comparison with overlap to avoid missing cross-boundary matches
- Progress bar (via tqdm) on stderr, clean output on stdout or a file
- HTML command builder (`gui.html`) — compose commands in a browser, no typing required
- Browser-native implementation (`app.html`) — runs the full matching algorithm in JavaScript, no Python required

## Requirements

- Python 3.8+
- `tqdm`
- `rapidfuzz` *(optional, only needed with `--use-rapidfuzz`)*

```bash
pip install tqdm
pip install rapidfuzz  # optional
```

## Usage

```
usage: find_similar_strings.py [-h] [--version] [-o OUTPUT] [--format {jsonl,json,sqlite,csv}]
                               [-n MIN_LEN] [--max-len MAX_LEN] [-t THRESHOLD] [-j JOBS]
                               [--chunk-size CHUNK_SIZE] [--overlap OVERLAP]
                               [--use-rapidfuzz] [--no-ignore-non-alpha]
                               [--fast] [--max-results MAX_RESULTS] [-q]
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

### Examples

```bash
# Basic usage — JSONL to stdout
python find_similar_strings.py doc1.txt doc2.txt

# Filter results with jq
python find_similar_strings.py doc1.txt doc2.txt | jq 'select(.similarity_filtered > 0.9)'

# Save as JSONL, lower threshold, higher minimum length
python find_similar_strings.py doc1.txt doc2.txt -o matches.jsonl -t 0.7 -n 200

# Save as a SQLite database (queryable with SQL)
python find_similar_strings.py doc1.txt doc2.txt --format sqlite -o matches.db

# Save as a JSON array
python find_similar_strings.py doc1.txt doc2.txt --format json -o matches.json

# Use RapidFuzz for a second pass, 4 workers
python find_similar_strings.py doc1.txt doc2.txt --use-rapidfuzz -j 4

# Include punctuation/digits in matching
python find_similar_strings.py doc1.txt doc2.txt --no-ignore-non-alpha

# Fast mode (autojunk heuristic, may miss some matches)
python find_similar_strings.py doc1.txt doc2.txt --fast

# Quiet mode (no progress bar or summary on stderr)
python find_similar_strings.py doc1.txt doc2.txt -q

# Limit output to top 50 results
python find_similar_strings.py doc1.txt doc2.txt --max-results 50
```

## Output formats

| Format | Description |
|---|---|
| `jsonl` *(default)* | One JSON object per line. Handles multi-line text natively. Pipe-friendly; easy to filter with `jq`. |
| `json` | Pretty-printed JSON array. Good for small result sets or web tooling. |
| `sqlite` | SQLite database with a `matches` table. Queryable with SQL; requires `-o`. |
| `csv` | Comma-separated values. Multi-line text is quoted per RFC 4180. |

### Output fields

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

Open `gui.html` in any browser to compose the command interactively. The command preview updates live as you fill in the form.

## Browser app

Open `app.html` in any browser to run the matching algorithm entirely in JavaScript — no Python or server needed. Drag and drop two files, adjust parameters, and view results inline.

## How it works

1. Both files are decoded as UTF-8 and a character→byte offset map is built.
2. By default, a letter-only view is created (non-alpha characters removed).
3. File1's filtered view is split into overlapping chunks.
4. Each chunk is compared against all of file2 using `difflib.SequenceMatcher`.
5. Matching blocks are merged into regions while the running similarity stays above the threshold.
6. Regions are mapped back from filtered indices → char indices → byte offsets.
7. Optionally re-scored and filtered with RapidFuzz on the raw UTF-8 text.
8. Duplicate regions from overlapping chunks are deduplicated.
9. Results are written in the chosen format, sorted by position in file1.
