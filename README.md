# find_similar_strings

Find parallel (similar) passages between two large text files.

Uses chunked multiprocess [`difflib`](https://docs.python.org/3/library/difflib.html) matching over a Unicode letter-only view. Outputs results as CSV.

## Features

- Parallel processing across CPU cores for large files
- Optional letter-only matching (ignores punctuation, spaces, digits)
- Optional [RapidFuzz](https://github.com/rapidfuzz/RapidFuzz) re-scoring for higher-precision filtering
- Chunked comparison with overlap to avoid missing cross-boundary matches
- Progress bar (via tqdm) on stderr, clean CSV on stdout or a file

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
usage: find_similar_strings.py [-h] [--version] [-o OUTPUT] [-n MIN_LEN]
                               [--max-len MAX_LEN] [-t THRESHOLD] [-j JOBS]
                               [--chunk-size CHUNK_SIZE] [--overlap OVERLAP]
                               [--use-rapidfuzz] [--no-ignore-non-alpha]
                               file1 file2
```

### Arguments

| Argument | Default | Description |
|---|---|---|
| `file1`, `file2` | — | Input files to compare |
| `-o`, `--output` | stdout | Write CSV to this file instead of stdout |
| `-n`, `--min-len` | 100 | Minimum region length in alphabetic characters |
| `--max-len` | 0 (no limit) | Maximum region length in alphabetic characters |
| `-t`, `--threshold` | 0.8 | Similarity threshold (0–1) |
| `-j`, `--jobs` | CPU count | Number of worker processes |
| `--chunk-size` | 500000 | Chunk size in alphabetic characters |
| `--overlap` | max(2×min_len, 100) | Overlap between chunks |
| `--use-rapidfuzz` | off | Re-score with RapidFuzz; reject if below threshold |
| `--no-ignore-non-alpha` | off | Use full text instead of letters only |

### Examples

```bash
# Basic usage — write to stdout
python find_similar_strings.py doc1.txt doc2.txt

# Save to a file, lower threshold, higher minimum length
python find_similar_strings.py doc1.txt doc2.txt -o matches.csv -t 0.7 -n 200

# Use RapidFuzz for a second pass, 4 workers
python find_similar_strings.py doc1.txt doc2.txt --use-rapidfuzz -j 4

# Include punctuation/digits in matching
python find_similar_strings.py doc1.txt doc2.txt --no-ignore-non-alpha
```

## Output CSV columns

| Column | Description |
|---|---|
| `file1`, `file2` | Input file paths |
| `start1`, `end1` | Byte offsets of the match in file1 |
| `start2`, `end2` | Byte offsets of the match in file2 |
| `span_length_bytes` | Length of the longer match in bytes |
| `similarity_filtered` | Similarity score in the filtered (letter-only) view |
| `similarity_rf` | RapidFuzz similarity score (empty if not used) |
| `text1`, `text2` | Matched passage text |

## How it works

1. Both files are decoded as UTF-8 and a character→byte offset map is built.
2. By default, a letter-only view is created (non-alpha characters removed).
3. File1's filtered view is split into overlapping chunks.
4. Each chunk is compared against all of file2 using `difflib.SequenceMatcher`.
5. Matching blocks are merged into regions while the running similarity stays above the threshold.
6. Regions are mapped back from filtered indices → char indices → byte offsets.
7. Optionally re-scored and filtered with RapidFuzz on the raw UTF-8 text.
8. Duplicate regions from overlapping chunks are deduplicated.
9. Results are written as CSV sorted by position in file1.
