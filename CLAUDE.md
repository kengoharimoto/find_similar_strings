# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Install dependencies
pip install tqdm
pip install rapidfuzz  # optional, only for --use-rapidfuzz

# Run the tool
python find_similar_strings.py file1.txt file2.txt

# Run with all options
python find_similar_strings.py file1.txt file2.txt -o out.jsonl -t 0.85 -n 200 -j 4

# Fast mode (autojunk heuristic, may miss some matches)
python find_similar_strings.py file1.txt file2.txt --fast

# Quiet mode (no progress bar or summary on stderr)
python find_similar_strings.py file1.txt file2.txt -q

# Limit output to top 50 results
python find_similar_strings.py file1.txt file2.txt --max-results 50

# SQLite output (requires -o)
python find_similar_strings.py file1.txt file2.txt --format sqlite -o matches.db

# Filter results with jq
python find_similar_strings.py file1.txt file2.txt | jq 'select(.similarity_filtered > 0.9)'
```

There are no tests or linting configured in this project.

## Architecture

The entire tool is a single file: `find_similar_strings.py`. `gui.html` is a standalone browser-based command builder (no server needed). `app.html` is a browser-native implementation that runs the matching algorithm entirely in JavaScript (no Python required).

### Processing pipeline

1. **Main process** reads file1 to determine its length, builds chunk ranges via `make_chunks()`
2. **Worker pool** (`ProcessPoolExecutor`) — each worker calls `init_worker()` which reads both full files into module-level globals (`GLOBAL_*`). Workers are read-only after init.
3. **`process_chunk()`** — each worker processes one chunk of file1's alpha-view against all of file2 using `difflib.SequenceMatcher`. Returns byte-offset tuples.
4. **Main process** collects all results, deduplicates by `(b_start1, b_end1, b_start2, b_end2)` key, sorts, and writes.

### Index space mapping (critical detail)

There are three parallel index spaces maintained for each file:
- **byte offsets** — positions in the raw UTF-8 bytes
- **char indices** — positions in the decoded Python `str`
- **alpha indices** — positions in the letter-only filtered string

Matching happens in alpha-index space. Results are mapped back: alpha → char (via `GLOBAL_ALPHA_TO_CHAR*`) → byte (via `GLOBAL_CHAR_TO_BYTE*`). When `--no-ignore-non-alpha` is used, alpha and char spaces are identical (`IdentityList` — zero-allocation identity mapping).

### Output writers

Four writer classes (`JsonlWriter`, `JsonWriter`, `SqliteWriter`, `CsvWriter`) share a `writerow(record)` / `finalize(records)` interface. JSONL and CSV are streaming; JSON buffers all records for `finalize()`. SQLite batches inserts (1000 rows at a time via `executemany`) and commits in `finalize()`.

### Chunking with overlap

`make_chunks()` yields overlapping ranges to avoid missing matches that cross chunk boundaries. Default overlap = `max(2 * min_len, 100)`. Duplicates from overlap are deduplicated in the main process after collection.
