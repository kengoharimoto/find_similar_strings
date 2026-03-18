#!/usr/bin/env python3
"""
find_similar_strings.py

Find parallel (similar) passages between two large text files using
chunked multiprocess difflib matching over a Unicode letter-only view.
Outputs results as JSONL (default), JSON, SQLite, or CSV.
"""

import argparse
import csv
import difflib
import json
import os
import sqlite3
import sys
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from typing import Dict, List, Optional, Tuple

from tqdm import tqdm

__version__ = "1.2.0"

# ---------------------------------------------------------------------------
# Per-worker globals (populated by init_worker, read-only in process_chunk)
# ---------------------------------------------------------------------------
GLOBAL_DATA1: Optional[bytes] = None
GLOBAL_DATA2: Optional[bytes] = None
GLOBAL_TEXT1: Optional[str] = None
GLOBAL_TEXT2: Optional[str] = None
GLOBAL_CHAR_TO_BYTE1: Optional[List[int]] = None
GLOBAL_CHAR_TO_BYTE2: Optional[List[int]] = None
GLOBAL_ALPHA_TEXT1: Optional[str] = None
GLOBAL_ALPHA_TEXT2: Optional[str] = None
GLOBAL_ALPHA_TO_CHAR1 = None  # List[int] or IdentityList
GLOBAL_ALPHA_TO_CHAR2 = None  # List[int] or IdentityList
GLOBAL_USE_RAPIDFUZZ: bool = False
GLOBAL_THRESHOLD: float = 0.8
GLOBAL_MIN_LEN: int = 100
GLOBAL_MAX_LEN: int = 0
GLOBAL_IGNORE_NON_ALPHA: bool = True
GLOBAL_AUTOJUNK: bool = False

FIELDS = [
    "file1", "start1", "end1",
    "file2", "start2", "end2",
    "span_length_bytes",
    "similarity_filtered",
    "similarity_rf",
    "text1", "text2",
]


# ---------------------------------------------------------------------------
# Zero-allocation identity list (used when --no-ignore-non-alpha)
# ---------------------------------------------------------------------------


class IdentityList:
    """Acts like list(range(n)) but uses no memory for storage."""

    __slots__ = ("_n",)

    def __init__(self, n: int):
        self._n = n

    def __getitem__(self, i: int) -> int:
        if i < 0:
            i += self._n
        if 0 <= i < self._n:
            return i
        raise IndexError(i)

    def __len__(self) -> int:
        return self._n


# ---------------------------------------------------------------------------
# Utility functions
# ---------------------------------------------------------------------------


def build_char_to_byte_map(text: str) -> List[int]:
    """Map each character index in `text` to its byte offset in UTF-8 encoding.

    Uses ord() to determine UTF-8 byte length per code point, avoiding
    per-character bytes object allocation.
    """
    mapping: List[int] = []
    offset = 0
    for ch in text:
        mapping.append(offset)
        cp = ord(ch)
        if cp < 0x80:
            offset += 1
        elif cp < 0x800:
            offset += 2
        elif cp < 0x10000:
            offset += 3
        else:
            offset += 4
    return mapping


def build_alpha_view(text: str) -> Tuple[str, List[int]]:
    """
    Build a letter-only (Unicode isalpha) view of `text`.

    Returns:
        alpha_text: string containing only alphabetic characters
        alpha_to_char: list mapping alpha index -> original char index in `text`
    """
    alpha_chars: List[str] = []
    alpha_to_char: List[int] = []
    for idx, ch in enumerate(text):
        if ch.isalpha():
            alpha_chars.append(ch)
            alpha_to_char.append(idx)
    return "".join(alpha_chars), alpha_to_char


def count_alpha(text: str) -> int:
    """Count alphabetic characters without building the full alpha string."""
    return sum(1 for ch in text if ch.isalpha())


def make_record(
    path1: str, b_start1: int, b_end1: int,
    path2: str, b_start2: int, b_end2: int,
    span_len_bytes: int,
    sim_filt: float, sim_rf: Optional[float],
    text1: str, text2: str,
) -> Dict:
    return {
        "file1": path1, "start1": b_start1, "end1": b_end1,
        "file2": path2, "start2": b_start2, "end2": b_end2,
        "span_length_bytes": span_len_bytes,
        "similarity_filtered": round(sim_filt, 5),
        "similarity_rf": round(sim_rf, 5) if sim_rf is not None else None,
        "text1": text1,
        "text2": text2,
    }


# ---------------------------------------------------------------------------
# Output writers
# ---------------------------------------------------------------------------


class JsonlWriter:
    def __init__(self, stream):
        self.stream = stream

    def writerow(self, record: Dict):
        self.stream.write(json.dumps(record, ensure_ascii=False) + "\n")

    def finalize(self, records):
        pass  # streaming — already written


class JsonWriter:
    def __init__(self, stream):
        self.stream = stream

    def writerow(self, record: Dict):
        pass  # buffered — collected in main

    def finalize(self, records):
        json.dump(records, self.stream, ensure_ascii=False, indent=2)
        self.stream.write("\n")


class SqliteWriter:
    def __init__(self, path: str):
        self.conn = sqlite3.connect(path)
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS matches (
                file1              TEXT,
                start1             INTEGER,
                end1               INTEGER,
                file2              TEXT,
                start2             INTEGER,
                end2               INTEGER,
                span_length_bytes  INTEGER,
                similarity_filtered REAL,
                similarity_rf      REAL,
                text1              TEXT,
                text2              TEXT
            )
            """
        )
        self.conn.commit()
        self._buffer: List[list] = []

    def writerow(self, record: Dict):
        self._buffer.append([record[f] for f in FIELDS])
        if len(self._buffer) >= 1000:
            self._flush()

    def _flush(self):
        if self._buffer:
            self.conn.executemany(
                "INSERT INTO matches VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                self._buffer,
            )
            self._buffer.clear()

    def finalize(self, records):
        self._flush()
        self.conn.commit()
        self.conn.close()


class CsvWriter:
    def __init__(self, stream):
        self.writer = csv.writer(stream)
        self.writer.writerow(FIELDS)

    def writerow(self, record: Dict):
        self.writer.writerow([record[f] for f in FIELDS])

    def finalize(self, records):
        pass


# ---------------------------------------------------------------------------
# Core matching logic
# ---------------------------------------------------------------------------


def find_parallel_regions_local(
    data1: str,
    data2: str,
    threshold: float,
    min_len: int,
    max_len: int,
    autojunk: bool = False,
) -> List[Tuple[int, int, int, int, float]]:
    """
    Find parallel regions between two string sequences using difflib.

    Returns a list of (a_start, a_end, b_start, b_end, similarity) tuples
    where indices are positions in the filtered strings and
    similarity = matched_chars / max(a_span, b_span).

    When autojunk=True (--fast mode), difflib's heuristic marks frequently
    occurring elements as junk, which speeds up matching but may miss some
    regions containing common character patterns.
    """
    sm = difflib.SequenceMatcher(None, data1, data2, autojunk=autojunk)
    blocks = sm.get_matching_blocks()

    regions: List[Tuple[int, int, int, int, float]] = []
    a_start = b_start = a_end = b_end = None
    matched_units = 0

    def flush_region():
        nonlocal a_start, a_end, b_start, b_end, matched_units
        if a_start is None:
            return
        span_len = max(a_end - a_start, b_end - b_start)
        if span_len > 0:
            similarity = matched_units / span_len
            if (
                span_len >= min_len
                and (max_len == 0 or span_len <= max_len)
                and similarity >= threshold
            ):
                regions.append((a_start, a_end, b_start, b_end, similarity))
        a_start = b_start = a_end = b_end = None
        matched_units = 0

    for block in blocks:
        i, j, size = block.a, block.b, block.size

        if size == 0:
            flush_region()
            break

        if a_start is None:
            a_start, b_start = i, j
            a_end, b_end = i + size, j + size
            matched_units = size
            continue

        new_a_end = i + size
        new_b_end = j + size
        span_len = max(new_a_end - a_start, new_b_end - b_start)
        proposed_similarity = (matched_units + size) / span_len

        if proposed_similarity >= threshold:
            matched_units += size
            a_end, b_end = new_a_end, new_b_end
        else:
            flush_region()
            a_start, b_start = i, j
            a_end, b_end = i + size, j + size
            matched_units = size

    return regions


def process_chunk(args):
    """
    Worker function for a single chunk of file1 in alpha-index space.

    Returns a list of region tuples in byte-offset space:
        (byte_start1, byte_end1, byte_start2, byte_end2,
         similarity_filtered, similarity_rf_or_None)
    """
    alpha_chunk_start, alpha_chunk_end = args

    slice1 = GLOBAL_ALPHA_TEXT1[alpha_chunk_start:alpha_chunk_end]

    local_regions = find_parallel_regions_local(
        slice1,
        GLOBAL_ALPHA_TEXT2,
        GLOBAL_THRESHOLD,
        GLOBAL_MIN_LEN,
        GLOBAL_MAX_LEN,
        autojunk=GLOBAL_AUTOJUNK,
    )

    rf_fuzz = None
    if GLOBAL_USE_RAPIDFUZZ:
        from rapidfuzz import fuzz
        rf_fuzz = fuzz

    results = []
    for a_start, a_end, b_start, b_end, sim_filtered in local_regions:
        alpha_start1 = a_start + alpha_chunk_start
        alpha_end1 = a_end + alpha_chunk_start

        char_start1 = GLOBAL_ALPHA_TO_CHAR1[alpha_start1]
        char_end1_excl = GLOBAL_ALPHA_TO_CHAR1[alpha_end1 - 1] + 1
        char_start2 = GLOBAL_ALPHA_TO_CHAR2[b_start]
        char_end2_excl = GLOBAL_ALPHA_TO_CHAR2[b_end - 1] + 1

        byte_start1 = GLOBAL_CHAR_TO_BYTE1[char_start1]
        byte_end1 = (
            GLOBAL_CHAR_TO_BYTE1[char_end1_excl]
            if char_end1_excl < len(GLOBAL_CHAR_TO_BYTE1)
            else len(GLOBAL_DATA1)
        )
        byte_start2 = GLOBAL_CHAR_TO_BYTE2[char_start2]
        byte_end2 = (
            GLOBAL_CHAR_TO_BYTE2[char_end2_excl]
            if char_end2_excl < len(GLOBAL_CHAR_TO_BYTE2)
            else len(GLOBAL_DATA2)
        )

        sim_rf: Optional[float] = None
        if rf_fuzz is not None:
            text1 = GLOBAL_DATA1[byte_start1:byte_end1].decode("utf-8", errors="replace")
            text2 = GLOBAL_DATA2[byte_start2:byte_end2].decode("utf-8", errors="replace")
            sim_rf_val = rf_fuzz.ratio(text1, text2) / 100.0
            if sim_rf_val < GLOBAL_THRESHOLD:
                continue
            sim_rf = sim_rf_val

        results.append(
            (byte_start1, byte_end1, byte_start2, byte_end2, sim_filtered, sim_rf)
        )

    return results


# ---------------------------------------------------------------------------
# Chunking
# ---------------------------------------------------------------------------


def make_chunks(length: int, chunk_size: int, overlap: int):
    """Yield (start, end) ranges covering [0, length) with overlap."""
    if chunk_size <= 0:
        raise ValueError("chunk_size must be > 0")
    if overlap < 0:
        raise ValueError("overlap must be >= 0")
    start = 0
    while start < length:
        end = min(length, start + chunk_size)
        yield (max(0, start - overlap), min(length, end + overlap))
        start = end


# ---------------------------------------------------------------------------
# Worker initializer
# ---------------------------------------------------------------------------


def init_worker(
    path1: str,
    path2: str,
    use_rapidfuzz: bool,
    threshold: float,
    min_len: int,
    max_len: int,
    ignore_non_alpha: bool,
    autojunk: bool,
):
    """Read both files and populate all worker-process globals.

    Each worker reads both files independently. On Linux (fork start method),
    the OS uses copy-on-write so globals from the parent are shared until
    modified. On macOS/Windows (spawn), each worker gets its own copies.
    """
    global GLOBAL_DATA1, GLOBAL_DATA2
    global GLOBAL_TEXT1, GLOBAL_TEXT2
    global GLOBAL_CHAR_TO_BYTE1, GLOBAL_CHAR_TO_BYTE2
    global GLOBAL_ALPHA_TEXT1, GLOBAL_ALPHA_TEXT2
    global GLOBAL_ALPHA_TO_CHAR1, GLOBAL_ALPHA_TO_CHAR2
    global GLOBAL_USE_RAPIDFUZZ, GLOBAL_THRESHOLD
    global GLOBAL_MIN_LEN, GLOBAL_MAX_LEN, GLOBAL_IGNORE_NON_ALPHA
    global GLOBAL_AUTOJUNK

    with open(path1, "rb") as f1:
        GLOBAL_DATA1 = f1.read()
    with open(path2, "rb") as f2:
        GLOBAL_DATA2 = f2.read()

    GLOBAL_TEXT1 = GLOBAL_DATA1.decode("utf-8", errors="replace")
    GLOBAL_TEXT2 = GLOBAL_DATA2.decode("utf-8", errors="replace")

    GLOBAL_CHAR_TO_BYTE1 = build_char_to_byte_map(GLOBAL_TEXT1)
    GLOBAL_CHAR_TO_BYTE2 = build_char_to_byte_map(GLOBAL_TEXT2)

    GLOBAL_IGNORE_NON_ALPHA = ignore_non_alpha

    if ignore_non_alpha:
        GLOBAL_ALPHA_TEXT1, GLOBAL_ALPHA_TO_CHAR1 = build_alpha_view(GLOBAL_TEXT1)
        GLOBAL_ALPHA_TEXT2, GLOBAL_ALPHA_TO_CHAR2 = build_alpha_view(GLOBAL_TEXT2)
    else:
        GLOBAL_ALPHA_TEXT1 = GLOBAL_TEXT1
        GLOBAL_ALPHA_TEXT2 = GLOBAL_TEXT2
        GLOBAL_ALPHA_TO_CHAR1 = IdentityList(len(GLOBAL_TEXT1))
        GLOBAL_ALPHA_TO_CHAR2 = IdentityList(len(GLOBAL_TEXT2))

    GLOBAL_USE_RAPIDFUZZ = use_rapidfuzz
    GLOBAL_THRESHOLD = threshold
    GLOBAL_MIN_LEN = min_len
    GLOBAL_MAX_LEN = max_len
    GLOBAL_AUTOJUNK = autojunk

    if use_rapidfuzz:
        try:
            from rapidfuzz import fuzz  # noqa: F401
        except ImportError:
            print(
                "ERROR: --use-rapidfuzz was requested but RapidFuzz is not installed.\n"
                "Install it with:  pip install rapidfuzz",
                file=sys.stderr,
            )
            sys.exit(1)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def fmt_size(n: int) -> str:
    """Format byte count as human-readable size."""
    if n < 1024:
        return f"{n} B"
    if n < 1048576:
        return f"{n / 1024:.1f} KB"
    return f"{n / 1048576:.1f} MB"


def main():
    parser = argparse.ArgumentParser(
        description=(
            "Find parallel (similar) passages between two large text files using "
            "chunked multiprocess difflib matching over a Unicode letter-only view."
        )
    )
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    parser.add_argument("file1", help="First input file")
    parser.add_argument("file2", help="Second input file")
    parser.add_argument(
        "-o", "--output",
        default=None,
        help="Write output to this file (required for --format sqlite).",
    )
    parser.add_argument(
        "--format",
        choices=["jsonl", "json", "sqlite", "csv"],
        default="jsonl",
        help=(
            "Output format (default: jsonl). "
            "jsonl: one JSON object per line; "
            "json: pretty-printed JSON array; "
            "sqlite: SQLite database (-o required); "
            "csv: comma-separated values."
        ),
    )
    parser.add_argument(
        "-n", "--min-len",
        type=int, default=100,
        help="Minimum region length in alphabetic characters (default: 100).",
    )
    parser.add_argument(
        "--max-len",
        type=int, default=0,
        help="Maximum region length in alphabetic characters (0 = no limit, default: 0).",
    )
    parser.add_argument(
        "-t", "--threshold",
        type=float, default=0.8,
        help="Similarity threshold 0\u20131 (default: 0.8).",
    )
    parser.add_argument(
        "-j", "--jobs",
        type=int, default=None,
        help="Number of worker processes (default: CPU count).",
    )
    parser.add_argument(
        "--chunk-size",
        type=int, default=500_000,
        help=(
            "Chunk size in alphabetic characters for file1 (default: 500000). "
            "Larger = fewer chunks but more memory per worker."
        ),
    )
    parser.add_argument(
        "--overlap",
        type=int, default=None,
        help=(
            "Overlap between chunks in alphabetic characters "
            "(default: max(2 * min_len, 100))."
        ),
    )
    parser.add_argument(
        "--use-rapidfuzz",
        action="store_true",
        help=(
            "Re-score regions with RapidFuzz fuzz.ratio on the original UTF-8 text. "
            "Regions below threshold after re-scoring are discarded."
        ),
    )
    parser.add_argument(
        "--no-ignore-non-alpha",
        action="store_true",
        help=(
            "Disable the letter-only filter. By default non-letter characters "
            "(punctuation, digits, spaces) are ignored during matching."
        ),
    )
    parser.add_argument(
        "--fast",
        action="store_true",
        help=(
            "Enable difflib autojunk heuristic for faster matching. "
            "May miss regions containing very common character patterns."
        ),
    )
    parser.add_argument(
        "--max-results",
        type=int, default=0,
        help=(
            "Maximum number of results to output (0 = no limit, default: 0). "
            "Results are sorted by position before truncation."
        ),
    )
    parser.add_argument(
        "-q", "--quiet",
        action="store_true",
        help="Suppress progress bar and summary output on stderr.",
    )

    if len(sys.argv) == 1:
        parser.print_help(sys.stderr)
        sys.exit(1)

    args = parser.parse_args()

    # --- Validate arguments ---
    if not (0.0 < args.threshold <= 1.0):
        parser.error("--threshold must be between 0 (exclusive) and 1 (inclusive).")
    if args.min_len <= 0:
        parser.error("--min-len must be a positive integer.")
    if args.max_len < 0:
        parser.error("--max-len must be >= 0 (use 0 for no limit).")
    if args.max_len > 0 and args.max_len < args.min_len:
        parser.error("--max-len must be >= --min-len when set.")
    if args.chunk_size <= 0:
        parser.error("--chunk-size must be a positive integer.")
    if args.jobs is not None and args.jobs <= 0:
        parser.error("--jobs must be a positive integer.")
    if args.max_results < 0:
        parser.error("--max-results must be >= 0 (use 0 for no limit).")
    if args.format == "sqlite" and not args.output:
        parser.error("--format sqlite requires -o/--output to specify the database file.")
    if not os.path.isfile(args.file1):
        parser.error(f"File not found: {args.file1!r}")
    if not os.path.isfile(args.file2):
        parser.error(f"File not found: {args.file2!r}")

    if args.use_rapidfuzz:
        try:
            import rapidfuzz  # noqa: F401
        except ImportError:
            parser.error(
                "--use-rapidfuzz was requested but RapidFuzz is not installed.\n"
                "Install it with:  pip install rapidfuzz"
            )

    ignore_non_alpha = not args.no_ignore_non_alpha
    t_start = time.monotonic()

    # Read file1 in main process to determine chunk count
    with open(args.file1, "rb") as f:
        data1_main = f.read()
    text1_main = data1_main.decode("utf-8", errors="replace")

    if ignore_non_alpha:
        len_alpha1 = count_alpha(text1_main)
    else:
        len_alpha1 = len(text1_main)

    # Memory estimation warning
    num_workers = args.jobs or os.cpu_count() or 1
    file1_bytes = len(data1_main)
    file2_bytes = os.path.getsize(args.file2)
    est_gb = num_workers * (file1_bytes + file2_bytes) * 5 / (1 << 30)
    if est_gb > 4 and not args.quiet:
        print(
            f"Warning: estimated memory usage ~{est_gb:.1f} GB "
            f"({num_workers} workers \u00d7 {fmt_size(file1_bytes + file2_bytes)} \u00d7 ~5). "
            f"Consider reducing -j or --chunk-size.",
            file=sys.stderr,
        )

    # Build the writer
    out_file = None
    writer = None
    try:
        if args.format == "sqlite":
            writer = SqliteWriter(args.output)
            out_stream = None
        else:
            out_file = (
                open(args.output, "w", encoding="utf-8")
                if args.output else None
            )
            out_stream = out_file if out_file is not None else sys.stdout
            if args.format == "jsonl":
                writer = JsonlWriter(out_stream)
            elif args.format == "json":
                writer = JsonWriter(out_stream)
            else:  # csv
                writer = CsvWriter(out_stream)

        if len_alpha1 == 0:
            if not args.quiet:
                print("Warning: no alphabetic characters found in file1.", file=sys.stderr)
            writer.finalize([])
            return

        overlap = args.overlap if args.overlap is not None else max(2 * args.min_len, 100)
        chunk_ranges = list(make_chunks(len_alpha1, args.chunk_size, overlap))

        all_regions = []

        try:
            with ProcessPoolExecutor(
                max_workers=args.jobs,
                initializer=init_worker,
                initargs=(
                    args.file1, args.file2,
                    args.use_rapidfuzz, args.threshold,
                    args.min_len, args.max_len,
                    ignore_non_alpha, args.fast,
                ),
            ) as executor:
                futures = [executor.submit(process_chunk, chunk) for chunk in chunk_ranges]

                with tqdm(
                    total=len(futures), desc="Processing chunks",
                    unit="chunk", file=sys.stderr,
                    disable=args.quiet,
                ) as pbar:
                    for fut in as_completed(futures):
                        try:
                            regions = fut.result()
                        except Exception as exc:
                            print(f"Worker error: {exc}", file=sys.stderr)
                        else:
                            all_regions.extend(regions)
                        finally:
                            pbar.update(1)
        except KeyboardInterrupt:
            print("\nInterrupted by user.", file=sys.stderr)
            sys.exit(130)

        # Deduplicate regions from overlapping chunks; keep max similarities
        region_dict = {}
        for b_start1, b_end1, b_start2, b_end2, sim_filt, sim_rf in all_regions:
            key = (b_start1, b_end1, b_start2, b_end2)
            if key in region_dict:
                prev_filt, prev_rf = region_dict[key]
                best_filt = max(prev_filt, sim_filt)
                best_rf = (
                    None if prev_rf is None and sim_rf is None
                    else max(x for x in (prev_rf, sim_rf) if x is not None)
                )
                region_dict[key] = (best_filt, best_rf)
            else:
                region_dict[key] = (sim_filt, sim_rf)

        sorted_regions = sorted(
            (
                (b_start1, b_end1, b_start2, b_end2, sims[0], sims[1])
                for (b_start1, b_end1, b_start2, b_end2), sims in region_dict.items()
            ),
            key=lambda r: (r[0], r[2]),
        )

        # Apply --max-results truncation
        if args.max_results > 0 and len(sorted_regions) > args.max_results:
            sorted_regions = sorted_regions[:args.max_results]

        # Read file2 for final text extraction (deferred to reduce peak memory)
        with open(args.file2, "rb") as f:
            data2_main = f.read()

        records = []
        for b_start1, b_end1, b_start2, b_end2, sim_filt, sim_rf in sorted_regions:
            span_len_bytes = max(b_end1 - b_start1, b_end2 - b_start2)
            text1 = data1_main[b_start1:b_end1].decode("utf-8", errors="replace")
            text2 = data2_main[b_start2:b_end2].decode("utf-8", errors="replace")
            record = make_record(
                args.file1, b_start1, b_end1,
                args.file2, b_start2, b_end2,
                span_len_bytes, sim_filt, sim_rf,
                text1, text2,
            )
            writer.writerow(record)
            records.append(record)

        writer.finalize(records)
    finally:
        if out_file:
            out_file.close()

    elapsed = time.monotonic() - t_start

    if not args.quiet:
        dest = f"{args.output!r}" if args.output else "stdout"
        truncated = ""
        if args.max_results > 0 and len(region_dict) > args.max_results:
            truncated = f" (limited from {len(region_dict)} total)"
        print(
            f"Done. {len(sorted_regions)} region(s) written to {dest} "
            f"({args.format}) in {elapsed:.1f}s.{truncated}",
            file=sys.stderr,
        )


if __name__ == "__main__":
    main()
