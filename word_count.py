import os
import re
import time
from collections import Counter


# ── Tokeniser ────────────────────────────────────────────────────────────────

def tokenise(text):
    """Lowercase the text and extract words using a simple regex."""
    text = text.lower()
    words = re.findall(r'\b[a-z]+\b', text)
    return words


# ── Map phase ─────────────────────────────────────────────────────────────────

def count_words_in_file(filepath):
    """Read one file and return a Counter of its word frequencies."""
    with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
        text = f.read()
    words = tokenise(text)
    return Counter(words)


# ── Reduce phase ──────────────────────────────────────────────────────────────

def merge_counters(counters):
    """Merge a list of Counters into one global Counter."""
    global_counts = Counter()
    for counter in counters:
        global_counts.update(counter)
    return global_counts


# ── Sequential implementation ─────────────────────────────────────────────────

def sequential_word_count(filepaths):
    """Process all files one by one and return global word counts."""
    partial_counts = []
    for filepath in filepaths:
        partial_counts.append(count_words_in_file(filepath))
    return merge_counters(partial_counts)

# ── Threading implementation ──────────────────────────────────────────────────

import threading

def threaded_word_count(filepaths, num_threads=4):
    """Process files in parallel using threads."""
    results = [None] * len(filepaths)
    lock = threading.Lock()

    def worker(index, filepath):
        local_count = count_words_in_file(filepath)
        with lock:
            results[index] = local_count

    threads = []
    for i, filepath in enumerate(filepaths):
        t = threading.Thread(target=worker, args=(i, filepath))
        threads.append(t)
        t.start()

        # Only allow num_threads to run at once
        if len(threads) >= num_threads:
            for t in threads:
                t.join()
            threads = []

    # Wait for any remaining threads
    for t in threads:
        t.join()

    return merge_counters(results)

# ── Multiprocessing implementation ────────────────────────────────────────────

from multiprocessing import Pool

def multiprocess_word_count(filepaths, num_processes=4):
    """Process files in parallel using separate processes."""
    with Pool(processes=num_processes) as pool:
        partial_counts = pool.map(count_words_in_file, filepaths)
    return merge_counters(partial_counts)

# ── Main ──────────────────────────────────────────────────────────────────────

if __name__ == '__main__':
    try:
        corpus_dir = 'corpus'
        filepaths = [
            os.path.join(corpus_dir, f)
            for f in sorted(os.listdir(corpus_dir))
            if f.endswith('.txt')
        ]

        print(f"Found {len(filepaths)} files in corpus.\n")

        # ── Sequential ────────────────────────────────────────────────────────
        start = time.time()
        results_seq = sequential_word_count(filepaths)
        end = time.time()
        time_seq = end - start
        print(f"Sequential completed in    {time_seq:.4f} seconds")
        print(f"Total unique words:         {len(results_seq)}\n")

        # ── Threaded ──────────────────────────────────────────────────────────
        start = time.time()
        results_thr = threaded_word_count(filepaths, num_threads=4)
        end = time.time()
        time_thr = end - start
        print(f"Threaded (4) completed in  {time_thr:.4f} seconds")
        print(f"Total unique words:         {len(results_thr)}")
        if results_seq == results_thr:
            print("✓ Matches sequential\n")
        else:
            print("✗ WARNING: does not match sequential\n")

        # ── Multiprocessing ───────────────────────────────────────────────────
        start = time.time()
        results_mp = multiprocess_word_count(filepaths, num_processes=4)
        end = time.time()
        time_mp = end - start
        print(f"Multiprocess (4) completed in  {time_mp:.4f} seconds")
        print(f"Total unique words:             {len(results_mp)}")
        if results_seq == results_mp:
            print("✓ Matches sequential\n")
        else:
            print("✗ WARNING: does not match sequential\n")

        # ── Summary ───────────────────────────────────────────────────────────
        print("─" * 45)
        print(f"{'Method':<25} {'Time':>8} {'Speed-up':>10}")
        print("─" * 45)
        print(f"{'Sequential':<25} {time_seq:>8.4f} {'1.00x':>10}")
        print(f"{'Threaded (4)':<25} {time_thr:>8.4f} {time_seq/time_thr:>9.2f}x")
        print(f"{'Multiprocess (4)':<25} {time_mp:>8.4f} {time_seq/time_mp:>9.2f}x")
        print("─" * 45)

    except Exception as e:
        print(f"ERROR: {e}")
        import traceback
        traceback.print_exc()