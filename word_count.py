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


# ── Main ──────────────────────────────────────────────────────────────────────

if __name__ == '__main__':
    try:
        # Build list of all .txt files in the corpus folder
        corpus_dir = 'corpus'
        filepaths = [
            os.path.join(corpus_dir, f)
            for f in sorted(os.listdir(corpus_dir))
            if f.endswith('.txt')
        ]

        print(f"Found {len(filepaths)} files in corpus.\n")

        # Run sequential version and time it
        start = time.time()
        results = sequential_word_count(filepaths)
        end = time.time()

        print(f"Sequential completed in {end - start:.4f} seconds")
        print(f"Total unique words: {len(results)}")
        print(f"\nTop 20 most common words:")
        for word, count in results.most_common(20):
            print(f"  {word:<20} {count}")

    except Exception as e:
        print(f"ERROR: {e}")
        import traceback
        traceback.print_exc()