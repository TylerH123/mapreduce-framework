#!/usr/bin/env python3
"""Word count reducer.

BAD EXAMPLE: it's inefficient and processes multiple groups all at once.

"""
import sys
import collections

def main():
    """Reduce multiple groups."""
    word_count = collections.defaultdict(int)
    for line in sys.stdin:
        word, _, count = line.partition("\t")
        word_count[word] += int(count)
    for word, count in word_count.items():
        print(f"{word} {count}")

if __name__ == "__main__":
    main()