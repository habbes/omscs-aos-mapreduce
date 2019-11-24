#!/usr/bin/python3

# this program verifies the MapReduce implementation by checking
# the output results (intermediate_files and final result files) of the map reduce
# counter program against the specified input files

import os
import sys
import re
from collections import Counter
from pprint import pprint

def get_words_from_line(line):
    return list(filter(lambda x: x, re.split(r"[\s,.\"']+", line)))

def get_file_keys(filenames):
    return set(map(lambda f: f.split('_')[0], filenames))

def count_words_from_input_files(input_dir, pattern):
    files = filter(lambda f: f.find(pattern) >= 0, os.listdir(input_dir))
    words = Counter()
    for filename in files:
        with open(os.path.join(input_dir, filename), "r") as file:
            lines = file.readlines()
        for line in lines:
            words.update(get_words_from_line(line))
    return words

def count_words_from_output_files(output_dir, pattern, is_final):
    files = list(filter(lambda f: f.find(pattern) >= 0, os.listdir(output_dir)))
    keys = get_file_keys(files)
    # combine counts from files with the same key
    counters = {}
    for key in keys:
        counters[key] = Counter()

    for filename in files:
        file_key = filename.split('_')[0]
        file_counter = counters[file_key]
        with open(os.path.join(output_dir, filename), "r") as file:
            lines = file.readlines()
        for line in lines:
            try:
                word, count = get_words_from_line(line.strip())
            except:
                assert False, f"Failed to get words from line: {line}"
            if is_final:
                file_counter[word] = int(count)
            else:
                file_counter[word] += 1
    return list(counters.values())

def combine_word_counters(words_counters):
    combined_counter = Counter()
    for counter in words_counters:
        combined_counter.update(counter)
    return combined_counter

def test_no_word_in_multiple_files(word_counters):
    for i, counter in enumerate(word_counters):
        other_counters = word_counters[0: i] + word_counters[i + 1:]
        for other_counter in other_counters:
            for word in counter:
                assert word not in other_counter, f"word {word} found in two files"


def test_word_counts_are_correct(input_words, output_word_lists):
    combined_output_words = combine_word_counters(output_word_lists)
    # pprint(input_words)
    # pprint(combined_output_words)
    assert input_words == combined_output_words, "word counts do not match expected results"


def run_tests(input_dir, input_pattern, output_dir, output_pattern, is_final):
    input_counter = count_words_from_input_files(input_dir, input_pattern)
    output_counters = count_words_from_output_files(output_dir, output_pattern, is_final)

    test_no_word_in_multiple_files(output_counters)
    test_word_counts_are_correct(input_counter, output_counters)


if __name__ == '__main__':
    if len(sys.argv) != 6:
        print(f"USAGE: python3 {sys.argv[0]} <input_dir> <input_file_pattern> <output_dir> <output_pattern> <i|f>")
        sys.exit(1)

    _, input_dir, input_pattern, output_dir, output_pattern, is_final = sys.argv
    is_final = is_final == 'f'
    run_tests(input_dir, input_pattern, output_dir, output_pattern, is_final)
    print("MapReduce counter results tests passed!")