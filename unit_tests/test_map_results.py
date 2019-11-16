#!/usr/bin/python3

# this program verifies implementation of the map operation by checking
# the output results from the test/input/simple_data_* input files

import os
import sys
import re
from collections import Counter
from pprint import pprint

def get_words_from_line(line):
    return list(filter(lambda x: x, re.split(r"[\s,.\"']+", line)))

def count_words_from_input_files(input_dir, pattern):
    files = filter(lambda f: f.find(pattern) >= 0, os.listdir(input_dir))
    words = Counter()
    for filename in files:
        with open(os.path.join(input_dir, filename), "r") as file:
            lines = file.readlines()
        for line in lines:
            words.update(get_words_from_line(line))
    return words

def count_words_from_output_files(output_dir):
    files = filter(lambda f: f.endswith("_temp.txt"), os.listdir(output_dir))
    all_words = []
    for filename in files:
        file_words = Counter()
        with open(os.path.join(output_dir, filename), "r") as file:
            lines = file.readlines()
        for line in lines:
            file_words[get_words_from_line(line)[0]] += 1
        all_words.append(file_words)
    return all_words

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
    pprint(input_words)
    pprint(combined_output_words)
    assert input_words == combined_output_words, "word counts do not match expected results"


def run_tests(input_dir, input_pattern, output_dir):
    input_counter = count_words_from_input_files(input_dir, input_pattern)
    output_counters = count_words_from_output_files(output_dir)

    test_no_word_in_multiple_files(output_counters)
    test_word_counts_are_correct(input_counter, output_counters)


if __name__ == '__main__':
    if len(sys.argv) < 4:
        print(f"USAGE: python3 {sys.argv[0]} <input_dir> <input_file_pattern> <output_dir>")
        sys.exit(1)

    _, input_dir, pattern, output_dir = sys.argv
    run_tests(input_dir, pattern, output_dir)
    print("Map results tests passed!")