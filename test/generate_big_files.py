#!/usr/bin/python3

# generates 200MB+ text files to use for test MapReduce implementation

MB = 2 ** 20

def multiply_file(source_file, target_file, target_size):
    with open(source_file, 'r') as f:
        contents = f.read()
    multiples = (target_size // len(contents)) + 2
    with open(target_file, 'w') as f:
        for _ in range(multiples):
            f.write(contents)
            f.write('\n')


args = [
    ('input/testdata_1.txt', 'input/bigfile_1.txt', 200 * MB),
    ('input/testdata_3.txt', 'input/bigfile_2.txt', 200 * MB)
]

for source, target, size in args:
    multiply_file(source, target, size)
    print("Created file", target)