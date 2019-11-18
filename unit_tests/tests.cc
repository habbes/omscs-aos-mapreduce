#include <iostream>
#include <vector>
#include "../src/mapreduce_spec.h"
#include "../src/file_shard.h"

#define TEST(expr, msg) if (!(expr)) { fprintf(stderr, "Test failed: %s\n", msg); return false;}

template <typename T>
inline bool are_equal(T actual, T expected)
{
    if (actual != expected) {
        std::cerr << "Expected " << actual << " to equal " << expected << std::endl;
        return false;
    }
    return true;
}

#define RUN_TESTS(fn) if (!(fn())) return EXIT_FAILURE;

bool test_read_mr_spec_from_config_file()
{
    MapReduceSpec spec;
    read_mr_spec_from_config_file("./config.ini", spec);

    TEST(spec.n_workers == 6, "num workers should be 6");
    TEST(spec.output_dir == std::string("output"), "parsed output");
    TEST(spec.n_output_files == 8, "parsed n output files");
    TEST(spec.map_kilobytes == 500, "parsed num kilobytes");
    TEST(spec.user_id == std::string("cs6210"), "parses user id");

    std::vector<std::string> expected_addrs = {
        "localhost:50051", "localhost:50052", "localhost:50053",
        "localhost:50054", "localhost:50055", "localhost:50056"
    };
    TEST(spec.worker_ipaddr_ports == expected_addrs, "parsed addresses and ports");

    std::vector<std::string> expected_in_files = {
        "input/testdata_1.txt", "input/testdata_2.txt"
    };
    TEST(spec.input_files == expected_in_files, "parsed input files");

    return true;
}

bool test_shard_files()
{
    MapReduceSpec spec;
    spec.map_kilobytes = 200;
    spec.input_files.push_back("./testdata_1.txt");
    spec.input_files.push_back("./testdata_2.txt");
    spec.input_files.push_back("./testdata_3.txt");

    std::vector<FileShard> shards;
    shard_files(spec, shards);

    FileShard shard_1, shard_2, shard_3;
    shard_1.offsets.push_back({.file = "./testdata_1.txt", .start = 0, .stop = 204850});

    shard_2.offsets.push_back({.file = "./testdata_1.txt", .start = 204851, .stop = 342122});
    shard_2.offsets.push_back({.file = "./testdata_2.txt", .start = 0, .stop = 43120});
    shard_2.offsets.push_back({.file = "./testdata_3.txt", .start = 0, .stop = 24502});

    shard_3.offsets.push_back({.file = "./testdata_3.txt", .start = 24503, 171419});

    std::vector<FileShard> expectedShards = { shard_1, shard_2, shard_3 };
    // TEST(shards == expectedShards, "decoded shards");
    TEST(are_equal(shards.size(), expectedShards.size()), "shards length correct");
    for (int i = 0; i < shards.size(); i++) {
        auto & actual = shards.at(i);
        auto & expected = expectedShards.at(i);
        TEST(are_equal(actual.offsets.size(), expected.offsets.size()), "shards offsets length correct");
        for (int j = 0; j < actual.offsets.size(); j++) {
            auto & actual_offset = actual.offsets.at(j);
            auto & expected_offset = expected.offsets.at(j);
            TEST(are_equal(actual_offset.file, expected_offset.file), "shard offset file");
            TEST(are_equal(actual_offset.start, expected_offset.start), "shard offset start");
            TEST(are_equal(actual_offset.stop, expected_offset.stop), "shard offset stop");
        }
    }

    return true;
}

bool test_shard_files_with_one_shard_one_file()
{
    MapReduceSpec spec;
    spec.map_kilobytes = 500;
    spec.input_files.push_back("./testdata_1.txt");

    std::vector<FileShard> shards;
    shard_files(spec, shards);

    TEST(are_equal((int) shards.size(), 1), "shards length");
    TEST(are_equal(shards[0].offsets.size(), (size_t) 1), "shard offsets length is 1");
    TEST(are_equal(shards[0].offsets[0].file, std::string("./testdata_1.txt")), "single shard offset file");
    TEST(are_equal(shards[0].offsets[0].start, 0), "single shard offset start");
    TEST(are_equal(shards[0].offsets[0].stop, 342122), "single shard offset stop");

    return true;
}

bool test_shard_files_with_error()
{
    MapReduceSpec spec;
    spec.map_kilobytes = 200;
    spec.input_files.push_back("./non_existent_file.txt");
    std::vector<FileShard> shards;
    bool result = shard_files(spec, shards);

    TEST(are_equal(result, false), "return false if input files don't exist");

    return true;
}

bool test_read_shard()
{
    FileShard shard;
    shard.offsets.push_back({
        .file = "testdata_1.txt",
        .start = 693,
        .stop = 876
    });
    shard.offsets.push_back({
        .file = "testdata_2.txt",
        .start = 0,
        .stop = 122
    });
    std::vector<std::string> lines;
    read_shard(shard, lines);

    TEST(are_equal((int) lines.size(), 4), "read 4 lines from shard");
    TEST(are_equal(lines[0], std::string(
        "eddy starred ballets diet HauptmannHauptmann. Galveston pettifogged potfuls manufacturer."
    )), "read first line");
    TEST(are_equal(lines[1], std::string(
        "unblocking querying watersheds whittling intonederive. unlocked freakier hated Jimenez fraud."
    )), "read second line");
    TEST(are_equal(lines[2], std::string(
        "antihistamines wight dethronement."
    )), "read third line");
    TEST(are_equal(lines[3], std::string(
        "bamboozle chilliest smartening Potemkintempter. respect observable degenerative famish."
    )), "read fourth line");

    return true;
}

int main()
{
    std::cout << "Running tests..." << std::endl;
    
    RUN_TESTS(test_read_mr_spec_from_config_file);
    RUN_TESTS(test_shard_files);
    RUN_TESTS(test_shard_files_with_one_shard_one_file);
    RUN_TESTS(test_shard_files_with_error);
    RUN_TESTS(test_read_shard);

    std::cout << "All tests passed!" << std::endl;
    
    return 0;
}