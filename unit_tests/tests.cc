#include <iostream>
#include <vector>
#include "../src/mapreduce_spec.h"

#define TEST(expr, msg) if (!(expr)) { fprintf(stderr, "Test failed: %s\n", msg); return EXIT_FAILURE;}

int main()
{
    std::cout << "Running tests..." << std::endl;
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


    std::cout << "All tests passed!" << std::endl;
    
    return 0;
}