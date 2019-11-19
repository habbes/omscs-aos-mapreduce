#pragma once

#include <memory>
#include <vector>
#include <grpc/grpc.h>
#include <grpcpp/channel.h>

#include "masterworker.grpc.pb.h"
#include "masterworker.pb.h"
#include "file_shard.h"

enum WorkerStatus {
    BUSY_MAP,
    BUSY_REDUCE,
    AVAILABLE,
    DEAD
};

struct MapJob
{
    int job_id;
    FileShard shard;
};

struct ReduceJob
{
    int job_id;
    int n_output_files;
    std::string output_dir;
    std::vector<std::string> intermediate_files;
};

class WorkerClient {
public:
	WorkerClient(std::shared_ptr<grpc::Channel> channel);
    WorkerStatus status();
    // when I defined the method with intermediate_files as vector reference type instead of pointer
    // I got errors when compiling the program, "undefined reference" to the function that was using it
    bool executeMapJob(const MapJob & job, int n_output_files, const std::string & output_dir,
        std::vector<std::string> *intermediate_files);
    bool executeReduceJob(const ReduceJob & job, std::vector<std::string> *output_files);

private:
	std::unique_ptr<masterworker::Worker::Stub> stub_;
    WorkerStatus status_;
};
