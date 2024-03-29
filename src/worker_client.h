#pragma once

#include <memory>
#include <vector>
#include <string>
#include <grpc/grpc.h>
#include <grpcpp/channel.h>

#include "masterworker.grpc.pb.h"
#include "masterworker.pb.h"
#include "file_shard.h"


enum WorkerStatus {
    BUSY_MAP,
    BUSY_REDUCE,
    AVAILABLE,
    PENDING,
    DEAD
};

struct MapJob
{
    int job_id;
    FileShard shard;
    int n_output_files;
    std::string output_dir;
    std::string user_id;
};

struct ReduceJob
{
    int job_id;
    int n_output_files;
    std::string output_dir;
    std::vector<std::string> intermediate_files;
    std::string user_id;
};

class WorkerClient {
public:
	WorkerClient(std::shared_ptr<grpc::Channel> channel, std::string id);
    WorkerStatus status();
    // when I defined the method with intermediate_files as vector reference type instead of pointer
    // I got errors when compiling the program, "undefined reference" to the function that was using it
    bool executeMapJob(const MapJob & job,
        std::function<void(masterworker::MapJobReply *reply)> reply_callback);
    bool executeReduceJob(const ReduceJob & job,
        std::function<void(masterworker::ReduceJobReply *reply)> reply_callback);
    std::string & id();
    bool acquireForJob();
    bool busy();
    bool notWorking();
    void handleErrorStatus(grpc::Status & status);
    void handleNonSuccessReply();
    void handleSuccess();

private:
	std::unique_ptr<masterworker::Worker::Stub> stub_;
    WorkerStatus status_;
    std::string id_;
    int num_timeouts_;
    int num_rpc_errors_;
};
