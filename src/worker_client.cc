#include "worker_client.h"

#include <grpcpp/grpcpp.h>

#include "masterworker.pb.h"
#include "masterworker.grpc.pb.h"

using masterworker::Worker;
using masterworker::MapJobRequest;
using masterworker::MapJobReply;
using masterworker::ReduceJobRequest;
using masterworker::ReduceJobReply;

WorkerClient::WorkerClient(std::shared_ptr<grpc::Channel> channel)
    : stub_(Worker::NewStub(channel))
{
    status_ = WorkerStatus::AVAILABLE;
}

WorkerStatus WorkerClient::status()
{
    return status_;
}

bool WorkerClient::executeMapJob(const MapJob & job, std::vector<std::string> *intermediate_files)
{
    auto & shard = job.shard;
    print_shard(shard, "Master: Executing map job");
    status_ = WorkerStatus::BUSY_MAP;
    
    grpc::ClientContext context;
    MapJobRequest request;
    MapJobReply reply;
    
    request.set_job_id(job.job_id);
    request.set_n_output_files(job.n_output_files);
    request.set_output_dir(job.output_dir);
    request.set_user_id(job.user_id);
    for (const auto & offset: shard.offsets) {
        auto request_offset = request.add_offsets();
        request_offset->set_file(offset.file);
        request_offset->set_start(offset.start);
        request_offset->set_stop(offset.stop);
    }
    
    grpc::Status request_status = stub_->ExecuteMapJob(&context, request, &reply);

    status_ = WorkerStatus::AVAILABLE;

    if (!request_status.ok()) {
        printf("----Task failed from Status\n");
        return false;
    }

    if (!reply.success()) {
        printf("----Task failed from No Success\n");
        return false;
    }

    for (int i = 0; i < reply.intermediate_files_size(); i++) {
        intermediate_files->push_back(reply.intermediate_files(i));
    }

    return true;
}

bool WorkerClient::executeReduceJob(const ReduceJob & job, std::vector<std::string> * output_files)
{
    printf("Master: Executing reduce job %d\n", job.job_id);
    status_ = WorkerStatus::BUSY_REDUCE;

    grpc::ClientContext context;
    ReduceJobRequest request;
    ReduceJobReply reply;

    request.set_key(std::to_string(job.job_id));
    request.set_output_dir(job.output_dir);
    request.set_user_id(job.user_id);
    for (auto file: job.intermediate_files) {
        auto request_file = request.add_intermediate_files();
        *request_file = file;
        printf("FILE TO RED %s, %s\n", file.c_str(), request_file->c_str());
    }
    
    grpc::Status request_status = stub_->ExecuteReduceJob(&context, request, &reply);

    status_ = WorkerStatus::AVAILABLE;

    if (!request_status.ok()) {
        printf("---- Reduce Task failed from Status\n");
        return false;
    }

    if (!reply.success()) {
        printf("----Reduce Task failed from No Success\n");
        return false;
    }

    output_files->push_back(reply.output_file());

    return true;
}
