#include "worker_client.h"

#include <functional>
#include <grpcpp/grpcpp.h>

#include "masterworker.pb.h"
#include "masterworker.grpc.pb.h"

using masterworker::Worker;
using masterworker::MapJobRequest;
using masterworker::MapJobReply;
using masterworker::ReduceJobRequest;
using masterworker::ReduceJobReply;

WorkerClient::WorkerClient(std::shared_ptr<grpc::Channel> channel, std::string id)
    : stub_(Worker::NewStub(channel)), id_(id)
{
    status_ = WorkerStatus::AVAILABLE;
}
 
WorkerStatus WorkerClient::status()
{
    return status_;
}

bool WorkerClient::acquireForJob()
{
    if (status_ != WorkerStatus::AVAILABLE) {
        return false;
    }
    status_ = WorkerStatus::PENDING;
    return true;
}

bool WorkerClient::busy()
{
    return status_ == WorkerStatus::BUSY_MAP || status_== WorkerStatus::BUSY_REDUCE || status_ == WorkerStatus::PENDING;
}

bool WorkerClient::notWorking()
{
    return status_ == WorkerStatus::AVAILABLE || status_ == WorkerStatus::DEAD;
}

bool WorkerClient::executeMapJob(const MapJob & job,
    std::function<void(MapJobReply *reply)> reply_callback)
{
    if (status_ != WorkerStatus::PENDING) {
        return false;
    }
    status_ = WorkerStatus::BUSY_MAP;
    printf("Worker %s status busy %d\n", id_.c_str(), status_);
    auto & shard = job.shard;
    print_shard(shard, "Master: Executing map job");
    
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

    if (!request_status.ok()) {
        return false;
    }

    if (!reply.success()) {
        return false;
    }

    reply_callback(&reply);

    status_ = WorkerStatus::AVAILABLE;

    return true;
}

bool WorkerClient::executeReduceJob(const ReduceJob & job,
    std::function<void(ReduceJobReply *reply)> reply_callback)
{
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
    }
    
    grpc::Status request_status = stub_->ExecuteReduceJob(&context, request, &reply);

    if (!request_status.ok()) {
        return false;
    }

    if (!reply.success()) {
        return false;
    }

    reply_callback(&reply);

    status_ = WorkerStatus::AVAILABLE;

    return true;
}

std::string & WorkerClient::id()
{
    return id_;
}