#include "worker_client.h"

#include <chrono>
#include <functional>
#include <grpcpp/grpcpp.h>

#include "masterworker.pb.h"
#include "masterworker.grpc.pb.h"

using masterworker::Worker;
using masterworker::MapJobRequest;
using masterworker::MapJobReply;
using masterworker::ReduceJobRequest;
using masterworker::ReduceJobReply;

constexpr const int JOB_TIMEOUT_SECONDS = 30;
constexpr const int MAX_ALLOWED_TIMEOUTS = 2;
constexpr const int MAX_ALLOWED_RPC_ERRORS = 10;

WorkerClient::WorkerClient(std::shared_ptr<grpc::Channel> channel, std::string id)
    : stub_(Worker::NewStub(channel)), id_(id)
{
    status_ = WorkerStatus::AVAILABLE;
    num_timeouts_ = 0;
    num_rpc_errors_ = 0;
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
    auto & shard = job.shard;
    
    grpc::ClientContext context;
    MapJobRequest request;
    MapJobReply reply;

    auto deadline = std::chrono::system_clock::now() + std::chrono::seconds(JOB_TIMEOUT_SECONDS);
    context.set_deadline(deadline);
    
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
        handleErrorStatus(request_status);
        return false;
    }

    if (!reply.success()) {
        handleNonSuccessReply();
        return false;
    }

    reply_callback(&reply);
    handleSuccess();

    return true;
}

bool WorkerClient::executeReduceJob(const ReduceJob & job,
    std::function<void(ReduceJobReply *reply)> reply_callback)
{
    if (status_ != WorkerStatus::PENDING) {
        return false;
    }
    status_ = WorkerStatus::BUSY_REDUCE;
    printf("Master: reduce job %d assigned to worker %s\n", job.job_id, id_.c_str());
    grpc::ClientContext context;
    ReduceJobRequest request;
    ReduceJobReply reply;

    auto deadline = std::chrono::system_clock::now() + std::chrono::seconds(JOB_TIMEOUT_SECONDS);
    context.set_deadline(deadline);

    request.set_key(std::to_string(job.job_id));
    request.set_output_dir(job.output_dir);
    request.set_user_id(job.user_id);
    for (auto file: job.intermediate_files) {
        auto request_file = request.add_intermediate_files();
        *request_file = file;
    }
    
    grpc::Status request_status = stub_->ExecuteReduceJob(&context, request, &reply);

    if (!request_status.ok()) {
        handleErrorStatus(request_status);
        return false;
    }

    if (!reply.success()) {
        handleNonSuccessReply();
        return false;
    }

    reply_callback(&reply);
    handleSuccess();

    return true;
}

std::string & WorkerClient::id()
{
    return id_;
}

void WorkerClient::handleErrorStatus(grpc::Status & status)
{
    printf("Master: Worker %s failed due to request error. Error code: %d. Error message: %s.\n",
        id_.c_str(), status.error_code(), status.error_message().c_str());
    switch (status.error_code()) {
        case grpc::StatusCode::DEADLINE_EXCEEDED:
            num_timeouts_++;
            if (num_timeouts_ >= MAX_ALLOWED_TIMEOUTS) {
                printf("Master: Worker %s failed due to deadline. Marked as dead.\n", id_.c_str());
                status_ = WorkerStatus::DEAD;
            } else {
                printf("Master: Worker %s failed due to deadline\n", id_.c_str());
                status_ = WorkerStatus::AVAILABLE;
            }
            break;
        case grpc::StatusCode::UNAVAILABLE:
            printf("Master: Worker %s failed due to connectivity issue. Marked as dead.\n", id_.c_str());
            status_ = WorkerStatus::DEAD;
            break;
        default:
            num_rpc_errors_++;
            if (num_rpc_errors_ >= MAX_ALLOWED_RPC_ERRORS) {
                printf("Master: Worker %s exceeded allowed RPC errors. Marked as dead.\n", id_.c_str());
                status_ = WorkerStatus::DEAD;
            }
            else {
                status_ = WorkerStatus::AVAILABLE;
            }
    }
}

void WorkerClient::handleNonSuccessReply()
{
    printf("Master: Worker %s failed task.", id_.c_str());
    status_ = WorkerStatus::AVAILABLE;
}

void WorkerClient::handleSuccess()
{
    num_timeouts_ = 0;
    num_rpc_errors_ = 0;
    status_ = WorkerStatus::AVAILABLE;
}