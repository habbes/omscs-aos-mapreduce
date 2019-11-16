#include "worker_client.h"

#include <grpcpp/grpcpp.h>

#include "masterworker.pb.h"
#include "masterworker.grpc.pb.h"

using masterworker::Worker;
using masterworker::MapJobRequest;
using masterworker::MapJobReply;

WorkerClient::WorkerClient(std::shared_ptr<grpc::Channel> channel)
    : stub_(Worker::NewStub(channel))
{
    status_ = WorkerStatus::AVAILABLE;
}

WorkerStatus WorkerClient::status()
{
    return status_;
}

bool WorkerClient::executeMapJob(const FileShard & shard, int n_output_files, const std::string & output_dir)
{
    print_shard(shard, "Master: Executing map job");
    status_ = WorkerStatus::BUSY_MAP;
    
    grpc::ClientContext context;
    MapJobRequest request;
    MapJobReply reply;
    
    request.set_n_output_files(n_output_files);
    request.set_output(output_dir);
    for (const auto & offset: shard.offsets) {
        auto request_offset = request.add_offsets();
        request_offset->set_file(offset.file);
        request_offset->set_start(offset.start);
        request_offset->set_stop(offset.stop);
    }
    
    grpc::Status request_status = stub_->ExecuteMapJob(&context, request, &reply);

    status_ = WorkerStatus::AVAILABLE;

    return request_status.ok();
}

