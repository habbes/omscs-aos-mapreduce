#pragma once

#include <memory>
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

class WorkerClient {
public:
	WorkerClient(std::shared_ptr<grpc::Channel> channel);
    WorkerStatus status();
    bool executeMapJob(FileShard & shard);

private:
	std::unique_ptr<masterworker::Worker::Stub> stub_;
    WorkerStatus status_;
};
