#pragma once

#include <memory>
#include <grpc/grpc.h>
// #include <grpcpp/server.h>
// #include <grpcpp/server_builder.h>
// #include <grpcpp/server_context.h>
#include <grpcpp/channel.h>
// #include <grpcpp/client_context.h>
// #include <grpcpp/create_channel.h>
// #include <grpcpp/security/credentials.h>

#include "masterworker.grpc.pb.h"
#include "masterworker.pb.h"

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

private:
	std::unique_ptr<masterworker::Worker::Stub> stub_;
    WorkerStatus status_;
};
