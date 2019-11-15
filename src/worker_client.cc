#include "worker_client.h"
#include "masterworker.pb.h"
#include "masterworker.grpc.pb.h"

using masterworker::Worker;

WorkerClient::WorkerClient(std::shared_ptr<grpc::Channel> channel)
    : stub_(Worker::NewStub(channel))
{

}

WorkerStatus WorkerClient::status()
{
    return status_;
}