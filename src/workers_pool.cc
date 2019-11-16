#include "workers_pool.h"
#include "worker_client.h"

#include <string>
#include <vector>
#include <grpc/grpc.h>
#include <grpcpp/channel.h>
#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>

WorkersPool::WorkersPool(const std::vector<std::string> & addresses)
{
    for (const auto address : addresses) {
        std::unique_ptr<WorkerClient> service(
            new WorkerClient(
                grpc::CreateChannel(address, grpc::InsecureChannelCredentials())
            )
        );
        services_.push_back(std::move(service));
        printf("Master: Connected to worker %s\n", address.c_str());
    }
}

void WorkersPool::addMapTask(FileShard shard)
{
    print_shard(shard, "Master: Add map task");
    map_queue_.push(shard);
}

bool WorkersPool::runMapTasks()
{
    while (!map_queue_.empty()) {
        const auto shard = map_queue_.front();
        map_queue_.pop();
        auto & worker = getNextWorker();
        auto result = worker->executeMapJob(shard);
        if (!result) {
            print_shard(shard, "Master: map task failed, requeing...");
            map_queue_.push(shard);
        } else {
            print_shard(shard, "Master: completed map task");
        }
    }
    return true;
}

std::unique_ptr<WorkerClient> & WorkersPool::getNextWorker()
{
    while (true) {
        for (auto & service: services_) {
            if (service->status() == WorkerStatus::AVAILABLE) {
                return service;
            }
        }
    }
    
}
