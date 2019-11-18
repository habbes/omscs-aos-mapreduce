#include "workers_pool.h"
#include "worker_client.h"

#include <string>
#include <vector>
#include <grpc/grpc.h>
#include <grpcpp/channel.h>
#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>

WorkersPool::WorkersPool(const MapReduceSpec & spec)
{
    next_task_id_ = 1;
    n_output_files_ = spec.n_output_files;
    output_dir_ = spec.output_dir;
    for (const auto address : spec.worker_ipaddr_ports) {
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
    MapJob task = { next_task_id_++, shard };
    print_shard(shard, "Master: Add map task");
    map_queue_.push(task);
}

bool WorkersPool::runMapTasks()
{
    while (!map_queue_.empty()) {
        const auto task = map_queue_.front();
        map_queue_.pop();
        auto & worker = getNextWorker();
        auto result = worker->executeMapJob(task, n_output_files_, output_dir_, &intermediate_files_);
        if (!result) {
            print_shard(task.shard, "Master: map task failed, requeing...");
            map_queue_.push(task);
        } else {
            print_shard(task.shard, "Master: completed map task");
        }
    }
    printf("Master: Map tasks complete, intermediate files generated:\n");
    for (auto & file: intermediate_files_) {
        printf("file: %s\n", file.c_str());
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
