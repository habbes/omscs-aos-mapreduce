#include "workers_pool.h"
#include "worker_client.h"

#include <string>
#include <vector>
#include <algorithm>
#include <grpc/grpc.h>
#include <grpcpp/channel.h>
#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>

using masterworker::MapJobReply;
using masterworker::ReduceJobReply;

WorkersPool::WorkersPool(const MapReduceSpec & spec)
{
    next_task_id_ = 1;
    n_output_files_ = spec.n_output_files;
    output_dir_ = spec.output_dir;
    user_id_ = spec.user_id;
    threadpool_ = std::unique_ptr<threadpool>(new threadpool(4));
    for (const auto address : spec.worker_ipaddr_ports) {
        std::unique_ptr<WorkerClient> service(
            new WorkerClient(
                grpc::CreateChannel(address, grpc::InsecureChannelCredentials()),
                address
            )
        );
        services_.push_back(std::move(service));
        printf("Master: Connected to worker %s\n", address.c_str());
    }
}

void WorkersPool::addMapTask(FileShard shard)
{
    MapJob task = {
        next_task_id_++,
        shard,
        n_output_files: n_output_files_,
        output_dir: output_dir_,
        user_id: user_id_
    };
    print_shard(shard, "Master: Add map task");
    map_queue_.push(task);
}

bool WorkersPool::runMapTasks()
{
    auto intermediate_files_ptr = &intermediate_files_;
    auto cond_task_done_ptr = &cond_task_done_;
    auto queue_lock_ptr = &queue_lock_;
    auto map_queue_ptr = &map_queue_;
    auto complete = false;
    auto this_ = this;
    // auto handler = std::bind(&WorkersPool::handleMapJobReply, this);
    while (1) {
        {
            std::unique_lock<std::mutex> l(queue_lock_);
            while (map_queue_.empty()) {
                cond_task_done_.wait(l);
                printf("IS queue empty %d are all done %d\n", map_queue_.empty(), areAllWorkersDone());
                if (map_queue_.empty() && areAllWorkersDone()) {
                    printf("DONE\n");
                    complete = true;
                    break;
                    
                }
            }
            if (complete) {
                printf("BREAKING...\n");
                break;
            }
            const auto task = std::move(map_queue_.front());
            map_queue_.pop();
            auto worker = getNextWorker();
            
            threadpool_->enqueue_task([worker, task, map_queue_ptr, intermediate_files_ptr, this_, queue_lock_ptr, cond_task_done_ptr]() {
                auto result = worker->executeMapJob(task, intermediate_files_ptr,
                    [this_](MapJobReply *reply) { this_->handleMapJobReply(reply); });
                if (!result) {
                    std::unique_lock<std::mutex> l(*queue_lock_ptr);
                    print_shard(task.shard, "Master: map task failed, requeing...");
                    map_queue_ptr->push(task);
                } else {
                    print_shard(task.shard, "Master: completed map task");
                }
                cond_task_done_ptr->notify_one();
            });
        }
    }
    // while (!map_queue_.empty()) {
    //     const auto task = map_queue_.front();
    //     map_queue_.pop();
    //     auto worker = getNextWorker();
    //     auto result = worker->executeMapJob(task, &intermediate_files_);
    //     if (!result) {
    //         print_shard(task.shard, "Master: map task failed, requeing...");
    //         map_queue_.push(task);
    //     } else {
    //         print_shard(task.shard, "Master: completed map task");
    //     }
    // }
    printf("Master: Map tasks complete, intermediate files generated:\n");
    for (auto & file: intermediate_files_) {
        printf("file: %s\n", file.c_str());
    }
    return true;
}

bool WorkersPool::runReduceTasks()
{
    while (!reduce_queue_.empty()) {
        const auto task = reduce_queue_.front();
        reduce_queue_.pop();
        auto worker = getNextWorker();
        auto result = worker->executeReduceJob(task, &output_files_);
        if (!result) {
            printf("Master: reduce task %d failed, requeueing...\n", task.job_id);
            reduce_queue_.push(task);
        } else {
            printf("Master: completed reduce task %d\n", task.job_id);
        }
    }
    printf("Master: Reduce tasks complete, result files generated:\n");
    for (auto & file: output_files_) {
        printf("file: %s\n", file.c_str());
    }
    return true;
}

std::shared_ptr<WorkerClient> WorkersPool::getNextWorker()
{
    while (true) {
        for (auto & service: services_) {
            if (service->status() == WorkerStatus::AVAILABLE) {
                service->setPending();
                return service;
            }
        }
    }
    
}

bool WorkersPool::areAllWorkersDone()
{
    for (auto & service: services_) {
        auto status = service->status();
        printf("Worker %s has status %d (available is %d)\n", service->id().c_str(), status, WorkerStatus::AVAILABLE);
        if (service->busy()) {
            return false;
        }
    }
    return true;
}

bool WorkersPool::areAllWorkersBusy()
{
    for (auto & service: services_) {
        if (service->notWorking()) {
            return false;
        }
    }
    return true;
}

void WorkersPool::prepareReduceJobs()
{
    ReduceJob job;
    for (int i = 0; i < n_output_files_; i++)
    {
        job.job_id = i;
        job.n_output_files = n_output_files_;
        job.output_dir = output_dir_;
        job.user_id = user_id_;
        job.intermediate_files.clear();
        std::string file_key = std::to_string(i);
        std::string file_prefix = output_dir_ + std::string("/") + file_key + std::string("_");
        for (auto & file: intermediate_files_) {
            if (file.find(file_prefix) == 0) {
                job.intermediate_files.push_back(file);
            }
        }
        reduce_queue_.push(job);
    }
}

void WorkersPool::handleMapJobReply(MapJobReply *reply)
{
    {
        std::unique_lock<std::mutex> l(files_lock_);
        for (int i = 0; i < reply->intermediate_files_size(); i++) {
            printf("Master: adding int file %s\n", reply->intermediate_files(i).c_str());
            intermediate_files_.push_back(reply->intermediate_files(i));
        }
    }
}