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
    return runTasks<MapJob>(map_queue_,
        [this](std::shared_ptr<WorkerClient>worker, const MapJob & task) { this->scheduleMapTask(worker, task); },
        [this]() {
            printf("Master: Map tasks complete, intermediate files generated:\n");
            for (auto & file: this->intermediate_files_) {
                printf("file: %s\n", file.c_str());
            }
        });
}

void WorkersPool::scheduleMapTask(std::shared_ptr<WorkerClient> worker, const MapJob & task)
{
    auto result = worker->executeMapJob(task,
        [this](MapJobReply *reply) {this->handleMapJobReply(reply); });

    if (!result) {
        std::unique_lock<std::mutex> l(queue_lock_);
        print_shard(task.shard, "Master: map task failed, requeing...");
        map_queue_.push(task);
    } else {
        print_shard(task.shard, "Master: completed map task");
    }
    cond_task_done_.notify_one();
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

bool WorkersPool::runReduceTasks()
{
    return runTasks<ReduceJob>(reduce_queue_,
        [this](std::shared_ptr<WorkerClient>worker, const ReduceJob & task) { this->scheduleReduceTask(worker, task); },
        [this]() {
            printf("Master: Reduce tasks complete, final result files generated:\n");
            for (auto & file: this->output_files_) {
                printf("file: %s\n", file.c_str());
            }
        });
}

template<typename T>
bool WorkersPool::runTasks(std::queue<T> &queue,
    std::function<void(std::shared_ptr<WorkerClient> worker, const T & task)> runTask,
    std::function<void(void)> onComplete)
{
    auto complete = false;
    while (1) {
        {
            std::unique_lock<std::mutex> l(queue_lock_);
            while (queue.empty()) {
                cond_task_done_.wait(l);
                if (queue.empty() && areAllWorkersDone()) {
                    complete = true;
                    break;
                }
            }
            if (complete) {
                break;
            }
            const auto task = std::move(queue.front());
            queue.pop();
            auto worker = getNextWorker();
            std::function<void(void)> runner = [worker, task, runTask]() { runTask(worker, task); };
            threadpool_->enqueue_task(runner); 
        }
    }
    onComplete();
    return true;
}

void WorkersPool::scheduleReduceTask(std::shared_ptr<WorkerClient> worker, const ReduceJob &task)
{
    auto result = worker->executeReduceJob(task,
        [this](ReduceJobReply *reply) { this->handleReduceJobReply(reply); });
    
    if (!result) {
        std::unique_lock<std::mutex> l(queue_lock_);
        printf("Master: reduce task %d failed, requeueing...\n", task.job_id);
        reduce_queue_.push(task);
    } else {
        printf("Master: completed reduce task %d\n", task.job_id);
    }
    cond_task_done_.notify_one();
}

void WorkersPool::handleReduceJobReply(ReduceJobReply *reply)
{
    std::unique_lock<std::mutex> l(files_lock_);
    output_files_.push_back(reply->output_file());
}

std::shared_ptr<WorkerClient> WorkersPool::getNextWorker()
{
    // index is made static to remember the last
    // service visited, and start visiting from the next
    // one in the list to ensure a fair round-robin traversal
    // also ensures that a worker that err'd on a task
    // is not retried until all other available workers have
    // been retried
    static int last_index = 0;
    while (true) {
        last_index = last_index % services_.size();
        for (; last_index < services_.size(); last_index++) {
            auto service = services_.at(last_index);
            if (service->acquireForJob()) {
                return service;
            }
        }
    }
    
}

bool WorkersPool::areAllWorkersDone()
{
    for (auto & service: services_) {
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

