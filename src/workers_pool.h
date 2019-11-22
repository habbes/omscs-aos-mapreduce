#pragma once

#include <vector>
#include <queue>
#include <string>
#include <mutex>
#include <condition_variable>
#include "masterworker.pb.h"
#include "worker_client.h"
#include "file_shard.h"
#include "threadpool.h"


class WorkersPool
{
public:
    WorkersPool(const MapReduceSpec & spec);
    void addMapTask(FileShard shard);
    bool runMapTasks();
    bool runReduceTasks();
    void prepareReduceJobs();
    void handleMapJobReply(masterworker::MapJobReply *reply);
private:
    std::shared_ptr<WorkerClient> getNextWorker();
    bool areAllWorkersBusy();
    bool areAllWorkersDone();
    

    std::vector<std::shared_ptr<WorkerClient>> services_;
    int next_task_id_;
    std::queue<MapJob> map_queue_;
    int n_output_files_;
    std::string output_dir_;
    std::string user_id_;
    std::vector<std::string> intermediate_files_;
    std::queue<ReduceJob> reduce_queue_;
    std::vector<std::string> output_files_;
    std::unique_ptr<threadpool> threadpool_;
    std::mutex queue_lock_;
    std::condition_variable cond_task_done_;
    std::mutex files_lock_;
};
