#pragma once

#include <vector>
#include <queue>
#include <string>
#include "worker_client.h"
#include "file_shard.h"


class WorkersPool
{
public:
    WorkersPool(const MapReduceSpec & spec);
    void addMapTask(FileShard shard);
    bool runMapTasks();
    bool runReduceTasks();
    void prepareReduceJobs();
private:
    std::unique_ptr<WorkerClient> & getNextWorker();

    std::vector<std::unique_ptr<WorkerClient>> services_;
    int next_task_id_;
    std::queue<MapJob> map_queue_;
    int n_output_files_;
    std::string output_dir_;
    std::string user_id_;
    std::vector<std::string> intermediate_files_;
    std::queue<ReduceJob> reduce_queue_;
    std::vector<std::string> output_files_;
};
