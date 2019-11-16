#pragma once

#include <vector>
#include <queue>
#include <string>
#include "worker_client.h"
#include "file_shard.h"


class WorkersPool
{
public:
    WorkersPool(const std::vector<std::string> & addresses);
    void addMapTask(FileShard shard);
    bool runMapTasks();
private:
    std::unique_ptr<WorkerClient> & getNextWorker();

    std::vector<std::unique_ptr<WorkerClient>> services_;
    std::queue<FileShard> map_queue_;
};