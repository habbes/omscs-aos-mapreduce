#pragma once

#include <vector>
#include <string>
#include "worker_client.h"


class WorkersPool
{
public:
    WorkersPool(const std::vector<std::string> & addresses);

private:
    std::vector<std::unique_ptr<WorkerClient>> services_;
};
