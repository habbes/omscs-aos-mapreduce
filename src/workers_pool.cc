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