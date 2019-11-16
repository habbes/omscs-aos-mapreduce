#pragma once

#include <mr_task_factory.h>
#include "mr_tasks.h"

#include <cstdio>
#include <memory>
#include <grpc/grpc.h>
#include <grpcpp/grpcpp.h>
#include <grpcpp/security/credentials.h>
#include "masterworker.grpc.pb.h"

#include "file_shard.h"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;

namespace service = masterworker;
using service::MapJobRequest;
using service::MapJobReply;
using service::ReduceJobRequest;
using service::ReduceJobReply;

class WorkerService final : public service::Worker::Service {
public:
	WorkerService(std::shared_ptr<BaseMapper> mapper) : mapper_(mapper) {

	}

	Status ExecuteMapJob(ServerContext *context, const MapJobRequest *request, MapJobReply *reply) override {
		FileShard shard;
		for (int i = 0; i < request->offsets_size(); i++) {
			auto offset = request->offsets(i);
			shard.offsets.push_back({
				.file = offset.file(),
				.start = offset.start(),
				.stop = offset.stop()
			});
		}
		print_shard(shard, "Worker: received map job");

		std::vector<std::string> records;
		bool result = read_shard(shard, records);
		if (!result) {
			print_shard(shard, "Worker: FAILED to read shard");
			reply->set_success(false);
			return Status::CANCELLED;
		}

		for (const auto & record : records) {
			mapper_->map(record);
		}
		
		reply->set_success(true);
		return Status::OK;
	}

	Status ExecuteReduceJob(ServerContext *context, const ReduceJobRequest *request, ReduceJobReply *reply) override {

	}
private:
	std::shared_ptr<BaseMapper> mapper_;
};

/* CS6210_TASK: Handle all the task a Worker is supposed to do.
	This is a big task for this project, will test your understanding of map reduce */
class Worker : public service::Worker::Service {

	public:
		/* DON'T change the function signature of this constructor */
		Worker(std::string ip_addr_port);

		/* DON'T change this function's signature */
		bool run();

		Status ExecuteMapJob(ServerContext *context, const MapJobRequest *request, MapJobReply *reply) override;
		Status ExecuteReduceJob(ServerContext *context, const ReduceJobRequest *request, ReduceJobReply *reply) override;
		
	private:
		/* NOW you can add below, data members and member functions as per the need of your implementation*/
		bool handleShard(const FileShard & shard, int n_output_files,
			const std::string & output_dir);
		bool writeMapperResults(std::shared_ptr<BaseMapper> mapper, int n_output_files,
			const std::string & output_dir);
		std::string getMapperResultsFilename(const std::string & key, int n_output_files,
			const std::string & output_dir);


		std::string address_;
		std::string worker_id_;
};


/* CS6210_TASK: ip_addr_port is the only information you get when started.
	You can populate your other class data members here if you want */
Worker::Worker(std::string ip_addr_port) {
	address_ = ip_addr_port;
	worker_id_ = address_;
	int colon_pos = worker_id_.find(':');
	if (colon_pos != std::string::npos) {
		worker_id_.replace(colon_pos, 1, "_");
	}
}

extern std::shared_ptr<BaseMapper> get_mapper_from_task_factory(const std::string& user_id);
extern std::shared_ptr<BaseReducer> get_reducer_from_task_factory(const std::string& user_id);

/* CS6210_TASK: Here you go. once this function is called your woker's job is to keep looking for new tasks 
	from Master, complete when given one and again keep looking for the next one.
	Note that you have the access to BaseMapper's member BaseMapperInternal impl_ and 
	BaseReduer's member BaseReducerInternal impl_ directly, 
	so you can manipulate them however you want when running map/reduce tasks*/
bool Worker::run() {
	/*  Below 5 lines are just examples of how you will call map and reduce
		Remove them once you start writing your own logic */ 
	// std::cout << "worker.run(), I 'm not ready yet" <<std::endl;
	// auto mapper = get_mapper_from_task_factory("cs6210");
	// mapper->map("I m just a 'dummy', a \"dummy line\"");
	// auto reducer = get_reducer_from_task_factory("cs6210");
	// reducer->reduce("dummy", std::vector<std::string>({"1", "1"}));
	// return true;
	
	// WorkerService service(mapper);
	ServerBuilder builder;

	builder.AddListeningPort(address_, grpc::InsecureServerCredentials());
	builder.RegisterService(this);
	std::unique_ptr<Server> server(builder.BuildAndStart());

	printf("Worker listening on %s\n", address_.c_str());

	server->Wait();
	printf("Worker %s: done\n", address_.c_str());

	return true;
}

Status Worker::ExecuteMapJob(ServerContext *context, const MapJobRequest *request, MapJobReply *reply)
{
	FileShard shard;
	for (int i = 0; i < request->offsets_size(); i++) {
		auto offset = request->offsets(i);
		shard.offsets.push_back({
			.file = offset.file(),
			.start = offset.start(),
			.stop = offset.stop()
		});
	}
	print_shard(shard, "Worker: received map job");

	bool result = handleShard(shard, request->n_output_files(), request->output());
	if (!result) {
		print_shard(shard, "Worker: FAILED to read shard");
		reply->set_success(false);
		return Status::CANCELLED;
	}

	reply->set_success(true);
	return Status::OK;
}

Status Worker::ExecuteReduceJob(ServerContext *context, const ReduceJobRequest *request, ReduceJobReply *reply)
{

}

bool Worker::handleShard(const FileShard & shard, int n_output_files, const std::string & output_dir)
{
	auto mapper = get_mapper_from_task_factory("cs6210");
	std::vector<std::string> records;
	bool result = read_shard(shard, records);
	if (!result) return false;

	for (const auto & record : records) {
		mapper->map(record);
	}

	return writeMapperResults(mapper, n_output_files, output_dir);

	return true;
}

inline void close_open_files(std::unordered_map<std::string, FILE *> & open_files)
{
	for (auto & file_item: open_files) {
		fclose(file_item.second);
	}
}

bool Worker::writeMapperResults(std::shared_ptr<BaseMapper> mapper, int n_output_files, const std::string & output_dir)
{
	auto & mapped_values = mapper->impl_->emitted_values_;
	std::string filename;
	std::unordered_map<std::string, FILE *> open_files;
	FILE *file;
	for (auto & item : mapped_values) {
		filename = getMapperResultsFilename(item.first, n_output_files, output_dir);
		auto file_item = open_files.find(filename);
		if (file_item == open_files.end()) {
			file = fopen(filename.c_str(), "w");
			if (!file) {
				close_open_files(open_files);
				return false;
			}
			open_files.insert(std::make_pair(filename, file));
		} else {
			file = file_item->second;
		}
		fwrite(item.first.c_str(), sizeof(char), item.first.size(), file);
		fwrite(" ", sizeof(char), 1, file);
		fwrite(item.second.c_str(), sizeof(char), item.second.size(), file);
		fwrite("\n", sizeof(char), 1, file);
	}

	close_open_files(open_files);

	return true;
}

std::string Worker::getMapperResultsFilename(const std::string & key, int n_output_files, const std::string & output_dir)
{
	std::hash<std::string> hash;
	int prefix = hash(key) % n_output_files;
	std::string filename = output_dir + "/" + std::to_string(prefix) + "_" + worker_id_ + "_temp.txt";
	return filename;
}