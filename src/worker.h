#pragma once

#include <mr_task_factory.h>
#include "mr_tasks.h"

#include <cstdio>
#include <memory>
#include <utility>
#include <unordered_set>
#include <algorithm>
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

typedef std::pair<std::string, std::string> key_value_pair_t;


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
		bool handleMapShard(const FileShard & shard, const MapJobRequest *request,
			std::unordered_set<std::string> & result_files);
		bool writeMapperResults(std::shared_ptr<BaseMapper> mapper, const MapJobRequest *request,
			std::unordered_set<std::string> & result_files);
		std::string getMapperResultsFilename(const std::string & key, const MapJobRequest *request);
		bool readFilesToReduce(const ReduceJobRequest *request,
			std::vector<key_value_pair_t> & key_value_pairs);
		bool handleReduceKeyValuePairs(const ReduceJobRequest *request,
			std::vector<key_value_pair_t> & key_value_pairs, std::string & output_file);
		bool writeReducerResults(std::shared_ptr<BaseReducer> reducer, const ReduceJobRequest *request,
			std::string & output_file);

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
	std::unordered_set<std::string> result_files;
	bool result = handleMapShard(shard, request, result_files);
	if (!result) {
		print_shard(shard, "Worker: FAILED to read shard");
		reply->set_success(false);
		return Status::OK;
	}

	reply->set_success(true);
	for (auto & file: result_files) {
		auto interm_file = reply->add_intermediate_files();
		*interm_file = file;
	}
	return Status::OK;
}

Status Worker::ExecuteReduceJob(ServerContext *context, const ReduceJobRequest *request, ReduceJobReply *reply)
{
	printf("Worker: received reduce job %s\n", request->key().c_str());
	std::vector<key_value_pair_t> key_value_pairs;
	std::string output_file;

	auto result = readFilesToReduce(request, key_value_pairs)
		&& handleReduceKeyValuePairs(request, key_value_pairs, output_file);

	if (!result) {
		reply->set_success(false);
		return Status::OK;
	}
	printf("Worker: completed reduce job %s, key val pairs %d, output %s\n",
		request->key().c_str(), (int)key_value_pairs.size(), output_file.c_str());

	reply->set_output_file(output_file);
	reply->set_success(true);
	return Status::OK;
}

bool Worker::handleMapShard(
	const FileShard & shard, const MapJobRequest *request,
	std::unordered_set<std::string> & result_files)
{
	auto mapper = get_mapper_from_task_factory(request->user_id());
	std::vector<std::string> records;
	bool result = read_shard(shard, records);
	if (!result) return false;

	for (const auto & record : records) {
		mapper->map(record);
	}

	return writeMapperResults(mapper, request, result_files);
}

inline void close_open_files(std::unordered_map<std::string, FILE *> & open_files)
{
	for (auto & file_item: open_files) {
		fclose(file_item.second);
	}
}

bool Worker::writeMapperResults(std::shared_ptr<BaseMapper> mapper, const MapJobRequest *request,
	std::unordered_set<std::string> & result_files)
{
	auto & mapped_values = mapper->impl_->emitted_values_;
	std::string filename;
	std::unordered_map<std::string, FILE *> open_files;
	FILE *file;
	for (auto & item : mapped_values) {
		filename = getMapperResultsFilename(item.first, request);
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
		result_files.insert(filename);
	}


	close_open_files(open_files);

	return true;
}

std::string Worker::getMapperResultsFilename(const std::string & key, const MapJobRequest *request)
{
	std::hash<std::string> hash;
	int prefix = hash(key) % request->n_output_files();
	std::string filename =
		request->output_dir() + "/"
		+ std::to_string(prefix) + "_"
		+ std::to_string(request->job_id()) + "_"
		+ worker_id_
		+ "_temp.txt";
	return filename;
}

bool Worker::readFilesToReduce(const ReduceJobRequest *request, std::vector<key_value_pair_t> & key_value_pairs)
{
	std::string line;
	std::string filename;
	for (int i = 0; i < request->intermediate_files_size(); i++) {
		filename = request->intermediate_files(i);
		std::ifstream stream(filename);

		while (std::getline(stream, line)) {
			if (line.size() == 0) continue; // skip blank lines
			int space_pos = line.find(' ');
			if (space_pos == std::string::npos) {
				// invalid file
				return false;
			}
			std::string key = line.substr(0, space_pos);
			std::string value = line.substr(space_pos + 1);
			key_value_pairs.push_back(std::make_pair(key, value));
		}
	}
	auto comparer = [](std::pair<std::string, std::string> & left, std::pair<std::string, std::string> & right) {
		return left.first < right.first;
	};
	std::sort(key_value_pairs.begin(), key_value_pairs.end(), comparer);
	return true;
}

bool Worker::handleReduceKeyValuePairs(const ReduceJobRequest *request, std::vector<key_value_pair_t> & key_value_pairs, std::string & output_file)
{
	if (key_value_pairs.empty()) {
		return true;
	}

	std::string cur_key = key_value_pairs.at(0).first;
	std::vector<std::string> cur_values;
	auto reducer = get_reducer_from_task_factory(request->user_id());

	
	for (auto & pair: key_value_pairs) {
		if (pair.first != cur_key) {
			reducer->reduce(cur_key, cur_values);
			cur_key = pair.first;
			cur_values.clear();
		}
		cur_values.push_back(pair.second);
	}
	// reduce final key's values
	reducer->reduce(cur_key, cur_values);

	return writeReducerResults(reducer, request, output_file);
}

bool Worker::writeReducerResults(std::shared_ptr<BaseReducer> reducer, const ReduceJobRequest *request, std::string & output_file)
{
	output_file = request->output_dir()
		+ std::string("/")
		+ request->key()
		+ "_result.txt";
	
	FILE *file = fopen(output_file.c_str(), "w");
	if (!file) {
		return false;
	}

	for (auto & result : reducer->impl_->emitted_values_) {
		fwrite(result.first.c_str(), sizeof(char), result.first.size(), file);
		fwrite(" ", sizeof(char), 1, file);
		fwrite(result.second.c_str(), sizeof(char), result.second.size(), file);
		fwrite("\n", sizeof(char), 1, file);
	}
	
	fclose(file);
	return true;
}