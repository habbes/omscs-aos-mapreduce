#pragma once


#include "mapreduce_spec.h"
#include "file_shard.h"

#include "workers_pool.h"


/* CS6210_TASK: Handle all the bookkeeping that Master is supposed to do.
	This is probably the biggest task for this project, will test your understanding of map reduce */
class Master {

	public:
		/* DON'T change the function signature of this constructor */
		Master(const MapReduceSpec&, const std::vector<FileShard>&);

		/* DON'T change this function's signature */
		bool run();

	private:
		/* NOW you can add below, data members and member functions as per the need of your implementation*/
		MapReduceSpec spec_;
		std::vector<FileShard> shards_;

};


/* CS6210_TASK: This is all the information your master will get from the framework.
	You can populate your other class data members here if you want */
Master::Master(const MapReduceSpec& mr_spec, const std::vector<FileShard>& file_shards)
	: spec_(mr_spec), shards_(file_shards) {
}


/* CS6210_TASK: Here you go. once this function is called you will complete whole map reduce task and return true if succeeded */
bool Master::run() {
	puts("Master: STARTED!");
	bool result;
	WorkersPool workers(spec_.worker_ipaddr_ports);
	printf("Master: Number of map jobs %lu\n", shards_.size());
	for (const auto & shard : shards_) {
		workers.addMapTask(shard);
	}
	result = workers.runMapTasks();
	if (!result) {
		puts("Master: FAILED!");
		return false;
	}
	puts("Master: DONE!");
	return true;
}