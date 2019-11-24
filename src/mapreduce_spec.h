#pragma once

#include <string>
#include <vector>
#include <fstream>


/* CS6210_TASK: Create your data structure here for storing spec from the config file */
struct MapReduceSpec {
	int n_workers;
	std::vector<std::string> input_files;
	std::vector<std::string> worker_ipaddr_ports;
	std::string output_dir;
	int n_output_files;
	int map_kilobytes;
	std::string user_id;
};

inline void parse_comma_separated_list(const std::string & value, std::vector<std::string> & list)
{
	int item_start = 0;
	while (item_start < value.size()) {
		auto item_stop = value.find(',', item_start);
		if (item_stop == std::string::npos) {
			item_stop = value.size();
		}
		auto item = value.substr(item_start, item_stop - item_start);
		list.push_back(item);
		item_start = item_stop + 1;
	}
}


/* CS6210_TASK: Populate MapReduceSpec data structure with the specification from the config file */
inline bool read_mr_spec_from_config_file(const std::string& config_filename, MapReduceSpec& mr_spec) {
	std::ifstream file(config_filename);
	std::string line;
	while (std::getline(file, line)) {
		if (line.size() == 0) continue; // ignore empty lines
		if (line.at(0) == '#') continue; // ignore comment lines
		if (line.at(0) == ';') continue;
		auto boundary = line.find('=');
		if (boundary == std::string::npos) continue; // ignore invalid key-val pair

		auto key = line.substr(0, boundary);
		auto value = line.substr(boundary + 1);
		if (key == std::string("n_workers")) {
			mr_spec.n_workers = std::stoi(value);
		}
		else if (key == std::string("output_dir")) {
			mr_spec.output_dir = value;
		}
		else if (key == std::string("n_output_files")) {
			mr_spec.n_output_files = std::stoi(value);
		}
		else if (key == std::string("map_kilobytes")) {
			mr_spec.map_kilobytes = std::stoi(value);
		}
		else if (key == std::string("user_id")) {
			mr_spec.user_id = value;
		}
		else if (key == std::string("worker_ipaddr_ports")) {
			parse_comma_separated_list(value, mr_spec.worker_ipaddr_ports);
		}
		else if (key == std::string("input_files")) {
			parse_comma_separated_list(value, mr_spec.input_files);
		}
	}
	return true;
}


/* CS6210_TASK: validate the specification read from the config file */
inline bool validate_mr_spec(const MapReduceSpec& mr_spec) {
	if (mr_spec.input_files.size() == 0) {
		return false;
	}
	if (mr_spec.map_kilobytes <= 0) {
		return false;
	}
	if (mr_spec.n_output_files <= 0) {
		return false;
	}
	if (mr_spec.output_dir.empty()) {
		return false;
	}
	if (mr_spec.output_dir.at(mr_spec.output_dir.size() - 1) == '/') {
		return false;
	}
	if (mr_spec.user_id.size() == 0) {
		return false;
	}
	if (mr_spec.worker_ipaddr_ports.size() == 0) {
		return false;
	}
	if (mr_spec.n_workers <= 0) {
		return false;
	}
	if (mr_spec.n_workers != mr_spec.worker_ipaddr_ports.size()) {
		return false;
	}
	return true;
}
