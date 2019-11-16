#pragma once

#include <string>
#include <sstream>
#include <memory>
#include <vector>
#include <cstdio>
#include "mapreduce_spec.h"

struct FileShardOffset {
     std::string file;
     int start;
     int stop;

     bool operator==(const FileShardOffset & other) const {
          return this->file == other.file && this->start == other.start && this->stop == other.stop;
     }
};

/* CS6210_TASK: Create your own data structure here, where you can hold information about file splits,
     that your master would use for its own bookkeeping and to convey the tasks to the workers for mapping */
struct FileShard {
     std::vector<FileShardOffset> offsets;

     bool operator==(const FileShard & other) const {
          return this->offsets == other.offsets;
     }
};

inline int get_file_size(FILE *file)
{
     fseek(file, 0, SEEK_END);
     int size = ftell(file);
     fseek(file, 0, SEEK_SET);
     return size;
}

inline int get_file_next_boundary(FILE *file, const int pos)
{
     fseek(file, pos, SEEK_SET);
     int boundary_pos = pos;
     char buf;
     while (!feof(file)) {
          fread(&buf, sizeof(char), 1, file);
          boundary_pos += 1;
          if (buf == '\n') break;
     }
     return boundary_pos;
}

inline void print_shard(const FileShard & shard, const std::string & msg)
{
     printf("%s Shard:", msg.c_str());
     for (auto & offset: shard.offsets) {
          printf(" %s %d-%d,", offset.file.c_str(), offset.start, offset.stop);
     }
     printf("\n");
}


/* CS6210_TASK: Create fileshards from the list of input files, map_kilobytes etc. using mr_spec you populated  */ 
inline bool shard_files(const MapReduceSpec& mr_spec, std::vector<FileShard>& fileShards) {
     int shard_size = mr_spec.map_kilobytes * 1024;
     FileShard current_shard;
     FILE *file;
     int current_shard_remaining = shard_size;

     for (const auto filename : mr_spec.input_files) {
          file = fopen(filename.c_str(), "r");
          if (!file) {
               return false;
          }
          auto file_size = get_file_size(file);
          if (current_shard_remaining >= file_size) {
               current_shard.offsets.push_back({.file = filename, .start = 0, .stop = file_size - 1});
               current_shard_remaining -= file_size;
               fclose(file);
               continue;
          }
          auto current_file_pos = 0;
          while (current_file_pos < file_size) {
               int boundary = get_file_next_boundary(file, current_file_pos + current_shard_remaining - 1);
               boundary = boundary > file_size ? file_size : boundary;
               current_shard_remaining -= boundary - current_file_pos;
               current_shard.offsets.push_back({.file = filename, .start = current_file_pos, .stop = boundary - 1});
               
               if (current_shard_remaining <= 0) {
                    fileShards.push_back(std::move(current_shard));
                    current_shard_remaining = shard_size;
               }

               current_file_pos = boundary;
          }
          
          fclose(file);
     }
     if (!current_shard.offsets.empty()) {
          fileShards.push_back(std::move(current_shard));
     }
	return true;
}



inline bool read_shard(const FileShard & shard, std::vector<std::string> & out_lines)
{
     for (auto & offset : shard.offsets) {
          int size = offset.stop - offset.start + 1;
          std::unique_ptr<char> buffer(new char[size + 1]);
          FILE * file = fopen(offset.file.c_str(), "r");
          if (!file) {
               return false;
          }
          fseek(file, offset.start, SEEK_SET);
          int size_read = fread(buffer.get(), sizeof(char), size, file);
          if (size_read != size) {
               return false;
          }
          buffer.get()[size_read] = '\0';
          std::string string_buffer(buffer.get());
          std::istringstream stream(string_buffer);
          std::string line;
          while (std::getline(stream, line)) {
               out_lines.push_back(line);
          }
          fclose(file);
     }
     return true;
}
