#pragma once

#include <vector>
#include <string>
#include <fstream>
#include <iostream>
#include "mapreduce_spec.h"

/* CS6210_TASK: Create your own data structure here, where you can hold information about file splits,
     that your master would use for its own bookkeeping and to convey the tasks to the workers for mapping */
struct FileShard {
    std::string file_name;  // Name of the file
    size_t start_offset;    // Start byte offset of the shard
    size_t end_offset;      // End byte offset of the shard
};

/* CS6210_TASK: Create fileshards from the list of input files, map_kilobytes etc. using mr_spec you populated  */ 
inline bool shard_files(const MapReduceSpec& mr_spec, std::vector<FileShard>& fileShards) {
    size_t max_shard_size = mr_spec.map_kilobytes * 1024; // Convert kilobytes to bytes

    for (const auto& file_name : mr_spec.input_files) {
        std::ifstream file(file_name, std::ios::binary | std::ios::ate);
        if (!file.is_open()) {
            std::cerr << "Error: Unable to open file " << file_name << std::endl;
            return false;
        }

        size_t file_size = file.tellg(); // Get the size of the file in bytes
        size_t current_offset = 0;

        // Create shards for the current file
        while (current_offset < file_size) {
            size_t shard_end = std::min(current_offset + max_shard_size, file_size);
            fileShards.push_back(FileShard{file_name, current_offset, shard_end});
            current_offset = shard_end;
        }

        file.close();
    }

    return true;
}
