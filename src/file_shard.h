#pragma once

#include <vector>
#include <string>
#include <fstream>
#include <iostream>
#include <cmath>
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
    size_t total_size = 0;

    // Calculate total size of all input files
    for (const auto& file_name : mr_spec.input_files) {
        std::ifstream file(file_name, std::ios::binary | std::ios::ate);
        if (!file.is_open()) {
            std::cerr << "Error: Unable to open file " << file_name << std::endl;
            return false;
        }
        total_size += file.tellg();
        file.close();
    }

    // Calculate the number of shards (M)
    size_t num_shards = std::ceil(static_cast<double>(total_size) / max_shard_size);

    size_t current_shard_size = 0;
    size_t current_offset = 0;
    size_t shard_start_offset = 0;

    for (const auto& file_name : mr_spec.input_files) {
        std::ifstream file(file_name, std::ios::binary);
        if (!file.is_open()) {
            std::cerr << "Error: Unable to open file " << file_name << std::endl;
            return false;
        }

        size_t file_size = 0;
        file.seekg(0, std::ios::end);
        file_size = file.tellg();
        file.seekg(0, std::ios::beg);

        while (current_offset < file_size) {
            size_t shard_end_offset = std::min(current_offset + max_shard_size - current_shard_size, file_size);

            // Align shard_end_offset to the nearest newline ('\n')
            file.seekg(shard_end_offset, std::ios::beg);
            char c;
            while (file.get(c)) {
                if (c == '\n') {
                    shard_end_offset = file.tellg();
                    break;
                }
            }

            // If no newline is found, align to the end of the file
            if (shard_end_offset == current_offset) {
                shard_end_offset = file_size;
            }

            // Add the shard
            fileShards.push_back(FileShard{file_name, current_offset, shard_end_offset});

            // Update offsets and shard size
            current_shard_size += (shard_end_offset - current_offset);
            current_offset = shard_end_offset;

            // If the current shard is full, reset for the next shard
            if (current_shard_size >= max_shard_size) {
                current_shard_size = 0;
            }
        }

        // Reset current_offset for the next file
        current_offset = 0;
        file.close();
    }

    return true;
}
