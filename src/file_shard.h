#pragma once

#include <vector>
#include <string>
#include <fstream>
#include <iostream>
#include <cmath>
#include "mapreduce_spec.h"

/* Updated FileShard structure to hold multiple (filename, start, end) tuples */
struct FileShard {
    std::vector<std::tuple<std::string, size_t, size_t>> file_segments; // Vector of (filename, start_offset, end_offset)
};

/* CS6210_TASK: Create fileshards from the list of input files, map_kilobytes etc. using mr_spec you populated  */
inline bool shard_files(const MapReduceSpec& mr_spec, std::vector<FileShard>& file_shards) {
    size_t max_shard_size = mr_spec.map_kilobytes * 1024; // Convert kilobytes to bytes

    size_t current_shard_size = 0;
    FileShard current_shard;

    for (const auto& file_name : mr_spec.input_files) {
        std::ifstream file(file_name, std::ios::binary);
        if (!file.is_open()) {
            std::cerr << "Error in shard creation: Unable to open file " << file_name << std::endl;
            return false;
        }

        size_t current_offset = 0;
        size_t file_size = 0;

        file.seekg(0, std::ios::end);
        file_size = file.tellg();
        file.seekg(0, std::ios::beg);

        char c;
        size_t shard_start_offset = current_offset;

        while (current_offset < file_size) {
            file.seekg(current_offset, std::ios::beg);
            size_t line_end_offset = current_offset;

            // Read character by character until a newline or EOF
            while (file.get(c)) {
                ++line_end_offset;
                if (c == '\n' || file.eof()) {
                    break;
                }
            }

            // Add the current line to the shard
            size_t line_size = line_end_offset - current_offset;
            current_shard_size += line_size;


            // If the shard exceeds the max size, finalize it
            if (current_shard_size > max_shard_size) {
                current_shard.file_segments.emplace_back(file_name, shard_start_offset, line_end_offset);
                // std::cerr<< "File: " << file_name << " Shard size: " << current_shard_size << std::endl;
                // std::cerr<< "Start offset " << shard_start_offset << " End offset " << line_end_offset << std::endl;
                file_shards.push_back(current_shard);
                current_shard = FileShard(); // Start a new shard
                current_shard_size = 0;
                shard_start_offset = line_end_offset; // point to the next char after the newline
            }

            current_offset = line_end_offset;
        }

        // Handle the case where the last shard of this file is incomplete
        if (current_shard_size > 0) {
            current_shard.file_segments.emplace_back(file_name, shard_start_offset, current_offset);
            // std::cerr<< "File: " << file_name << " Shard size: " << current_shard_size << std::endl;
            // std::cerr<< "Start offset " << shard_start_offset << " End offset " << current_offset << std::endl;
        }

        file.close();
    }

    // Handle the case where the last shard spans multiple files
    if (!current_shard.file_segments.empty()) {
        file_shards.push_back(current_shard);
    }

    return true;
}
