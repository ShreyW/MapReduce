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


// TODO: redo this function
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

        size_t file_size = 0;
        file.seekg(0, std::ios::end);
        file_size = file.tellg();
        file.seekg(0, std::ios::beg);

        std::cout << "File size of " << file_name << ": " << file_size << " bytes" << std::endl;

        size_t current_offset = 0;

        while (current_offset < file_size) {
            size_t shard_end_offset = std::min(current_offset + max_shard_size - current_shard_size, file_size);
            std::cout << "Beggining of the loop. Current offset: " << current_offset << ", Shard end offset: " << shard_end_offset << std::endl;


            // Align shard_end_offset to the nearest newline ('\n')
            file.seekg(current_offset, std::ios::beg);
            char c;
            while (file.get(c)) {
                std::cout << "Current character: " << c << std::endl;
                if (c == '\n' || c == EOF) {
                    shard_end_offset = file.tellg();
                    std::cout << "On new line or EOF. Current offset: " << current_offset << ", Shard end offset: " << shard_end_offset << std::endl;
                    
                    // If no newline is found, align to the end of the file
                    if (shard_end_offset == current_offset) {
                        shard_end_offset = file_size;
                        std::cout << "No new line found. Aligning to end of file. Current offset: " << current_offset << ", Shard end offset: " << shard_end_offset << std::endl;
                    }
                }
            }

        
            // If the current shard is full, finalize it and start a new shard
            if (current_shard_size >= max_shard_size) {
                 // Add the current file segment to the shard
                current_shard.file_segments.emplace_back(file_name, current_offset, shard_end_offset);
                file_shards.push_back(current_shard);
                current_shard = FileShard(); // Start a new shard
                current_shard_size = 0;
                std::cout << "Current offset when new shard: " << current_offset << std::endl;
            }

            // Update offsets and shard size
            current_shard_size += (shard_end_offset - current_offset);
            std::cout << "Current shard size: " << current_shard_size << " bytes" << std::endl;
            current_offset = shard_end_offset;
            std::cout << "Current offset after adding segment: " << current_offset << std::endl;
        }

        file.close();
        std::cout<<"File: " << file_name << ", Size: " << file_size << " bytes" << std::endl;
        std::cout<<"Current Shard Size: " << current_shard_size << " bytes" << std::endl;
        std::cout<<"Current Shard Segments: " << current_shard.file_segments.size() << std::endl;
    }

    // Add the last shard if it contains any data
    if (!current_shard.file_segments.empty()) {
        file_shards.push_back(current_shard);
    }

    return true;
}
