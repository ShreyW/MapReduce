#pragma once

#include <string>
#include <iostream>
#include <fstream>
#include <vector>
#include <unordered_map>
#include <memory> // For smart pointers
#include <sstream> // For string stream

/* CS6210_TASK Implement this data structure as per your implementation.
        You will need this when your worker is running the map task */
struct BaseMapperInternal {

    /* DON'T change this function's signature */
    BaseMapperInternal();

    /* DON'T change this function's signature */
    void emit(const std::string& key, const std::string& val);
    

    /* NOW you can add below, data members and member functions as per the need of your implementation */
    void set_num_reducers(int num_reducers) { num_reducers_ = num_reducers; } // Set the number of reducers
    void set_task_id(int task_id) { task_id_ = task_id; }                     // Set the task ID
    void flush_emit_buffer(); //Flush the emit buffer to disk
    int partition_function(const std::string& key); // Partitioning logic

private:
    int num_reducers_; // Number of reducers
    int task_id_;      // Task ID

    // Emit buffer: one buffer per partition
    std::unordered_map<int, std::vector<std::pair<std::string, std::string>>> emit_buffer_;
};

/* Emit function */
inline void BaseMapperInternal::emit(const std::string& key, const std::string& val) {
    int partition = partition_function(key); // Use the partition function

    // Add the key-value pair to the appropriate buffer
    emit_buffer_[partition].emplace_back(key, val);
}

/* Flush the emit buffer to disk */
inline void BaseMapperInternal::flush_emit_buffer() {
    for (const auto& [partition, buffer] : emit_buffer_) {
        // Generate the file name for this partition
        std::string file_name = std::to_string(task_id_) + "_" + std::to_string(partition) + ".txt";

        // Open the file in append mode
        std::ofstream file(file_name, std::ios::app);
        if (!file.is_open()) {
            std::cerr << "Error: Unable to open intermediate file " << file_name << std::endl;
            continue;
        }

        // Write all key-value pairs in the buffer to the file
        for (const auto& [key, val] : buffer) {
            file << key << "," << val << "\n";
        }

        file.close();
    }

    // Clear the buffers after flushing
    emit_buffer_.clear();
}

/* Partitioning logic */
inline int BaseMapperInternal::partition_function(const std::string& key) {
    return std::hash<std::string>{}(key) % num_reducers_;
}

/*-----------------------------------------------------------------------------------------------*/

/* CS6210_TASK Implement this data structures per your implementation.
        You will need this when your worker is running the reduce task */
struct BaseReducerInternal {

    /* DON'T change this function's signature */
    BaseReducerInternal();

    /* DON'T change this function's signature */
    void emit(const std::string& key, const std::string& val);

    /* NOW you can add below, data members and member functions as per the need of your implementation */

    private:
        int task_id_;
};

/* Emit function */
inline void BaseReducerInternal::emit(const std::string& key, const std::string& val) {
    std::string file_name = std::to_string(task_id_) + ".txt";

    // Write the key-value pair to the output file
    std::ofstream file(file_name, std::ios::app);
    if (file.is_open()) {
        file << key << "\t" << val << "\n";
        file.close();
    } else {
        std::cerr << "Error: Unable to open output file " << file_name << std::endl;
    }
}
