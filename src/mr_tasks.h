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
    void set_user_id(std::string user_id) { user_id_ = user_id; }                     // Set the user ID
    void flush_emit_buffer(); // Flush the emit buffer to disk (intermediate files)
    int partition_function(const std::string& key); // Partitioning logic

private:
    int num_reducers_; // Number of reducers
    std::string user_id_;

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
    //Print the emit buffer to the console
    for (const auto& [partition, buffer] : emit_buffer_) {
        // std::cout << "Partition " << partition << " emit buffer contents:" << std::endl;
        // for (const auto& [key, val] : buffer) {
        //     std::cout << key << "," << val << std::endl;
        // }
    }

    for (const auto& [partition, buffer] : emit_buffer_) {
        // Generate the file name for this partition
        std::string file_name = "intermediate_" + user_id_ + "_" + std::to_string(partition) + ".txt";

        // Open the file in append mode
        std::ofstream file(file_name, std::ios::out | std::ios::app);
         // Check if the file opened successfully
        if (!file.is_open()) {
            std::cerr << "Error in mapper: Unable to open or create intermediate file " << file_name << std::endl;
            continue;
        }

        // Write all key-value pairs in the buffer to the file
        for (const auto& [key, val] : buffer) {
            file << key << "," << val << "\n";
        }

        file.flush();
        file.close();
    }

    // Clear the buffers after flushing
    emit_buffer_.clear();
}

/* Partitioning logic */
inline int BaseMapperInternal::partition_function(const std::string& key) {
    return std::hash<std::string>{}(key) % num_reducers_;
}


 /* CS6210_TASK Implement this function */
 inline BaseMapperInternal::BaseMapperInternal() {
    // Initializing done somewhere else
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
    void flush_emit_buffer(); // Flush the emit buffer to disk (output files)
    void set_user_id(std::string user_id) { user_id_ = user_id; }
    void set_output_dir(std::string output_dir) { output_dir_ = output_dir; }                  

    private:
        std::string user_id_;
        std::string output_dir_;
        std::vector<std::pair<std::string, std::string>> emit_buffer_; // Emit buffer: one buffer for all key-value pairs
};

/* Emit function */
inline void BaseReducerInternal::emit(const std::string& key, const std::string& val) {
    // Add the key-value pair to the appropriate buffer
    // printf("Emitting key-value pair: %s, %s\n", key.c_str(), val.c_str());
    emit_buffer_.emplace_back(key, val);
}

/* Flush the emit buffer to disk */
inline void BaseReducerInternal::flush_emit_buffer() {
    // Print the emit buffer to the console
    // printf("Emit buffer contents:\n");
    // for (const auto& [key, val] : emit_buffer_) {
    //     std::cout << key << "," << val << std::endl;
    // }
    //Generate the file name for this partition
    std::string file_name = output_dir_ + "/" + user_id_ + ".txt";
    
    // Open the file in append mode
    std::ofstream file(file_name, std::ios::app | std::ios::out);
    if (!file.is_open()) {
        std::cerr << "Error in reducer: Unable to open output file " << file_name << std::endl;
        return;
    }

    // Write all key-value pairs in the buffer to the file
    for (const auto& [key, val] : emit_buffer_) {
        file << key << " " << val << "\n";
    }
    
    file.flush();
    file.close();

    // Clear the buffers after flushing
    emit_buffer_.clear();
}


 /* CS6210_TASK Implement this function */
 inline BaseReducerInternal::BaseReducerInternal() {
    // Initializing done somewhere else
 }
