#pragma once

#include <string>
#include <iostream>
#include <fstream>
#include <vector>
#include <unordered_map>

/* CS6210_TASK Implement this data structure as per your implementation.
        You will need this when your worker is running the map task */
struct BaseMapperInternal {

    /* DON'T change this function's signature */
    BaseMapperInternal();

    /* DON'T change this function's signature */
    void emit(const std::string& key, const std::string& val);

    /* NOW you can add below, data members and member functions as per the need of your implementation */
    void set_num_reducers(int num_reducers); // Set the number of reducers
    int partition_function(const std::string& key); // Partitioning logic
    void process_file_shard(const std::string& file_name, size_t start_offset, size_t end_offset); // Process file shard

private:
    int num_reducers_; // Number of reducers
};

/* Emit function */
inline void BaseMapperInternal::emit(const std::string& key, const std::string& val) {
    int partition = partition_function(key); // Use the partition function
    std::string file_name = "intermediate_" + std::to_string(partition) + ".txt";

    // Write the key-value pair to the appropriate intermediate file
    std::ofstream file(file_name, std::ios::app);
    if (file.is_open()) {
        file << key << "\t" << val << "\n";
        file.close();
    } else {
        std::cerr << "Error: Unable to open intermediate file " << file_name << std::endl;
    }
}

/* Set the number of reducers */
inline void BaseMapperInternal::set_num_reducers(int num_reducers) {
    num_reducers_ = num_reducers;
}

/* Partitioning logic */
inline int BaseMapperInternal::partition_function(const std::string& key) {
    return std::hash<std::string>{}(key) % num_reducers_;
}

/* Process file shard */
void BaseMapperInternal::process_file_shard(const std::string& file_name, size_t start_offset, size_t end_offset) {
    std::ifstream file(file_name);
    if (!file.is_open()) {
        std::cerr << "Error: Unable to open file shard " << file_name << std::endl;
        return;
    }

    // Move to the start offset
    file.seekg(start_offset, std::ios::beg);

    std::string line;
    size_t current_offset = start_offset;

    // Retrieve the user-defined mapper instance
    auto mapper = get_mapper_from_task_factory("cs6210");

    // Read lines until the end offset is reached
    while (std::getline(file, line) && current_offset < end_offset) {
        current_offset = file.tellg(); // Update the current offset

        // Call the user-defined map function
        mapper->map(line);
    }

    file.close();
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
    void process_intermediate_files(const std::vector<std::string>& intermediate_files);
};

/* Emit function */
inline void BaseReducerInternal::emit(const std::string& key, const std::string& val) {
    std::cout << "Dummy emit by BaseReducerInternal: " << key << ", " << val << std::endl;
}

/* Process intermediate files */
void BaseReducerInternal::process_intermediate_files(const std::vector<std::string>& intermediate_files) {
    std::unordered_map<std::string, std::vector<std::string>> grouped_data;

    // Read intermediate files and group values by key
    for (const auto& file_name : intermediate_files) {
        std::ifstream file(file_name);
        if (!file.is_open()) {
            std::cerr << "Error: Reducer could not find intermediate file: " << file_name << std::endl;
            continue;
        }

        std::string line;
        while (std::getline(file, line)) {
            size_t tab_pos = line.find('\t');
            if (tab_pos == std::string::npos) continue;

            std::string key = line.substr(0, tab_pos);
            std::string value = line.substr(tab_pos + 1);

            grouped_data[key].push_back(value);
        }

        file.close();
    }

    // Retrieve the user-defined reducer instance
    auto reducer = get_reducer_from_task_factory("cs6210");

    // Call the user-defined reduce function
    for (const auto& [key, values] : grouped_data) {
        reducer->reduce(key, values);
    }
}
