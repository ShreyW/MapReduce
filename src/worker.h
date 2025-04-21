#pragma once

#include "mr_task_factory.h"
#include "mr_tasks.h"
#include "masterworker.grpc.pb.h"
#include "masterworker.pb.h"
#include <grpcpp/grpcpp.h>
#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include "file_shard.h"

class Worker : public masterworker::MasterWorkerService::Service {

    public:
        /* DON'T change the function signature of this constructor */
        Worker(std::string ip_addr_port);

        /* DON'T change this function's signature */
        bool run();

    private:
        std::string ip_addr_port_; // Worker address (ip:port)

        // gRPC service implementation
        grpc::Status ExecuteTask(grpc::ServerContext* context,
                                 const masterworker::TaskRequest* request,
                                 masterworker::TaskResult* response) override;

        // Helper functions for task handling
        void handle_map_task(const masterworker::TaskRequest* request, masterworker::TaskResult* response);
        void handle_reduce_task(const masterworker::TaskRequest* request, masterworker::TaskResult* response);
            
        // Reading of files and calling user-defined functions
        void Worker::process_file_shard(std::vector<FileShard> shard_group, BaseMapper mapper){
        void process_intermediate_file(const std::string& file_name, std::unordered_map<std::string, std::vector<std::string>>& key_value_map);
};


/* CS6210_TASK: ip_addr_port is the only information you get when started.
    You can populate your other class data members here if you want */
Worker::Worker(std::string ip_addr_port) : ip_addr_port_(std::move(ip_addr_port)) {}


/* CS6210_TASK: Here you go. once this function is called your worker's job is to keep looking for new tasks 
    from Master, complete when given one and again keep looking for the next one.
    Note that you have the access to BaseMapper's member BaseMapperInternal impl_ and 
    BaseReducer's member BaseReducerInternal impl_ directly, 
    so you can manipulate them however you want when running map/reduce tasks */
bool Worker::run() {
    std::cout << "Worker starting on " << ip_addr_port_ << std::endl;

    // Start the gRPC server
    grpc::ServerBuilder builder;
    builder.AddListeningPort(ip_addr_port_, grpc::InsecureServerCredentials());
    builder.RegisterService(this);

    std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
    std::cout << "Worker listening on " << ip_addr_port_ << std::endl;

    server->Wait();
    return true;
}


/* gRPC method to handle task execution */
grpc::Status Worker::ExecuteTask(grpc::ServerContext* context,
                                 const masterworker::TaskRequest* request,
                                 masterworker::TaskResult* response) {
    std::cout << "Worker received task: " << request->task_id() << std::endl;

    // Determine task type (map or reduce)
    if (request->task_id().find("map") != std::string::npos) {
        handle_map_task(request, response);
    } else if (request->task_id().find("reduce") != std::string::npos) {
        handle_reduce_task(request, response);
    } else {
        return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "Unknown task type");
    }

    return grpc::Status::OK;
}

// Call map on every line of the file shard
void Worker::process_file_shard(std::vector<FileShard> shard_group, BaseMapper mapper){
    for (const auto& shard : shard_group) {
        std::ifstream file(shard.file_name, std::ios::binary);
        if (!file.is_open()) {
            std::cerr << "Error: Unable to open file " << shard.file_name << std::endl;
            continue;
        }

        file.seekg(shard.start_offset, std::ios::beg);
        std::string line;
        while (std::getline(file, line)) {
            if (file.tellg() > shard.end_offset) {
                break; // Stop reading if we exceed the end offset
            }
            mapper.map(line);
        }
        file.close();
    }
}

/* Handle map tasks */
void Worker::handle_map_task(const masterworker::TaskRequest* request, masterworker::TaskResult* response) {
    std::string payload = request->payload();
    int num_reducers = request->num_reducers();

    // Parse the payload to extract multiple groups of file shards
    std::vector<std::vector<FileShard>> shard_groups;
    size_t group_start = 0, group_end;
    while ((group_end = payload.find('|', group_start)) != std::string::npos) { // Groups separated by '|'
        std::string group = payload.substr(group_start, group_end - group_start);
        std::vector<FileShard> shard_group;

        size_t shard_start = 0, shard_end;
        while ((shard_end = group.find(';', shard_start)) != std::string::npos) { // Shards separated by ';'
            std::string shard_info = group.substr(shard_start, shard_end - shard_start);
            size_t colon_pos = shard_info.find(':');
            size_t dash_pos = shard_info.find('-');
            std::string file_name = shard_info.substr(0, colon_pos);
            size_t start_offset = std::stoul(shard_info.substr(colon_pos + 1, dash_pos - colon_pos - 1));
            size_t end_offset = std::stoul(shard_info.substr(dash_pos + 1));
            shard_group.push_back(FileShard{file_name, start_offset, end_offset});
            shard_start = shard_end + 1;
        }

        shard_groups.push_back(shard_group);
        group_start = group_end + 1;
    }

    // Extract mapper ID from task_id
    int task_id = std::stoi(request->task_id().substr(4));
    BaseMapper mapper = get_mapper_from_task_factory("cs6210");
    mapper.set_num_reducers(num_reducers);
    mapper.set_task_id(task_id);

    // Process each group of file shards
    for (const auto& shard_group : shard_groups) {
        process_file_shard(shard_group, mapper);
    }

    // Flush the emit buffer to write intermediate files to disk
    mapper.flush_emit_buffer();

    // Collect intermediate file names
    for (int i = 0; i < num_reducers; ++i) {
        response->add_intermediate_files("map_" + std::to_string(task_id) + "_" + std::to_string(i) + ".txt");
    }

    response->set_task_id(request->task_id());
    response->set_user_id(request->user_id());
    std::cout << "Map task completed: " << request->task_id() << std::endl;
}


/* Handle reduce tasks */
void Worker::handle_reduce_task(const masterworker::TaskRequest* request, masterworker::TaskResult* response) {
    std::string payload = request->payload();
    std::vector<std::string> intermediate_files;
    size_t start = 0, end;

    // Parse the payload to extract intermediate file names
    while ((end = payload.find(',', start)) != std::string::npos) {
        intermediate_files.push_back(payload.substr(start, end - start));
        start = end + 1;
    }
    intermediate_files.push_back(payload.substr(start));

    // Instantiate the reducer
    BaseReducer reducer = get_reducer_from_task_factory("cs6210");

    // Aggregate key-value pairs from all intermediate files
    std::unordered_map<std::string, std::vector<std::string>> key_value_map;
    for (const auto& file : intermediate_files) {
        process_intermediate_file(file, key_value_map);
    }

    // Call the user-defined reduce function for each key
    for (const auto& [key, values] : key_value_map) {
        reducer.reduce(key, values); // User-defined reduce function
    }

    // Set the response
    response->set_task_id(request->task_id());
    response->set_user_id(request->user_id());
    std::cout << "Reduce task completed: " << request->task_id() << std::endl;
}

/* Process an intermediate file and aggregate key-value pairs */
void Worker::process_intermediate_file(const std::string& file_name, std::unordered_map<std::string, std::vector<std::string>>& key_value_map) {
    std::ifstream infile(file_name);
    if (!infile.is_open()) {
        std::cerr << "Warning: Unable to open intermediate file " << file_name << std::endl;
        return; // Ignore missing files
    }

    std::string line;
    while (std::getline(infile, line)) {
        size_t tab_pos = line.find('\t');
        if (tab_pos != std::string::npos) {
            std::string key = line.substr(0, tab_pos);
            std::string value = line.substr(tab_pos + 1);
            key_value_map[key].push_back(value); // Aggregate values for the same key
        }
    }

    infile.close();
}
