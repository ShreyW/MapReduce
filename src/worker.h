#pragma once

#include <mr_task_factory.h>
#include "mr_tasks.h"
#include "file_shard.h"

#include "masterworker.grpc.pb.h"
#include "masterworker.pb.h"

#include <grpcpp/grpcpp.h>
#include <iostream>
#include <fstream>
#include <vector>
#include <string>


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

        grpc::Status PingWorker(grpc::ServerContext* context,
                                const masterworker::PingRequest* request,
                                masterworker::PingResponse* response) override;

        // Helper functions for task handling
        void handle_map_task(const masterworker::TaskRequest* request, masterworker::TaskResult* response);
        void handle_reduce_task(const masterworker::TaskRequest* request, masterworker::TaskResult* response);
        void process_file_shard(FileShard shard, std::shared_ptr<BaseMapper> mapper);
        void process_intermediate_file(const std::string& file_name, std::map<std::string, std::vector<std::string>>& key_value_map);
};


/* CS6210_TASK: ip_addr_port is the only information you get when started.
    You can populate your other class data members here if you want */
Worker::Worker(std::string ip_addr_port){
    ip_addr_port_ = ip_addr_port;
}

extern std::shared_ptr<BaseMapper> get_mapper_from_task_factory(const std::string& user_id);
extern std::shared_ptr<BaseReducer> get_reducer_from_task_factory(const std::string& user_id);

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

    // Heartbeat missing

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
void Worker::process_file_shard(FileShard shard, std::shared_ptr<BaseMapper> mapper){
    for (const auto& file_segment : shard.file_segments) {
        std::string filename = std::get<0>(file_segment);
        size_t start_offset = std::get<1>(file_segment);
        size_t end_offset = std::get<2>(file_segment);

        std::ifstream file(filename, std::ios::binary);
        if (!file.is_open()) {
            std::cerr << "Error: Unable to open file " << filename << std::endl;
            continue;
        }

        file.seekg(start_offset, std::ios::beg);
        std::string line;
        while (std::getline(file, line)) {
            if (file.tellg() > end_offset) {
                break; // Stop reading if we exceed the end offset
            }
            mapper->map(line);
        }
        file.close();
    }
}

/* Handle map tasks */
void Worker::handle_map_task(const masterworker::TaskRequest* request, masterworker::TaskResult* response) {
    std::string payload = request->payload();
    int num_reducers = request->num_reducers();
    std::string user_id = request->user_id();

    // Parse the payload to extract multiple groups of file shards
    // payload structure: filename1:start_offset1-end_offset1;filename2:start_offset2-end_offset2;...
    FileShard shard;
    size_t shard_start = 0, shard_end;
    while ((shard_end = payload.find(';', shard_start)) != std::string::npos) { // Shards separated by ';'
        std::string shard_info = payload.substr(shard_start, shard_end - shard_start);
        size_t colon_pos = shard_info.find(':');
        size_t dash_pos = shard_info.find('-');
        std::string file_name = shard_info.substr(0, colon_pos);
        size_t start_offset = std::stoul(shard_info.substr(colon_pos + 1, dash_pos));
        size_t end_offset = std::stoul(shard_info.substr(dash_pos + 1, shard_end-1));
        shard.file_segments.emplace_back(file_name, start_offset, end_offset);
        shard_start = shard_end + 1;
    }

    // Extract mapper ID from task_id
    int task_id = std::stoi(request->task_id().substr(4));
    auto mapper = get_mapper_from_task_factory("cs6210");
    auto mapper_impl = mapper->impl_;
    
    mapper_impl->set_num_reducers(num_reducers);
    mapper_impl->set_user_id(user_id);

    // Process each group of file shards
    process_file_shard(shard, mapper);

    // Flush the emit buffer to write intermediate files to disk
    mapper_impl->flush_emit_buffer();

    response->set_task_id(request->task_id());
    response->set_user_id(request->user_id());
    std::cout << "Map task completed: " << request->task_id() << std::endl;
}

/* Process an intermediate file and aggregate key-value pairs */
// This takes care of sorting: the intermediate files are already sorted by the key_value_map
void Worker::process_intermediate_file(const std::string& file_name, std::map<std::string, std::vector<std::string>>& key_value_map) {
    std::ifstream infile(file_name);
    if (!infile.is_open()) {
        std::cerr << "Warning: Unable to open intermediate file " << file_name << std::endl;
        return; // Ignore missing files
    }

    std::cout << "Processing intermediate file: " << file_name << std::endl; // Debugging line

    std::string line;
    while (std::getline(infile, line)) {
        //std::cout << "Line: " << line << std::endl; // Debugging line
        size_t comma_pos = line.find(',');
        if (comma_pos != std::string::npos) {
            std::string key = line.substr(0, comma_pos);
            std::string value = line.substr(comma_pos + 1);
            key_value_map[key].push_back(value); // Aggregate values for the same key
        }
    }

    infile.close();
}

/* Handle reduce tasks */
void Worker::handle_reduce_task(const masterworker::TaskRequest* request, masterworker::TaskResult* response) {
    std::string payload = request->payload();
    std::string user_id = request->user_id();
    std::string output_dir = request->output_dir();
    std::string task_id = request->task_id();
    std::vector<std::string> intermediate_files;
    size_t start = 0, end;

    // Parse the payload to extract intermediate file names
    while ((end = payload.find(';', start)) != std::string::npos) {
        intermediate_files.push_back(payload.substr(start, end - start));
        start = end + 1;
    }

    // Instantiate the reducer
    auto reducer = get_reducer_from_task_factory("cs6210");

    auto reducer_impl = reducer->impl_;
    reducer_impl->set_user_id(user_id);
    reducer_impl->set_output_dir(output_dir);

    // Aggregate key-value pairs from all intermediate files
    std::map<std::string, std::vector<std::string>> key_value_map;
    for (const auto& file : intermediate_files) {
        process_intermediate_file(file, key_value_map);
    }

    // // Print the key-value map for debugging purposes
    // std::cout << "Key-Value Map:" << std::endl;
    // for (const auto& [key, values] : key_value_map) {
    //     std::cout << "Key: " << key << ", Values: [";
    //     for (size_t i = 0; i < values.size(); ++i) {
    //         std::cout << values[i];
    //         if (i < values.size() - 1) {
    //             std::cout << ", ";
    //         }
    //     }
    //     std::cout << "]" << std::endl;
    // }

    // Call the user-defined reduce function for each key
    for (const auto& [key, values] : key_value_map) {
        reducer->reduce(key, values); // User-defined reduce function
    }

    // Set the response
    response->set_task_id(task_id);
    response->set_user_id(user_id);
    std::cout << "Reduce task completed: " << request->task_id() << std::endl;

    reducer_impl->flush_emit_buffer(); // Flush the emit buffer to write output files to disk
    std::cout << "Output files written to: " << output_dir << std::endl;
}

/* gRPC method to handle ping requests */
grpc::Status Worker::PingWorker(grpc::ServerContext* context,
                                const masterworker::PingRequest* request,
                                masterworker::PingResponse* response) {
    return grpc::Status::OK;
}
