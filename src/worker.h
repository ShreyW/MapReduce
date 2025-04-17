#pragma once

#include <mr_task_factory.h>
#include "mr_tasks.h"
#include "masterworker.grpc.pb.h"
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

        // Helper functions for task handling
        void handle_map_task(const masterworker::TaskRequest* request, masterworker::TaskResult* response);
        void handle_reduce_task(const masterworker::TaskRequest* request, masterworker::TaskResult* response);
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


/* Handle map tasks */
void Worker::handle_map_task(const masterworker::TaskRequest* request, masterworker::TaskResult* response) {
    // Parse the payload to extract file name and offsets
    std::string payload = request->payload();
    size_t colon_pos = payload.find(':');
    size_t dash_pos = payload.find('-');
    std::string file_name = payload.substr(0, colon_pos);
    size_t start_offset = std::stoul(payload.substr(colon_pos + 1, dash_pos - colon_pos - 1));
    size_t end_offset = std::stoul(payload.substr(dash_pos + 1));

    // Create a mapper and set the number of reducers
    auto mapper = get_mapper_from_task_factory("cs6210");
    mapper->impl_.set_num_reducers(request->num_reducers());

    // Process the file shard
    mapper->impl_.process_file_shard(file_name, start_offset, end_offset);

    // Collect intermediate file names
    for (int i = 0; i < request->num_reducers(); ++i) {
        response->add_intermediate_files("intermediate_" + std::to_string(i) + ".txt");
    }

    response->set_task_id(request->task_id());
    std::cout << "Map task completed: " << request->task_id() << std::endl;
}


/* Handle reduce tasks */
void Worker::handle_reduce_task(const masterworker::TaskRequest* request, masterworker::TaskResult* response) {
    // Parse the payload to get the list of intermediate files
    std::string payload = request->payload();
    std::vector<std::string> intermediate_files;
    size_t start = 0, end;
    while ((end = payload.find(',', start)) != std::string::npos) {
        intermediate_files.push_back(payload.substr(start, end - start));
        start = end + 1;
    }
    intermediate_files.push_back(payload.substr(start));

    // Create a reducer
    auto reducer = get_reducer_from_task_factory("cs6210");

    // Process the intermediate files
    reducer->impl_.process_intermediate_files(intermediate_files);

    // Simulate output file creation
    std::string output_file = "output_" + request->task_id().substr(7) + ".txt";
    response->add_intermediate_files(output_file); // Add output file to response
    response->set_task_id(request->task_id());
    std::cout << "Reduce task completed: " << request->task_id() << std::endl;
}
