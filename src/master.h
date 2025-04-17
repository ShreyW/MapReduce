#pragma once

#include "mapreduce_spec.h"
#include "file_shard.h"
#include "masterworker.grpc.pb.h"
#include <vector>
#include <string>
#include <unordered_map>
#include <queue>
#include <mutex>
#include <condition_variable>
#include <chrono>
#include <thread>
#include <iostream>

enum WorkerState {
    AVAILABLE,
    BUSY,
    FAILED
};

struct WorkerInfo {
    std::string address; // Worker address (ip:port)
    WorkerState state;   // Worker state
    std::chrono::steady_clock::time_point last_heartbeat; // Last heartbeat timestamp
};

class Master {

    public:
        /* DON'T change the function signature of this constructor */
        Master(const MapReduceSpec&, const std::vector<FileShard>&);

        /* DON'T change this function's signature */
        bool run();

    private:
        /* NOW you can add below, data members and member functions as per the need of your implementation */
        void assign_map_tasks();
        void assign_reduce_tasks();
        void monitor_workers();
        void handle_task_completion(const masterworker::TaskResult& result);
        void schedule_backup_tasks();

        MapReduceSpec spec_;
        std::vector<FileShard> shards_;
        std::vector<WorkerInfo> workers_;
        std::queue<FileShard> map_task_queue_;
        std::queue<int> reduce_task_queue_;
        std::mutex mutex_;
        std::condition_variable cv_;
        int completed_map_tasks_;
        int completed_reduce_tasks_;
        std::unordered_map<std::string, std::string> task_to_worker_; // Maps task_id to worker address
        std::unordered_map<int, std::vector<std::string>> reduce_task_to_files_;
};


/* CS6210_TASK: This is all the information your master will get from the framework.
    You can populate your other class data members here if you want */
Master::Master(const MapReduceSpec& mr_spec, const std::vector<FileShard>& file_shards)
    : spec_(mr_spec), shards_(file_shards), completed_map_tasks_(0), completed_reduce_tasks_(0) {
    // Initialize workers
    for (const auto& address : spec_.worker_ipaddr_ports) {
        workers_.push_back({address, AVAILABLE, std::chrono::steady_clock::now()});
    }

    // Populate map task queue
    for (const auto& shard : shards_) {
        map_task_queue_.push(shard);
    }

    // Populate reduce task queue
    for (int i = 0; i < spec_.n_output_files; ++i) {
        reduce_task_queue_.push(i);
    }
}

/* CS6210_TASK: Here you go. once this function is called you will complete whole map reduce task and return true if succeeded */
bool Master::run() {
    // Start worker monitoring in a separate thread so that it's done concurrently
    std::thread monitor_thread(&Master::monitor_workers, this);

    // Assign map tasks
    assign_map_tasks();

    // Wait for all map tasks to complete
    {
        std::unique_lock<std::mutex> lock(mutex_);
        cv_.wait(lock, [this]() { return completed_map_tasks_ == shards_.size(); }); // completed_map_tasks_ is M
    }

    // Assign reduce tasks
    assign_reduce_tasks();

    // Wait for all reduce tasks to complete
    {
        std::unique_lock<std::mutex> lock(mutex_);
        cv_.wait(lock, [this]() { return completed_reduce_tasks_ == spec_.n_output_files; });
    }

    // Join the monitor thread
    monitor_thread.join();

    std::cout << "MapReduce process completed successfully!" << std::endl;
    return true;
}

void Master::assign_map_tasks() {
    for (auto& worker : workers_) {
        if (worker.state == AVAILABLE && !map_task_queue_.empty()) {
            FileShard shard = map_task_queue_.front();
            map_task_queue_.pop();

            // Create gRPC client and assign task
            auto channel = grpc::CreateChannel(worker.address, grpc::InsecureChannelCredentials());
            auto stub = masterworker::MasterWorkerService::NewStub(channel);

            masterworker::TaskRequest task;
            task.set_task_id("map_" + shard.file_name + "_" + std::to_string(shard.start_offset));
            task.set_payload(shard.file_name + ":" + std::to_string(shard.start_offset) + "-" + std::to_string(shard.end_offset));
            task.set_num_reducers(spec_.n_output_files); // Include the number of reducers (R)

            grpc::ClientContext context;
            masterworker::TaskResult response;
            stub->ExecuteTask(&context, task, &response);

            worker.state = BUSY;
            task_to_worker_[task.task_id()] = worker.address;
        }
    }
}

void Master::assign_reduce_tasks() {
    for (auto& worker : workers_) {
        if (worker.state == AVAILABLE && !reduce_task_queue_.empty()) {
            int reduce_task_id = reduce_task_queue_.front();
            reduce_task_queue_.pop();

            // Create gRPC client and assign task
            auto channel = grpc::CreateChannel(worker.address, grpc::InsecureChannelCredentials());
            auto stub = masterworker::MasterWorkerService::NewStub(channel);

            masterworker::TaskRequest task;
            task.set_task_id("reduce_" + std::to_string(reduce_task_id));

            // Collect intermediate files for this reduce task
            std::string intermediate_files;
            for (const auto& mapper : mappers_) {
                intermediate_files += mapper + "_intermediate_" + std::to_string(reduce_task_id) + ".txt,";
            }
            intermediate_files.pop_back(); // Remove trailing comma
            task.set_payload(intermediate_files);

            grpc::ClientContext context;
            masterworker::TaskResult response;
            stub->ExecuteTask(&context, task, &response);

            worker.state = BUSY;
            task_to_worker_[task.task_id()] = worker.address;
        }
    }
}

// Assumption: All mappers must complete before the reduce phase begins.
// Therefore, there is no need to notify reducers of mapper failures.

// Instead of pinging and waiting for a response, the workers heartbeat periodically
void Master::monitor_workers() {
    while (true) {
        std::this_thread::sleep_for(std::chrono::seconds(5)); // Check every 5 seconds

        std::unique_lock<std::mutex> lock(mutex_);
        for (auto& worker : workers_) {
            if (worker.state == BUSY &&
                std::chrono::steady_clock::now() - worker.last_heartbeat > std::chrono::seconds(10)) {
                // Worker failed
                std::cerr << "Worker " << worker.address << " failed!" << std::endl;
                worker.state = FAILED; // considered unavailable for the rest of the runtime

                // Reassign tasks assigned to this worker
                for (const auto& task : task_to_worker_) {
                    if (task.second == worker.address) {
                        if (task.first.find("map") != std::string::npos) { // Failed mapper
                            map_task_queue_.push(shards_[std::stoi(task.first.substr(4))]); // Fileshard to next available mapper
                        } else if (task.first.find("reduce") != std::string::npos) { // Failed reducer
                            reduce_task_queue_.push(std::stoi(task.first.substr(7))); // Intermediate file taks to next available reducer
                        }
                    }
                }
            }
        }
        cv_.notify_all();
    }
}

void Master::handle_task_completion(const masterworker::TaskResult& result) {
    std::unique_lock<std::mutex> lock(mutex_);
    if (result.task_id().find("map") != std::string::npos) {
        ++completed_map_tasks_;
    } else if (result.task_id().find("reduce") != std::string::npos) {
        ++completed_reduce_tasks_;
    }

    // Mark worker as available
    for (auto& worker : workers_) {
        if (task_to_worker_[result.task_id()] == worker.address) {
            worker.state = AVAILABLE;
            break;
        }
    }

    cv_.notify_all();
}