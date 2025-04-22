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
#include <grpcpp/grpcpp.h>
#include <grpcpp/alarm.h>

#include <dirent.h> 
#include <cstdio> // for std::remove

//std::string const INTERMEDIATE_FILE_DIR = "";

int HEARTBEAT_RATE = 10; // seconds
int STRAGGLER_MARGIN = 5; // seconds

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

struct AsyncCall {
    grpc::ClientContext context;
    masterworker::TaskResult response;
    grpc::Status status;
    std::unique_ptr<grpc::ClientAsyncResponseReader<masterworker::TaskResult>> response_reader;
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
        void process_responses();
        void remove_intermediate_files();

        void ping_workers();       // Thread to ping workers
        void monitor_heartbeats(); // Thread to monitor worker heartbeats

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
        grpc::CompletionQueue cq_; // Completion queue for asynchronous gRPC calls
        bool done_ = false; // Flag to indicate when to stop the helper threads
        std::thread ping_thread_;       // Thread for pinging workers
        std::thread monitor_thread_;   // Thread for monitoring heartbeats
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

    // Print the number of file shards (M) and the number of output files (R)
    std::cout << "Number of file shards (M): " << shards_.size() << std::endl;
    std::cout << "Number of output files (R): " << spec_.n_output_files << std::endl;
}

/* CS6210_TASK: Here you go. once this function is called you will complete whole map reduce task and return true if succeeded */
bool Master::run() {
    // Start the ping and monitor threads
    ping_thread_ = std::thread(&Master::ping_workers, this);
    monitor_thread_ = std::thread(&Master::monitor_heartbeats, this);

    // Start processing responses in a separate thread
    std::thread response_thread(&Master::process_responses, this);

    // Assign map tasks
    assign_map_tasks();

    // Wait for all map tasks to complete (barrier)
    {
        std::unique_lock<std::mutex> lock(mutex_);
        cv_.wait(lock, [this]() { return completed_map_tasks_ == shards_.size(); });
    }

    // Assign reduce tasks
    assign_reduce_tasks();

    // Wait for all reduce tasks to complete
    {
        std::unique_lock<std::mutex> lock(mutex_);
        cv_.wait(lock, [this]() { return completed_reduce_tasks_ == spec_.n_output_files; });
    }

    std::cout << "All tasks completed successfully!" << std::endl;

    // Signal threads to terminate
    {
        std::unique_lock<std::mutex> lock(mutex_);
        done_ = true;
    }

    // Join threads
    ping_thread_.join();
    monitor_thread_.join();
    response_thread.join();

    std::cout << "Threads done!" << std::endl;

    // Remove intermediate files
    //remove_intermediate_files();

    std::cout << "MapReduce process completed successfully!" << std::endl;
    return true;
}

void Master::assign_map_tasks() {
    int map_task_counter = 0;
    while(completed_map_tasks_ != shards_.size()) {
        for (auto& worker : workers_) {
            if (worker.state == AVAILABLE && !map_task_queue_.empty()) {
                // Get the next shard from the queue
                FileShard shard_group = map_task_queue_.front();
                map_task_queue_.pop();
    
                // Create gRPC client and assign task
                auto channel = grpc::CreateChannel(worker.address, grpc::InsecureChannelCredentials());
                auto stub = masterworker::MasterWorkerService::NewStub(channel);
    
                masterworker::TaskRequest task;
                task.set_task_id("map_" + std::to_string(map_task_counter++)); // Unique task ID
    
                // Serialize the FileShard into the payload
                std::string payload;
                for (const auto& segment : shard_group.file_segments) {
                    const std::string& file_name = std::get<0>(segment);
                    size_t start_offset = std::get<1>(segment);
                    size_t end_offset = std::get<2>(segment);
                    payload += file_name + ":" + std::to_string(start_offset) + "-" + std::to_string(end_offset) + ";";
                }
                task.set_payload(payload);
                task.set_num_reducers(spec_.n_output_files); // Include the number of reducers (R)
                task.set_user_id(worker.address); // Include user ID
    
                // Asynchronous gRPC call
                auto* call = new AsyncCall;
                call->response_reader = stub->AsyncExecuteTask(&call->context, task, &cq_);
                call->response_reader->Finish(&call->response, &call->status, (void*)call);
    
                worker.state = BUSY;
                task_to_worker_[task.task_id()] = worker.address;
            }
        }
    }
}

void Master::assign_reduce_tasks() {
    int num_reducers = spec_.n_output_files;           // Number of reducers (R)
    while(completed_reduce_tasks_ != num_reducers) {
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
                for (const auto& mapper_id : spec_.worker_ipaddr_ports) {
                    intermediate_files += "intermediate_" + mapper_id + "," + std::to_string(reduce_task_id) + ".txt;";
                }
                task.set_payload(intermediate_files);
                task.set_user_id(worker.address); // Include user ID
                task.set_output_dir(spec_.output_dir); // Include output directory

                // Asynchronous gRPC call
                auto* call = new AsyncCall;
                call->response_reader = stub->AsyncExecuteTask(&call->context, task, &cq_);
                call->response_reader->Finish(&call->response, &call->status, (void*)call);

                worker.state = BUSY;
                task_to_worker_[task.task_id()] = worker.address;
            }
        }
    }

}

// Assumption: All mappers must complete before the reduce phase begins.
// Therefore, there is no need to notify reducers of mapper failures.

void Master::monitor_workers() {
    while (true) {
        {
            std::unique_lock<std::mutex> lock(mutex_);
            if (done_) {
                break; // Exit the thread if the done_ flag is set
            }
        }

        {
            std::unique_lock<std::mutex> lock(mutex_);
            for (auto& worker : workers_) {
                if (worker.state == FAILED) {
                    continue; // Skip already failed workers
                }

                /// CHECK FOR FAILED WORKERS
                if(worker.last_heartbeat + std::chrono::seconds(3) < std::chrono::steady_clock::now()) {
                    // Worker failed
                    std::cerr << "Worker " << worker.address << " failed due to missed heartbeat!" << std::endl;
                    worker.state = FAILED;

                    // Remove all tasks assigned to this worker
                    for (auto it = task_to_worker_.begin(); it != task_to_worker_.end();) {
                        if (it->second == worker.address) {
                            it = task_to_worker_.erase(it); // Remove the mapping
                        } else {
                            ++it;
                        }
                    }

                    // Reassign tasks assigned to this worker
                    for (const auto& task : task_to_worker_) {
                        if (task.second == worker.address) {
                            if (task.first.find("map") != std::string::npos) { // Failed mapper
                                map_task_queue_.push(shards_[std::stoi(task.first.substr(4))]); // Reassign file shard
                            } else if (task.first.find("reduce") != std::string::npos) { // Failed reducer
                                reduce_task_queue_.push(std::stoi(task.first.substr(7))); // Reassign reduce task
                            }
                        }
                    }
                }

                // CHECK FOR STRAGGLERS
                if (worker.state == BUSY && std::chrono::steady_clock::now() - worker.last_heartbeat > std::chrono::seconds(STRAGGLER_MARGIN)) {
                    std::cerr << "Worker " << worker.address << " is a straggler!" << std::endl;
                    // Reassign tasks assigned to this worker without marking it as failed
                    for (const auto& task : task_to_worker_) {
                        if (task.second == worker.address) {
                            if (task.first.find("map") != std::string::npos) { // Failed mapper
                                map_task_queue_.push(shards_[std::stoi(task.first.substr(4))]); // Reassign file shard
                            } else if (task.first.find("reduce") != std::string::npos) { // Failed reducer
                                reduce_task_queue_.push(std::stoi(task.first.substr(7))); // Reassign reduce task
                            }
                        }
                    }
                }
            }
        }
        cv_.notify_all();
    }
}

void Master::ping_workers() {
    while (true) {
        {
            std::unique_lock<std::mutex> lock(mutex_);
            if (done_) {
                break; // Exit the thread if the done_ flag is set
            }
        }

        std::this_thread::sleep_for(std::chrono::seconds(5)); // Ping every 5 seconds

        std::unique_lock<std::mutex> lock(mutex_);
        for (auto& worker : workers_) {
            if (worker.state == FAILED) {
                continue; // Skip already failed workers
            }

            // Create a gRPC stub to communicate with the worker
            auto channel = grpc::CreateChannel(worker.address, grpc::InsecureChannelCredentials());
            auto stub = masterworker::MasterWorkerService::NewStub(channel);

            // Prepare the PingRequest
            masterworker::PingRequest request;
            masterworker::PingResponse response;
            grpc::ClientContext context;

            // Send the PingWorker RPC
            grpc::Status status = stub->PingWorker(&context, request, &response);

            if (status.ok()) {
                // Update the worker's last heartbeat timestamp
                worker.last_heartbeat = std::chrono::steady_clock::now();
            } else {
                std::cerr << "Ping failed for worker " << worker.address << std::endl;
            }
        }
    }
}

void Master::monitor_heartbeats() {
    while (true) {
        {
            std::unique_lock<std::mutex> lock(mutex_);
            if (done_) {
                break; // Exit the thread if the done_ flag is set
            }
        }

        std::this_thread::sleep_for(std::chrono::seconds(HEARTBEAT_RATE)); // Check every HEARTBEAT_RATE seconds
        {
            std::unique_lock<std::mutex> lock(mutex_);
            auto now = std::chrono::steady_clock::now();
            for (auto& worker : workers_) {
                if (worker.state == FAILED) {
                    continue; // Skip already failed workers
                }
    
                // Check if more than 10 seconds have passed since the last heartbeat
                if (std::chrono::duration_cast<std::chrono::seconds>(now - worker.last_heartbeat).count() > HEARTBEAT_RATE) {
                    // Worker failed
                    std::cerr << "Worker " << worker.address << " failed due to missed heartbeat!" << std::endl;
                    worker.state = FAILED;
    
                    // Remove all tasks assigned to this worker
                    for (auto it = task_to_worker_.begin(); it != task_to_worker_.end();) {
                        if (it->second == worker.address) {
                            it = task_to_worker_.erase(it); // Remove the mapping
                        } else {
                            ++it;
                        }
                    }
    
                    // Reassign tasks assigned to this worker
                    for (const auto& task : task_to_worker_) {
                        if (task.second == worker.address) {
                            if (task.first.find("map") != std::string::npos) { // Failed mapper
                                map_task_queue_.push(shards_[std::stoi(task.first.substr(4))]); // Reassign file shard
                            } else if (task.first.find("reduce") != std::string::npos) { // Failed reducer
                                std::cout << "Reassigning reduce task: " << task.first.substr(7) << std::endl;
                                reduce_task_queue_.push(std::stoi(task.first.substr(7))); // Reassign reduce task
                            }
                        }
                    }
                }
            }
        }
        cv_.notify_all();
    }
}

void Master::handle_task_completion(const masterworker::TaskResult& result) {
    {
        std::unique_lock<std::mutex> lock(mutex_);

        const std::string& task_id = result.task_id();
        const std::string& worker_address = result.user_id();

        // Check if the task-to-worker mapping exists for this task
        auto it = task_to_worker_.find(task_id);
        if (it == task_to_worker_.end() || it->second != worker_address) {
            // Unexpected or duplicate task completion
            std::cerr << "Unexpected or duplicate task completion for task: " << task_id
                    << " from worker: " << worker_address << std::endl;
            return;
        }

        // Remove the task-to-worker mapping for this task
        task_to_worker_.erase(task_id);

        // Mark worker as available
        for (auto& worker : workers_) {
            if (worker.address == worker_address) {
                worker.state = AVAILABLE;
                break;
            }
        }

        // Increment the appropriate completed task counter
        if (task_id.find("map") != std::string::npos) {
            ++completed_map_tasks_;
        } else if (task_id.find("reduce") != std::string::npos) {
            ++completed_reduce_tasks_;
        }

        std::cout << "\n\nHandling completion: " << task_id << std::endl;
        std::cout << "Task completed: " << task_id << std::endl;
        std::cout << "Worker: " << worker_address << std::endl;
        std::cout << "Reduce task queue size: " << reduce_task_queue_.size() << std::endl;
        std::cout << "Map task queue size: " << map_task_queue_.size() << std::endl;
        std::cout << "Completed map tasks: " << completed_map_tasks_ << std::endl;
        std::cout << "Completed reduce tasks: " << completed_reduce_tasks_ << std::endl;

        // Print worker states
        std::cout << "Worker States:" << std::endl;
        for (const auto& worker : workers_) {
            std::cout << "Worker " << worker.address << ": ";
            switch (worker.state) {
                case AVAILABLE:
                    std::cout << "AVAILABLE";
                    break;
                case BUSY:
                    std::cout << "BUSY";
                    break;
                case FAILED:
                    std::cout << "FAILED";
                    break;
            }
            std::cout << std::endl;
        }

        // Print task-to-worker mappings
        std::cout << "Task-to-Worker Mappings:" << std::endl;
        for (const auto& task_mapping : task_to_worker_) {
            std::cout << "Task " << task_mapping.first << " -> Worker " << task_mapping.second << std::endl;
        }
    }
    cv_.notify_all();
}

void Master::process_responses() {
    void* tag;
    bool ok;

    while (true) {
        {
            std::unique_lock<std::mutex> lock(mutex_);
            if (done_) {
                break; // Exit the thread if the done_ flag is set
            }
        }

        if (cq_.Next(&tag, &ok)) {
            if (ok) {
                auto* call = static_cast<AsyncCall*>(tag);
                std::cout << "Task ID: " << call->response.task_id() << std::endl;
                std::cout << "User ID: " << call->response.user_id() << std::endl;

                if (call->status.ok()) {
                    std::cout << "Task success for task_id: " << call->response.task_id() << std::endl;
                    handle_task_completion(call->response); // Call handle_task_completion
                } else {
                    std::cerr << "Error: Task failed for task_id: " << call->response.task_id() << std::endl;
                }

                delete call; // Clean up the call object
            }
        }
    }
}

void Master::remove_intermediate_files() {
    std::string directory = "./"; // Set the directory where intermediate files are stored
    DIR* dir = opendir(directory.c_str());
    if (!dir) {
        std::cerr << "Error: Unable to open directory " << directory << std::endl;
        return;
    }

    struct dirent* entry;
    while ((entry = readdir(dir)) != nullptr) {
        // Check if the file name starts with "intermediate_"
        if (std::string(entry->d_name).rfind("intermediate_", 0) == 0) {
            std::string file_path = directory + "/" + entry->d_name;
            if (std::remove(file_path.c_str()) == 0) {
                std::cout << "Removed file: " << file_path << std::endl;
            } else {
                std::cerr << "Error: Unable to remove file " << file_path << std::endl;
            }
        }
    }

    closedir(dir);
}

class Worker : public masterworker::MasterWorkerService::Service {
public:
    grpc::Status PingWorker(grpc::ServerContext* context,
                            const masterworker::PingRequest* request,
                            masterworker::PingResponse* response) override;
};

grpc::Status Worker::PingWorker(grpc::ServerContext* context,
                                const masterworker::PingRequest* request,
                                masterworker::PingResponse* response) {
    return grpc::Status::OK;
}