#pragma once

#include <string>
#include <vector>
#include <fstream>
#include <sstream>
#include <iostream>
#include <algorithm>

/* CS6210_TASK: Create your data structure here for storing spec from the config file */
struct MapReduceSpec {
    int n_workers;                              // Number of workers
    std::vector<std::string> worker_ipaddr_ports; // Worker IP addresses and ports
    std::vector<std::string> input_files;       // List of input files
    std::string output_dir;                     // Output directory
    int n_output_files;                         // Number of output files
    size_t map_kilobytes;                       // Maximum shard size in kilobytes
    std::string user_id;                        // User ID
};

/* CS6210_TASK: Populate MapReduceSpec data structure with the specification from the config file */
inline bool read_mr_spec_from_config_file(const std::string& config_filename, MapReduceSpec& mr_spec) {
    std::ifstream file(config_filename);
    if (!file.is_open()) {
        std::cerr << "Error: Unable to open config file " << config_filename << std::endl;
        return false;
    }

    std::string line;
    while (std::getline(file, line)) {
        // Ignore comments and empty lines
        line.erase(std::remove_if(line.begin(), line.end(), ::isspace), line.end());
        if (line.empty() || line[0] == ';') {
            continue;
        }

        // Split the line into key and value
        size_t delimiter_pos = line.find('=');
        if (delimiter_pos == std::string::npos) {
            std::cerr << "Error: Invalid config line: " << line << std::endl;
            return false;
        }

        std::string key = line.substr(0, delimiter_pos);
        std::string value = line.substr(delimiter_pos + 1);

        // Populate the fields based on the key
        if (key == "n_workers") {
            mr_spec.n_workers = std::stoi(value);
        } else if (key == "worker_ipaddr_ports") {
            std::stringstream ss(value);
            std::string item;
            while (std::getline(ss, item, ',')) {
                mr_spec.worker_ipaddr_ports.push_back(item);
            }
        } else if (key == "input_files") {
            std::stringstream ss(value);
            std::string item;
            while (std::getline(ss, item, ',')) {
                mr_spec.input_files.push_back(item);
            }
        } else if (key == "output_dir") {
            mr_spec.output_dir = value;
        } else if (key == "n_output_files") {
            mr_spec.n_output_files = std::stoi(value);
        } else if (key == "map_kilobytes") {
            mr_spec.map_kilobytes = std::stoul(value);
        } else if (key == "user_id") {
            mr_spec.user_id = value;
        } else {
            std::cerr << "Warning: Unknown config key: " << key << std::endl;
        }
    }

    file.close();
    return true;
}

/* CS6210_TASK: validate the specification read from the config file */
inline bool validate_mr_spec(const MapReduceSpec& mr_spec) {
    if (mr_spec.n_workers <= 0) {
        std::cerr << "Error: n_workers must be greater than 0" << std::endl;
        return false;
    }
    if (mr_spec.worker_ipaddr_ports.size() != static_cast<size_t>(mr_spec.n_workers)) {
        std::cerr << "Error: Number of worker IP addresses does not match n_workers" << std::endl;
        return false;
    }
    if (mr_spec.input_files.empty()) {
        std::cerr << "Error: No input files specified" << std::endl;
        return false;
    }
    if (mr_spec.output_dir.empty()) {
        std::cerr << "Error: Output directory not specified" << std::endl;
        return false;
    }
    if (mr_spec.n_output_files <= 0) {
        std::cerr << "Error: n_output_files must be greater than 0" << std::endl;
        return false;
    }
    if (mr_spec.map_kilobytes <= 0) {
        std::cerr << "Error: map_kilobytes must be greater than 0" << std::endl;
        return false;
    }
    if (mr_spec.user_id.empty()) {
        std::cerr << "Error: user_id not specified" << std::endl;
        return false;
    }
    return true;
}
