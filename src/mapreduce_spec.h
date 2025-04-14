#pragma once

#include <string>
#include <vector>
#include <fstream>
#include <sstream>
#include <iostream>
#include <algorithm>

class MapReduceSpec {
public:
    int n_workers;                              // Number of workers
    std::vector<std::string> worker_ipaddr_ports; // Worker IP addresses and ports
    std::vector<std::string> input_files;       // List of input files
    std::string output_dir;                     // Output directory
    int n_output_files;                         // Number of output files
    size_t map_kilobytes;                       // Maximum shard size in kilobytes
    std::string user_id;                        // User ID

    // Load configuration from a file
    bool load_from_config(const std::string& config_file) {
        std::ifstream file(config_file);
        if (!file.is_open()) {
            std::cerr << "Error: Unable to open config file " << config_file << std::endl;
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
                n_workers = std::stoi(value);
            } else if (key == "worker_ipaddr_ports") {
                parse_comma_separated(value, worker_ipaddr_ports);
            } else if (key == "input_files") {
                parse_comma_separated(value, input_files);
            } else if (key == "output_dir") {
                output_dir = value;
            } else if (key == "n_output_files") {
                n_output_files = std::stoi(value);
            } else if (key == "map_kilobytes") {
                map_kilobytes = std::stoul(value);
            } else if (key == "user_id") {
                user_id = value;
            } else {
                std::cerr << "Warning: Unknown config key: " << key << std::endl;
            }
        }

        file.close();

        // Validate the configuration
        return validate();
    }

private:
    // Helper function to parse comma-separated values
    void parse_comma_separated(const std::string& value, std::vector<std::string>& result) {
        std::stringstream ss(value);
        std::string item;
        while (std::getline(ss, item, ',')) {
            result.push_back(item);
        }
    }

    // Validate the configuration
    bool validate() const {
        if (n_workers <= 0) {
            std::cerr << "Error: n_workers must be greater than 0" << std::endl;
            return false;
        }
        if (worker_ipaddr_ports.size() != static_cast<size_t>(n_workers)) {
            std::cerr << "Error: Number of worker IP addresses does not match n_workers" << std::endl;
            return false;
        }
        if (input_files.empty()) {
            std::cerr << "Error: No input files specified" << std::endl;
            return false;
        }
        if (output_dir.empty()) {
            std::cerr << "Error: Output directory not specified" << std::endl;
            return false;
        }
        if (n_output_files <= 0) {
            std::cerr << "Error: n_output_files must be greater than 0" << std::endl;
            return false;
        }
        if (map_kilobytes <= 0) {
            std::cerr << "Error: map_kilobytes must be greater than 0" << std::endl;
            return false;
        }
        if (user_id.empty()) {
            std::cerr << "Error: user_id not specified" << std::endl;
            return false;
        }
        return true;
    }
};
