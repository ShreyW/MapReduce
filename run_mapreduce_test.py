#!/usr/bin/env python3

import os
import subprocess
import time
import argparse
import signal
import random
import sys
from pathlib import Path

class MapReduceTestRunner:
    def __init__(self, test_fault_tolerance=False, kill_delay=2):
        self.base_dir = Path(os.path.dirname(os.path.abspath(__file__)))
        self.build_dir = self.base_dir / "build"
        self.bin_dir = self.build_dir / "bin"
        self.config_file = self.bin_dir / "config.ini"
        self.worker_pids = []
        self.test_fault_tolerance = test_fault_tolerance
        self.kill_delay = kill_delay
        self.config = {}
        self.truth_file = self.base_dir / "truth.txt"
        
    def build_project(self):
        """Build the project using setup_and_build.sh"""
        print("Building the project...")
        build_script = self.base_dir / "setup_and_build.sh"
        result = subprocess.run(['bash', str(build_script)], capture_output=True, text=True)
        
        if result.returncode != 0:
            print("Error building the project:")
            print(result.stderr)
            sys.exit(1)
            
        print("Build completed successfully")
    
    def parse_config(self):
        """Parse the config.ini file to extract worker addresses"""
        print("Parsing config.ini file...")
        
        if not os.path.exists(self.config_file):
            print(f"Error: Config file not found at {self.config_file}")
            sys.exit(1)
        
        # For config files without section headers, read manually as key-value pairs
        self.config = {}
        with open(self.config_file, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('//'):
                    key, value = line.split('=', 1)
                    self.config[key.strip()] = value.strip()
        
        worker_addresses = self.config.get('worker_ipaddr_ports', '').split(',')
        
        if not worker_addresses:
            print("Error: No worker addresses found in config.ini")
            sys.exit(1)
        
        print(f"Found {len(worker_addresses)} worker addresses in config.ini")
        return worker_addresses
    
    def start_workers(self, worker_addresses):
        """Start all worker processes"""
        print("Starting worker processes...")
        os.chdir(self.bin_dir)
        
        for addr in worker_addresses:
            if not addr:
                continue
            
            cmd = ['./mr_worker', addr]
            process = subprocess.Popen(cmd)
            self.worker_pids.append(process.pid)
            print(f"Started worker at {addr} with PID {process.pid}")
        
        # Give workers time to start up
        print("Waiting for workers to initialize...")
        time.sleep(1)
    
    def kill_random_worker(self):
        """Kill a random worker to test fault tolerance"""
        if not self.worker_pids:
            print("No workers to kill")
            return
        
        victim_index = random.randint(0, len(self.worker_pids) - 1)
        victim_pid = self.worker_pids[victim_index]
        
        print(f"[Fault Tolerance Test] Killing worker with PID {victim_pid} in {self.kill_delay} seconds...")
        time.sleep(self.kill_delay)
        
        try:
            os.kill(victim_pid, signal.SIGKILL)
            print(f"[Fault Tolerance Test] Killed worker with PID {victim_pid}")
            self.worker_pids.pop(victim_index)
        except OSError as e:
            print(f"Error killing worker: {e}")
    
    def run_mapreduce(self):
        """Run the MapReduce demo"""
        print("Running MapReduce demo...")
        
        os.chdir(self.bin_dir)
        output_dir = self.bin_dir / "output"
        if not os.path.exists(output_dir):
            os.mkdir(output_dir)
        
        # Start the MapReduce demo
        cmd = ['./mrdemo', 'config.ini']
        process = subprocess.Popen(cmd)
        
        # If testing fault tolerance, kill a worker while MapReduce is running
        if self.test_fault_tolerance:
            self.kill_random_worker()
        
        returncode = process.wait()
        if returncode != 0:
            print(f"Error running MapReduce demo (exit code {returncode})")
            self.cleanup_workers()
            sys.exit(returncode)
        
        print("MapReduce execution completed")
    

    def generate_truth_data(self):
        print("Generating truth data...")

        input_dir = self.bin_dir / "input"
        if not os.path.exists(input_dir):
            print(f"Error: Input directory {input_dir} not found")
            return {}

        os.environ["LC_ALL"] = "C"
        print(f"Processing input files from: {input_dir}")
        
        all_words = []
        for file_path in sorted(input_dir.glob('*')):
            if file_path.is_file():
                with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
                    content = f.read()
                    if not content.endswith('\n'):
                        content += '\n'

                    words = content.replace(' ', '\n').splitlines()
                    words = [word[:-1] if word.endswith('.') else word for word in words]
                    all_words.extend(words)
        
        all_words.sort()
        word_counts = {}
        for word in all_words:
            if word:
                word_counts[word] = word_counts.get(word, 0) + 1
            
        formatted_output = []
        for word, count in sorted(word_counts.items()):
            formatted_output.append(f"{word} {count}")
        
        output_text = '\n'.join(formatted_output)
        if output_text:
            output_text += '\n'
            
        with open(self.truth_file, 'w') as f:
            f.write(output_text)
            
        print(f"Truth data generated and saved to {self.truth_file}")
        return word_counts
    
    def validate_output(self, truth_data):
        """Validate MapReduce output against truth data"""
        print("Validating MapReduce output...")
        
        output_dir = self.bin_dir / "output"
        if not os.path.exists(output_dir):
            print(f"Error: Output directory {output_dir} not found")
            return False
        
        combined_output = {}
        output_files = os.listdir(output_dir)
        for file_name in output_files:
            file_path = output_dir / file_name
            with open(file_path, 'r') as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    
                    parts = line.split()
                    if len(parts) >= 2 and parts[-1].isdigit():
                        word = ' '.join(parts[:-1])
                        count = int(parts[-1])
                        combined_output[word] = combined_output.get(word, 0) + count
                    else:
                        print(f"Warning: Ignoring malformed line in {file_name}: {line}")
        
        failures = []

        for word, expected_count in truth_data.items():
            if word in combined_output:
                actual_count = combined_output[word]
                if actual_count != expected_count:
                    failures.append(f"FAIL: {word} {actual_count} Expected: {expected_count}")
            else:
                failures.append(f"FAIL: {word} missing, expected count: {expected_count}")
        
        for word, actual_count in combined_output.items():
            if word not in truth_data:
                failures.append(f"FAIL: Unexpected {word} {actual_count}")
        
        # Save failures to a file
        failures_file = self.base_dir / "failures.txt"
        with open(failures_file, 'w') as f:
            f.write(f"MapReduce Validation Failures - {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Total Failures: {len(failures)}\n\n")
            for failure in failures:
                f.write(f"{failure}\n")
        
        # Print failures if any
        if failures:
            print(f"\n{len(failures)} validation failures found:")
            for failure in failures[:10]:
                print(failure)
            
            if len(failures) > 10:
                print(f"...and {len(failures) - 10} more failures.")
            
            print(f"\nAll failures have been saved to {failures_file}")
            return False
        
        print("All output validated successfully!")
        return True
    
    def cleanup_workers(self):
        """Cleanup all worker processes"""
        print("Cleaning up worker processes...")
        for pid in self.worker_pids:
            try:
                os.kill(pid, signal.SIGTERM)
                print(f"Terminated worker with PID {pid}")
            except OSError:
                pass
    
    def run(self):
        """Run the full test workflow"""
        try:
            # self.build_project()
            worker_addresses = self.parse_config()
            self.start_workers(worker_addresses)
            self.run_mapreduce()
            
            # Generate truth data and validate output
            truth_data = self.generate_truth_data()
            test_success = self.validate_output(truth_data)
            
            if test_success:
                print("\n✅ MapReduce test completed successfully!")
            else:
                print("\n❌ MapReduce test completed with errors")
                
        except KeyboardInterrupt:
            print("\nTest interrupted by user")
        finally:
            self.cleanup_workers()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='MapReduce Test Runner')
    parser.add_argument('--fault-tolerance', action='store_true', 
                        help='Test fault tolerance by killing a worker during execution')
    parser.add_argument('--kill-delay', type=int, default=2,
                        help='Seconds to wait before killing a worker (default: 2)')
    
    args = parser.parse_args()
    
    runner = MapReduceTestRunner(
        test_fault_tolerance=args.fault_tolerance,
        kill_delay=args.kill_delay
    )
    runner.run()