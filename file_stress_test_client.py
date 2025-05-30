import socket
import json
import base64
import logging
import os
import sys
import multiprocessing
import concurrent.futures
import time
import random
import csv
import threading
import argparse
import statistics
from collections import defaultdict

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("checking.log"),
        logging.StreamHandler()
    ]
)

class StressTestClient:
    def __init__(self, server_address=('localhost', 6666)):
        self.server_address = server_address
        self.results = {
            'upload': [], 'download': [], 'list': []
        }
        self.success_count = {
            'upload': 0, 'download': 0, 'list': 0
        }
        self.fail_count = {
            'upload': 0, 'download': 0, 'list': 0
        }
        
        # Membuat direktori testfiles untuk menyimpan kumpulan testfiles
        if not os.path.exists('testfiles'):
            os.makedirs('testfiles')
        
        # Membuat direktori downloads untuk menyimpan kumpulan hasil download
        if not os.path.exists('downloads'):
            os.makedirs('downloads')

    def generate_testfile(self, size_mb):
        filename=f"test_file_{size_mb}MB.bin" # format nama testfile
        filepath=os.path.join('testfiles', filename)
        
        if os.path.exists(filepath) and os.path.getsize(filepath) == size_mb * 1024 * 1024:
            logging.info(f"Test file {filename} already exists with correct size")
            return filepath
        
        logging.info(f"Generating test file: {filename} ({size_mb} MB)")

        with open(filepath, 'wb') as f:
            # Untuk menghindari memory issues
            chunk_size=1024 * 1024  
            for _ in range(size_mb):
                f.write(os.urandom(chunk_size))
        
        logging.info(f"Test file generated: {filepath}")
        return filepath

    def send_command(self, command_str=""):
        sock=socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # Untuk large files
        sock.settimeout(600)

        try:
            start_connect=time.time()
            sock.connect(self.server_address)
            connect_time = time.time() - start_connect
            logging.debug(f"Connection established in {connect_time:.2f}s")
            
            # Apabila file besar, command dikirim dalam chunk
            chunks=[command_str[i:i+65536] for i in range(0, len(command_str), 65536)]
            for chunk in chunks:
                sock.sendall((chunk).encode())
            
            sock.sendall("\r\n\r\n".encode())
            
            # Menerima respons dalam bentuk chunk
            data_received = "" 

            while True:
                try:
                    data=sock.recv(1024*1024) 
                    if data:
                        data_received += data.decode()
                        if "\r\n\r\n" in data_received:
                            break
                    else:
                        break
                
                except socket.timeout:
                    logging.error("Socket timeout when receiving data")
                    return {'status': 'ERROR', 'data': 'Socket timeout when receiving data'}
            
            json_response=data_received.split("\r\n\r\n")[0]
            result=json.loads(json_response)
            return result
        
        except socket.timeout as e:
            logging.error(f"Socket timeout: {str(e)}")
            return {'status': 'ERROR', 'data': f'Socket timeout: {str(e)}'}
        
        except ConnectionRefusedError:
            logging.error("Connection refused. Is the server running?")
            return {'status': 'ERROR', 'data': 'Connection refused. Server running or not?'}
        
        except Exception as e:
            logging.error(f"Error in send_command: {str(e)}")
            return {'status': 'ERROR', 'data': str(e)}
        
        finally:
            sock.close()

    def remote_list(self, worker_id):
        # For list operation
        start_time = time.time()
        
        try:
            command_str = "LIST"
            result = self.send_command(command_str)
            
            end_time = time.time()
            duration = end_time - start_time
            
            if result['status'] == 'OK':
                file_count = len(result['data'])
                logging.info(f"Worker {worker_id}: List successful - {file_count} files in {duration:.2f}s")
                self.success_count['list'] += 1
            
            else:
                logging.error(f"Worker {worker_id}: List failed: {result['data']}")
                self.fail_count['list'] += 1
                
            return {
                'worker_id': worker_id, 'operation': 'list', 'duration': duration,
                'status': result['status']
            }
        
        except Exception as e:
            end_time = time.time()
            duration = end_time - start_time

            logging.error(f"Worker {worker_id}: List exception: {str(e)}")
            self.fail_count['list'] += 1
            
            return {
                'worker_id': worker_id, 'operation': 'list', 'duration': duration,
                'status': 'ERROR', 'error': str(e)
            }

    def remote_upload(self, file_path, worker_id):
        # For upload operation
        start_time = time.time()
        filename = os.path.basename(file_path)
        file_size = os.path.getsize(file_path)
        
        try:
            logging.info(f"Worker {worker_id}: Starting upload of {filename} ({file_size/1024/1024:.2f} MB)")
            
            # File dibaca dalam bentuk chunks
            with open(file_path, 'rb') as fp:
                file_content = base64.b64encode(fp.read()).decode()
            
            command_str = f"UPLOAD {filename} {file_content}"
            result = self.send_command(command_str)
            
            end_time = time.time()
            duration = end_time - start_time
            throughput = file_size / duration if duration > 0 else 0
            
            if result['status'] == 'OK':
                logging.info(f"Worker {worker_id}: Upload successful - {filename} ({file_size/1024/1024:.2f} MB) in {duration:.2f}s - {throughput/1024/1024:.2f} MB/s")
                self.success_count['upload'] += 1
            else:
                logging.error(f"Worker {worker_id}: Upload failed - {filename}: {result['data']}")
                self.fail_count['upload'] += 1
                
            return {
                'worker_id': worker_id, 'operation': 'upload', 'file_size': file_size,
                'duration': duration, 'throughput': throughput, 'status': result['status']
            }
        
        except Exception as e:
            end_time = time.time()
            duration = end_time - start_time

            logging.error(f"Worker {worker_id}: Upload exception - {filename}: {str(e)}")
            self.fail_count['upload'] += 1

            return {
                'worker_id': worker_id, 'operation': 'upload', 'file_size': file_size, 'duration': duration,
                'throughput': 0, 'status': 'ERROR', 'error': str(e)
            }

    def remote_download(self, filename, worker_id):
        # For download operation
        start_time = time.time()
        
        try:
            logging.info(f"Worker {worker_id}: Starting download of {filename}")
            
            command_str = f"GET {filename}"
            result = self.send_command(command_str)
            
            if result['status'] == 'OK':
                file_content = base64.b64decode(result['data_file'])
                file_size = len(file_content)
                
                # Setelah download, disimpan ke folder download
                download_path = os.path.join('downloads', f"worker{worker_id}_{filename}")
                with open(download_path, 'wb') as f:
                    f.write(file_content)
                
                end_time = time.time()
                duration = end_time - start_time
                throughput = file_size / duration if duration > 0 else 0
                
                logging.info(f"Worker {worker_id}: Download successful - {filename} ({file_size/1024/1024:.2f} MB) in {duration:.2f}s - {throughput/1024/1024:.2f} MB/s")
                self.success_count['download'] += 1
                
                return {
                    'worker_id': worker_id, 'operation': 'download', 'file_size': file_size, 'duration': duration, 'throughput': throughput,
                    'status': 'OK'
                }
            
            else:
                end_time = time.time()
                duration = end_time - start_time
                logging.error(f"Worker {worker_id}: Download failed - {filename}: {result['data']}")
                self.fail_count['download'] += 1
                
                return {
                    'worker_id': worker_id, 'operation': 'download', 'file_size': 0,
                    'duration': duration, 'throughput': 0, 'status': 'ERROR',
                    'error': result['data']
                }
            
        except Exception as e:
            end_time = time.time()
            duration = end_time - start_time

            logging.error(f"Worker {worker_id}: Download exception - {filename}: {str(e)}")
            self.fail_count['download'] += 1
            
            return {
                'worker_id': worker_id, 'operation': 'download', 'file_size': 0,
                'duration': duration, 'throughput': 0, 'status': 'ERROR',
                'error': str(e)
            }

    def reset_counters(self):
        # Untuk reset nilai counters
        self.success_count = {
            'upload': 0, 'download': 0, 'list': 0
        }

        self.fail_count = {
            'upload': 0, 'download': 0, 'list': 0
        }

        self.results = {
            'upload': [], 'download': [], 'list': []
        }

    def run_stress_test(self, operation, file_size_mb, client_pool_size, executor_type='thread'):
        # Menjalankan stress test sesuai dengan beberapa parameter spesifik yang diinginkan
        self.reset_counters()
        
        if operation not in ['upload', 'download', 'list']:
            logging.error(f"Unknown operation: {operation} please input the correct one")
            return
            
        logging.info(f"Starting {operation} stress test with {file_size_mb}MB files, {client_pool_size} {executor_type} workers")
        
        test_file = None
        if operation == 'upload' or operation == 'download':
            test_file = self.generate_testfile(file_size_mb)
        
        # Apabila operasi download, make sure terlebih dahulu apakah file sudah exist
        if operation == 'download':
            logging.info(f"Ensuring test file exists on server for download test")
            upload_result = self.remote_upload(test_file, 0)

            if upload_result['status'] != 'OK':
                logging.error(f"Upload failed: {upload_result.get('error', 'Unknown error')}")
                return None
        # Kalau thread, pakai threadpoolexecutor
        if executor_type == 'thread':
            executor_class = concurrent.futures.ThreadPoolExecutor
        else:  # Kalau process, pakai processpoolexecutor
            executor_class = concurrent.futures.ProcessPoolExecutor
        
        # Jalankan stress test nya
        all_results = []
        
        with executor_class(max_workers=client_pool_size) as executor:
            futures = []
            
            for i in range(client_pool_size):
                if operation == 'upload': # sesuaikan dengan fungsi
                    futures.append(executor.submit(self.remote_upload, test_file, i))

                elif operation == 'download':
                    file_name = os.path.basename(test_file)
                    futures.append(executor.submit(self.remote_download, file_name, i))

                else: # List
                    futures.append(executor.submit(self.remote_list, i))
            
            for future in concurrent.futures.as_completed(futures):
                try:
                    result = future.result()
                    all_results.append(result)
                    self.results[operation].append(result)
                except Exception as e:
                    logging.error(f"Worker failed with exception: {str(e)}")
        
        success_count = sum(1 for r in all_results if r['status'] == 'OK')
        fail_count    = len(all_results) - success_count
        
        # Menghitung durasi dan throughputs
        durations = [r['duration'] for r in all_results if r['status'] == 'OK']
        throughputs = [r['throughput'] for r in all_results if r.get('throughput', 0) > 0]
        
        # if not durations:
        #    logging.warning("There is no successful operations")
        #    return {
        #        'operation': operation, 'file_size_mb': file_size_mb, 'client_pool_size': client_pool_size,
        #        'executor_type': executor_type, 'success_count': self.success_count[operation], 'fail_count': self.fail_count[operation]
        #    }
        
        stats = {
            'operation': operation,
            'file_size_mb': file_size_mb,
            'client_pool_size': client_pool_size,
            'executor_type': executor_type,
            'avg_duration': statistics.mean(durations) if durations else 0,
            'avg_throughput': statistics.mean(throughputs) if throughputs else 0,
            'success_count': success_count,
            'fail_count': fail_count
        }
        
        logging.info(f"Test complete: {stats['success_count']} succeeded, {stats['fail_count']} failed")
        logging.info(f"Average duration: {stats['avg_duration']:.2f}s, Average throughput: {stats['avg_throughput']/1024/1024:.2f} MB/s")
        
        return stats

    def run_combination_tests(self, file_sizes, client_pool_sizes, server_pool_sizes, executor_types, operations):
        all_stats = []
        
        # Kita harus restart manual saat hendak berganti server pool size
        for server_pool_size in server_pool_sizes:
            logging.info(f"Tests for server pool size: {server_pool_size}")
            logging.info("Input the appropriate pool size to continue and restarting the server!")
            input("Press Enter when the server is ready...")
            
            for executor_type in executor_types:
                for operation in operations:
                    for file_size in file_sizes:
                        for client_pool_size in client_pool_sizes:
                            stats = self.run_stress_test(operation, file_size, client_pool_size, executor_type)
                            if stats:
                                stats['server_pool_size'] = server_pool_size
                                all_stats.append(stats)
        
        self.save_csv(all_stats) # save hasilnya ke csv
    
    def save_csv(self, all_stats):
        timestamp = time.strftime("%Y%m%d-%H%M%S")
        csv_filename = f"stress_test_{timestamp}.csv"
        
        with open(csv_filename, 'w', newline='') as csvfile:
            fieldnames = [
                'Nomor',
                'Operasi', 'Volume (MB)', 'Jumlah Client Worker Pool', 'Jumlah Server Worker Pool',
                'Waktu total per client (s)', 'Throughput per client (bytes/s)',
                'Jumlah Worker Sukses', 'Jumlah Worker Gagal',
                'Executor Type'
            ]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            
            writer.writeheader()
            total_success = 0
            total_fail = 0

            for i, stats in enumerate(all_stats, start=1):
                stats_with_number = {
                    'Nomor': i,
                    'Operasi': stats['operation'],
                    'Volume (MB)': stats['file_size_mb'],
                    'Jumlah Client Worker Pool': stats['client_pool_size'],
                    'Jumlah Server Worker Pool': stats.get('server_pool_size', '-'),
                    'Waktu total per client (s)': stats['avg_duration'],
                    'Throughput per client (bytes/s)': stats['avg_throughput'],
                    'Jumlah Worker Sukses': stats['success_count'],
                    'Jumlah Worker Gagal': stats['fail_count'],
                    'Executor Type': stats['executor_type']
                }
                total_success += stats['success_count']
                total_fail += stats['fail_count']
                writer.writerow(stats_with_number)

            # Tambahkan ringkasan total di akhir
            writer.writerow({
                'Nomor': 'TOTAL',
                'Jumlah Worker Sukses': total_success,
                'Jumlah Worker Gagal': total_fail
            })

        logging.info(f"Results already saved to {csv_filename}")
        return csv_filename


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='File Server Stress Test Client')
    parser.add_argument('--host', default='localhost', help='Server host (default: localhost)')
    parser.add_argument('--port', type=int, default=6666, help='Server port (default: 6666)')
    parser.add_argument('--operation', nargs='+', choices=['upload', 'download', 'list', 'all'], default=['all'], 
                        help='Operation to test (default: all)')
    parser.add_argument('--file-sizes', type=int, nargs='+', default=[10, 50, 100], 
                        help='File sizes in MB (default: 10 50 100)')
    parser.add_argument('--client-pools', type=int, nargs='+', default=[1, 5, 10], 
                        help='Client worker pool sizes (default: 1 5 10)')
    parser.add_argument('--server-pools', type=int, nargs='+', default=[1, 5, 10], 
                        help='Server worker pool sizes to test against (default: 1 5 10)')
    parser.add_argument('--executor', choices=['thread', 'process', 'both'], default='thread', 
                        help='Executor type (default: thread)')
    parser.add_argument('--debug', action='store_true', help='Enable debug logging')
    
    args = parser.parse_args()
    
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
    
    file_sizes = args.file_sizes
    client_pool_sizes = args.client_pools
    server_pool_sizes = args.server_pools
    
    # kalau sudah both tidak bisa >1 argumen
    if args.executor == 'both':
        executor_types = ['thread', 'process']
    else:
        executor_types = [args.executor]
        
    # kalau sudah all tidak bisa >1 argumen
    if 'all' in args.operation:
        operations = ['list', 'download', 'upload']
    else:
        operations = args.operation

    
    client = StressTestClient((args.host, args.port))
    
    # Untuk single test (without combination)
    if len(operations) == 1 and len(file_sizes) == 1 and len(client_pool_sizes) == 1 and len(server_pool_sizes) == 1:
        logging.info(f"Running a single test with operation={operations[0]}, file_size={file_sizes[0]}MB, client_pool={client_pool_sizes[0]}")
        stats = client.run_stress_test(operations[0], file_sizes[0], client_pool_sizes[0], executor_types[0])
        
        if stats:
            stats['server_pool_size'] = server_pool_sizes[0]
            client.save_csv([stats])

    else:
        # Untuk semua test combination
        client.run_combination_tests(file_sizes, client_pool_sizes, server_pool_sizes, executor_types, operations)
