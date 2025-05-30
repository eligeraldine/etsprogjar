from socket import *
import logging
import socket
from file_protocol import FileProtocol # Import protokol file untuk parsing perintah
import concurrent.futures
import sys

fp = FileProtocol()

# Fungsi untuk manage setiap koneksi klien
def manage_client(connection, address):
    logging.warning(f"manage connection from {address}")
    buffer = ""
    try:
        connection.settimeout(1800) # Timeout koneksi selama 30 menit
        
        while True:
            data=connection.recv(1024*1024)
            if not data:
                break
            buffer=buffer+data.decode()
            while "\r\n\r\n" in buffer:
                command, buffer=buffer.split("\r\n\r\n", 1)
                hasil=fp.proses_string(command)
                response=hasil + "\r\n\r\n"
                connection.sendall(response.encode()) # Kirim respons ke klien
    
    except Exception as e:
        logging.warning(f"error: {str(e)}")
    finally:
        logging.warning(f"connection from {address} has closed")
        connection.close()


class Server:
    def __init__(self, ipaddress='0.0.0.0', port=8889, pool_size=5):
        self.ipinfo=(ipaddress, port)
        self.pool_size=pool_size
        self.my_socket=socket.socket(socket.AF_INET, socket.SOCK_STREAM) # Buat socket TCP
        self.my_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        
        self.my_socket.settimeout(1800)

    # Fungsi utama server untuk menerima dan memproses koneksi masuk
    def run(self):
        logging.warning(f"server running on ip address {self.ipinfo}, thread pool size is {self.pool_size}")
        self.my_socket.bind(self.ipinfo)
        self.my_socket.listen(5)
        
        # ThreadPoolExecutor
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.pool_size) as executor:
            try:
                while True:
                    connection, client_address=self.my_socket.accept()
                    logging.warning(f"connection from {client_address}")
                    
                    executor.submit(manage_client, connection, client_address)
            except KeyboardInterrupt:
                logging.warning("now server shutting down")
            finally:
                if self.my_socket:
                    self.my_socket.close()


def main():
    import argparse
    parser=argparse.ArgumentParser(description='File Server')
    parser.add_argument('--port', type=int, default=6666, help='Server port (default: 6666)')
    parser.add_argument('--pool-size', type=int, default=1, help='thread pool size (default: 1)')
    args=parser.parse_args()
    
    svr=Server(ipaddress='0.0.0.0', port=args.port, pool_size=args.pool_size)
    svr.run()


if __name__ == "__main__":
    logging.basicConfig(level=logging.WARNING, format='%(asctime)s - %(levelname)s - %(message)s')
    main()
