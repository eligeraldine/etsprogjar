import json
import logging
import shlex

from file_interface import FileInterface

"""
* class FileProtocol bertugas untuk memproses 
data yang masuk, dan menerjemahkannya apakah sesuai dengan
protokol/aturan yang dibuat

* data yang masuk dari client adalah dalam bentuk bytes yang 
pada akhirnya akan diproses dalam bentuk string

* class FileProtocol akan memproses data yang masuk dalam bentuk
string
"""
class FileProtocol:
    def __init__(self):
        self.file = FileInterface()
        
    def proses_string(self, string_datamasuk=''):
        logging.warning(f"processing string of length: {len(string_datamasuk)}")
        try:
            if " " not in string_datamasuk:
                c_request = string_datamasuk.strip().lower()
                params = []
            else:
                parts = string_datamasuk.split(" ", 1)
                c_request = parts[0].strip().lower()
                
                if len(parts) < 2:
                    params = []
                else:
                    if c_request == "upload": # ada case khusus untuk upload untuk manage large base64 content
                        filename_and_content = parts[1].split(" ", 1)
                        params = filename_and_content
                    
                    else:
                        try:
                            params = shlex.split(parts[1])
                        except Exception as e:
                            logging.warning(f"error parsing parameters with shlex: {str(e)}")
                            params = parts[1].split()
            
            logging.warning(f"request processing: {c_request} --> {len(params)} parameters")


            if hasattr(self.file, c_request):
                cl = getattr(self.file, c_request)(params)
                return json.dumps(cl)
            else:
                return json.dumps(dict(status='ERROR', data='Unknown command'))
        
        except Exception as e:
            logging.warning(f"Request processing error: {str(e)}")
            return json.dumps(dict(status='ERROR', data=f'Request processing error: {str(e)}'))


if __name__=='__main__':
    #contoh pemakaian
    fp = FileProtocol()
    print(fp.proses_string("LIST"))
    print(fp.proses_string("GET pokijan.jpg"))
