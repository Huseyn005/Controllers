import os
import struct
import pandas as pd
import numpy as np

class CaspianSGX:
    def __init__(self, file_path):
        self.file_path = file_path
        self.filename = os.path.basename(file_path)
        self.header = {}
        self.data = None
        self.raw_head = None

    def read(self):
        file_size = os.path.getsize(self.file_path)
        
        with open(self.file_path, 'rb') as f:
            self.raw_head = f.read(64)
            
            if self.raw_head[:8] != b'CPETRO01':
                return False

            ints = struct.unpack('<4I', self.raw_head[8:24])
            self.header = {
                'survey_id': ints[0],
                'dim_1': ints[1],
                'dim_2': ints[2],
                'param_3': ints[3]
            }

            raw_data = f.read()
            data_len = len(raw_data)
            
            if data_len % 4 == 0:
                self.data = np.frombuffer(raw_data, dtype=np.float32)
                self.header['format'] = 'float32'
            elif data_len % 2 == 0:
                self.data = np.frombuffer(raw_data, dtype=np.int16)
                self.header['format'] = 'int16'
            else:
                self.data = np.frombuffer(raw_data, dtype=np.uint8)
                self.header['format'] = 'uint8'
            
            return True

    def to_dataframe(self):
        if self.data is None:
            return pd.DataFrame()

        d1 = self.header['dim_1']
        d2 = self.header['dim_2']
        total = len(self.data)
        
        matrix = None

        if d1 * d2 == total and d1 > 0:
            matrix = self.data.reshape((d2, d1))
        elif d1 > 0 and total % d1 == 0:
            matrix = self.data.reshape((-1, d1))
        else:
            matrix = self.data.reshape((-1, 1))

        df = pd.DataFrame(matrix)
        df.columns = df.columns.astype(str)
        
        df['survey_id'] = self.header['survey_id']
        df['format'] = self.header['format']
        
        return df

def main(DATA_DIR):
    if not os.path.exists(DATA_DIR):
        print(f"Error: Directory not found at {DATA_DIR}")
        return

    count = 0
    success_count = 0

    print("Scanning...")

    for root, dirs, files in os.walk(DATA_DIR):
        for file in files:
            if file.endswith(".sgx"):
                count += 1
                full_path = os.path.join(root, file)
                
                try:
                    loader = CaspianSGX(full_path)
                    if loader.read():
                        print(f"\n--- {file} ---")
                        
                        hex_bytes = " ".join(f"{b:02X}" for b in loader.raw_head[:16])
                        print(f"File Signature: {loader.raw_head[:8].decode()},    ID   : Survey {loader.header['survey_id']}") 
                        
                        d1, d2 = loader.header['dim_1'], loader.header['dim_2']
                        print(f"  {'Dimensions':<12} : {d1} x {d2} (Traces/Samples)")

                        file_end = os.path.getsize(full_path)

                        print(f" - Data Range: Bytes 64 - {file_end}")
                        print(f" - Storage: {loader.header['format']} (Little Endian)")

                        df = loader.to_dataframe()
                        out_name = full_path.replace(".sgx", ".parquet")
                        df.to_parquet(out_name)
                        
                        print(f"Converted -> {os.path.basename(out_name)}")
                        success_count += 1
                    
                except Exception as e:
                    print(f"Failed {file}: {e}")

    print(f"\nDONE. Found {count} files. Converted {success_count}.")
