import os
import struct
import pandas as pd
import hashlib
from datetime import datetime

class CaspianSGX:
    def __init__(self, file_path):
        self.file_path = file_path
        self.filename = os.path.basename(file_path)
        self.header = {}
        self.traces = []
        self.raw_head = None

    def read(self):
        file_size = os.path.getsize(self.file_path)
        
        with open(self.file_path, 'rb') as f:
            # Spec: Magic(8s), SurveyID(I), TraceCount(I)
            self.raw_head = f.read(16)
            
            if len(self.raw_head) < 16:
                return False

            magic, survey_id, trace_count = struct.unpack('<8sII', self.raw_head)
            
            if magic != b'CPETRO01':
                return False

            self.header = {
                'survey_id': survey_id,
                'trace_count': trace_count,
                'file_size': file_size
            }

            # Spec: WellID(4), Depth(4), Amp(4), Quality(1)
            for _ in range(trace_count):
                record_bytes = f.read(13)
                if len(record_bytes) != 13: break
                
                # Unpack Little Endian (<)
                well_id, depth, amp, qual = struct.unpack('<IffB', record_bytes)
                
                self.traces.append({
                    'survey_id': survey_id, # Link to header
                    'well_id': well_id,
                    'depth': depth,
                    'amplitude': amp,
                    'quality_flag': qual
                })
            
            return True

    def to_dataframe(self):
        if not self.traces:
            return pd.DataFrame()

        # Convert list of dicts directly to DataFrame
        df = pd.DataFrame(self.traces)
        
        # Add Provenance Metadata (Required for Ep2 Vault)
        df['ingest_source'] = self.filename
        df['ingest_timestamp'] = datetime.now().isoformat()
        
        return df

def main(target_dir):
    if not os.path.exists(target_dir):
        print(f"Error: Directory not found at {target_dir}")
        return

    count = 0
    success_count = 0

    print("Scanning...")

    for root, dirs, files in os.walk(target_dir):
        for file in files:
            if file.endswith(".sgx"):
                count += 1
                full_path = os.path.join(root, file)
                
                try:
                    loader = CaspianSGX(full_path)
                    if loader.read():
                        print(f"\n--- {file} ---")
                        
                        # Print Header Info
                        magic = loader.raw_head[:8].decode()
                        sid = loader.header['survey_id']
                        t_count = loader.header['trace_count']
                        
                        print(f"File Signature: {magic},    ID   : Survey {sid}") 
                        print(f"  {'Trace Count':<12} : {t_count} records")

                        # Print Data Range
                        file_end = loader.header['file_size']
                        print(f" - Data Range: Bytes 16 - {file_end}")
                        print(f" - Structure : 13-byte records (WellID, Depth, Amp, Quality)")

                        # Convert & Save
                        df = loader.to_dataframe()
                        out_name = full_path.replace(".sgx", "_reconstructed.parquet")
                        df.to_parquet(out_name, index=False)
                        
                        print(f"Converted -> {os.path.basename(out_name)}")
                        success_count += 1
                    
                except Exception as e:
                    print(f"Failed {file}: {e}")

    print(f"\nDONE. Found {count} files. Converted {success_count}.")
