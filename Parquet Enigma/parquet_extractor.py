import pandas as pd
import struct
import io
import os
import re
import argparse

parser = argparse.ArgumentParser()
parser.add_argument(
    "--data-dir",
    required=True,
    help="Exact data directory passed from bash"
)

args = parser.parse_args()

target_folder = args.data_dir

def try_load_parquet(file_bytes, original_path, fix_type):
    try:
        virtual_file = io.BytesIO(file_bytes)
        df = pd.read_parquet(virtual_file)
        
        output_csv = original_path.replace(".parquet", "_RECOVERED.csv")
        df.to_csv(output_csv, index=False)
        print(f"    [+] SUCCESS ({fix_type})! Data saved to: {os.path.basename(output_csv)}")
        return True
    except Exception:
        return False

def repair_file(file_path):
    filename = os.path.basename(file_path)
    print(f"[*] Analyzing: {filename}...")
    
    if "RECOVERED" in filename:
        return

    with open(file_path, 'rb') as f:
        raw_data = bytearray(f.read())

    par1_locations = [m.start() for m in re.finditer(b'PAR1', raw_data)]
    
    for loc in par1_locations:
        cut_point = loc + 4
        
        if cut_point < len(raw_data):
            trimmed_data = raw_data[:cut_point]
            if try_load_parquet(trimmed_data, file_path, "Garbage Trimmed"):
                return

    file_size = len(raw_data)
    
    if raw_data[-4:] == b'PAR1':
        print("    [.] File ends in PAR1. Attempting to crack metadata size...")
        
        for candidate_length in range(1, file_size - 8):
            struct.pack_into('<I', raw_data, file_size - 8, candidate_length)
            
            if try_load_parquet(raw_data, file_path, f"Size Fixed: {candidate_length}"):
                return

    print("    [-] File could not be repaired.")

def main():
    if not os.path.exists(target_folder):
        print(f"Error: Folder not found: {target_folder}")
        return

    for root, dirs, files in os.walk(target_folder):
        for file in files:
            if file.endswith(".parquet"):
                repair_file(os.path.join(root, file))

if __name__ == "__main__":
    main()