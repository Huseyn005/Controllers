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

def scan_parquet_files(directory):
    print(f"Starting scan in: {directory}\n")
    
    files_found = 0
    for root, dirs, files in os.walk(directory):
        for filename in files:
            if filename.endswith(".parquet"):
                files_found += 1
                full_path = os.path.join(root, filename)
                
                try:
                    with open(full_path, 'rb') as f:
                        file_size = os.path.getsize(full_path)

                        read_size = min(file_size, 2000)
                        f.seek(-read_size, os.SEEK_END)
                        tail_data = f.read()
                        
                        decoded = tail_data.decode('latin-1')
                        
                        found_snippets = re.findall(r'[a-zA-Z0-9_{}-]{5,}', decoded)
                        
                        interesting_hits = [s for s in found_snippets if "CTF" in s or "flag" in s or "{" in s]
                        
                        if interesting_hits:
                            print(f"\n[+] SUSPICIOUS FILE FOUND: {filename}")
                            print(f"    Path: {full_path}")
                            print("    Potential Flags:")
                            for hit in interesting_hits:
                                print(f"    -> {hit}")
                            print("-" * 40)
                            
                except Exception as e:
                    print(f"Error reading {filename}: {e}")

    print(f"\nScan complete. Checked {files_found} parquet files.")

if __name__ == "__main__":
    scan_parquet_files(target_folder)