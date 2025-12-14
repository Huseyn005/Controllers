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

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, ".."))
BASE_OUTPUT_DIR = os.path.join(PROJECT_ROOT, "processed_data")
os.makedirs(BASE_OUTPUT_DIR, exist_ok=True)

def try_load_parquet(file_bytes, original_path, fix_type):
    try:
        virtual_file = io.BytesIO(file_bytes)
        df = pd.read_parquet(virtual_file)

        base_name = os.path.splitext(os.path.basename(original_path))[0]
        out_name = os.path.join(BASE_OUTPUT_DIR,f"{base_name}_reconstructed.parquet")

        df.to_parquet(out_name, index=False)

        print(f"    [+] SUCCESS ({fix_type})! Repaired parquet saved as: reconstructed.parquet")
        return True
    except Exception:
        return False


def repair_file(file_path):
    filename = os.path.basename(file_path)
    print(f"[*] Analyzing: {filename}...")

    if "_reconstructed" in filename:
        return

    with open(file_path, 'rb') as f:
        raw_data = bytearray(f.read())

    # Attempt 1: Trim garbage after PAR1
    par1_locations = [m.start() for m in re.finditer(b'PAR1', raw_data)]

    for loc in par1_locations:
        cut_point = loc + 4
        if cut_point < len(raw_data):
            trimmed_data = raw_data[:cut_point]
            if try_load_parquet(trimmed_data, file_path, "Garbage Trimmed"):
                return

    # Attempt 2: Bruteforce metadata size
    file_size = len(raw_data)

    if raw_data[-4:] == b'PAR1':
        print("    [.] File ends in PAR1. Attempting metadata size recovery...")

        for candidate_length in range(1, file_size - 8):
            struct.pack_into('<I', raw_data, file_size - 8, candidate_length)

            if try_load_parquet(raw_data, file_path, f"Metadata Size Fixed: {candidate_length}"):
                return

    print("    [-] File could not be repaired.")


def main():
    if not os.path.exists(target_folder):
        print(f"Error: Folder not found: {target_folder}")
        return

    for root, _, files in os.walk(target_folder):
        for file in files:
            if file.endswith(".parquet"):
                repair_file(os.path.join(root, file))


if __name__ == "__main__":
    main()
