import os
import struct

# UPDATE PATH
folder_path = r"C:\Users\ASUS\Desktop\caspian_hackathon_assets\track_1_forensics"

def crack_format(file_path):
    print(f"--- Analyzing: {os.path.basename(file_path)} ---")
    file_size = os.path.getsize(file_path)
    print(f"File Size: {file_size} bytes")

    with open(file_path, 'rb') as f:
        # Read the first 64 bytes
        header = f.read(64)

        ints = struct.unpack('<10I', header[8:48])
        
        print("Header Values (after CPETRO01):")
        for i, val in enumerate(ints):
            print(f"  Offset {8 + i*4}: {val}")

        possible_header_sizes = [32, 64, 128, 256]
        
        for h_size in possible_header_sizes:
            data_size = file_size - h_size
            if data_size <= 0: continue
            
            num_floats = data_size / 4
            
            if num_floats.is_integer():
                print(f"\n[?] Checking logic for Header Size {h_size}...")
                print(f"    Available Data Space: {int(num_floats)} floats")
                
                v1 = ints[0] # 101 (Survey ID?)
                v2 = ints[1] # 166 (Traces?)
                v3 = ints[2] # Next value...
                
                if v1 * v2 == num_floats:
                    print(f"    [!!!] MATCH FOUND!")
                    print(f"    Structure: {v1} x {v2} (SurveyID x Traces?)")
                elif v2 * v3 == num_floats:
                     print(f"    [!!!] MATCH FOUND!")
                     print(f"    Structure: {v2} x {v3} (Traces x Samples?)")
                else:
                    # Maybe just one number defines the count?
                    if v2 == num_floats:
                        print(f"    [!!!] MATCH FOUND! 1D Array of size {v2}")

if __name__ == "__main__":
    # Find the file and run
    for f in os.listdir(folder_path):
        if f.endswith(".sgx"):
            crack_format(os.path.join(folder_path, f))
            break # Just check the first one