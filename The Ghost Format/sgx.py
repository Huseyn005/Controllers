import os
import struct

target_folder = r"C:\Users\ASUS\Desktop\caspian_hackathon_assets\track_1_forensics"

def analyze_sgx(file_path):
    print(f"--- INSPECTING: {os.path.basename(file_path)} ---")
    
    with open(file_path, 'rb') as f:
        # Read the first 4KB of the file
        header_data = f.read(4000)

    # CHECK 1: Is there a Text Header? (Standard SEG-Y has one)
    # It is usually 3200 bytes long. It can be ASCII or EBCDIC (old IBM format).
    print("\n[1] Text Header Analysis (Bytes 0-3200):")
    text_block = header_data[:3200]
    
    # Try Decoding as ASCII
    try:
        ascii_head = text_block.decode('ascii')
        if "C " in ascii_head[:20]: # Seismic headers usually start with "C 1 CLIENT..."
            print(" -> FOUND ASCII HEADER! (Modern Format)")
            print(f"Snippet: {ascii_head[:100]}...")
    except:
        pass

    # Try Decoding as EBCDIC (Legacy Format)
    try:
        ebcdic_head = text_block.decode('cp500') 
        if "C " in ebcdic_head[:20]:
            print(" -> FOUND EBCDIC HEADER! (Legacy/IBM Format)")
            print(f"Snippet: {ebcdic_head[:100]}...")
    except:
        pass

    # CHECK 2: Is there a Binary Header? (Standard SEG-Y is Bytes 3200-3600)
    print("\n[2] Binary Header Analysis (Bytes 3200-3600):")
    bin_block = header_data[3200:3600]

    try:
        # Unpack as Big Endian (Standard for SEG-Y)
        interval, num_samples, fmt_code = struct.unpack('>HHH', bin_block[16:22])
        print(f" -> Sample Interval: {interval} microseconds")
        print(f" -> Samples per Trace: {num_samples}")
        print(f" -> Format Code: {fmt_code} (1=IBM Float, 5=IEEE Float, etc)")
    except Exception as e:
        print(f" -> Could not parse binary header: {e}")

    # CHECK 3: Raw Hex Dump (In case it's completely custom)
    print("\n[3] First 16 Bytes (Hex):")
    print(" ".join(f"{b:02X}" for b in header_data[:16]))

if __name__ == "__main__":
    if not os.path.exists(target_folder):
        print("Folder not found.")
    else:
        found = False
        for f in os.listdir(target_folder):
            if f.endswith(".sgx"):
                analyze_sgx(os.path.join(target_folder, f))
                found = True
                break
        if not found:
            print("No .sgx files found.")