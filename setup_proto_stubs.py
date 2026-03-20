#!/usr/bin/env python3
"""Download and generate Aptos fullnode protobuf stubs."""

import os
import sys
import subprocess
import urllib.request
from pathlib import Path

def setup_directories(base_dir: str = ".") -> None:
    """Create all necessary directories."""
    dirs = [
        "aptos",
        "aptos/transaction",
        "aptos/transaction/v1",
        "aptos/indexer",
        "aptos/indexer/v1",
        "aptos/internal",
        "aptos/internal/fullnode",
        "aptos/internal/fullnode/v1",
    ]
    
    for d in dirs:
        path = Path(base_dir) / d
        os.makedirs(path, exist_ok=True)
        (path / "__init__.py").touch(exist_ok=True)

def download_proto_files(base_dir: str = ".") -> bool:
    """Download all required proto files from Aptos GitHub."""
    base_url = "https://raw.githubusercontent.com/aptos-labs/aptos-core/main/protos/proto"
    
    files = {
        "aptos/internal/fullnode/v1/fullnode_data.proto": f"{base_url}/aptos/internal/fullnode/v1/fullnode_data.proto",
        "aptos/transaction/v1/transaction.proto": f"{base_url}/aptos/transaction/v1/transaction.proto",
        "aptos/indexer/v1/grpc.proto": f"{base_url}/aptos/indexer/v1/grpc.proto",
    }
    
    print("Downloading Aptos proto files from aptos-core...")
    for filepath, url in files.items():
        full_path = Path(base_dir) / filepath
        print(f"  • {filepath}...", end=" ", flush=True)
        
        try:
            urllib.request.urlretrieve(url, full_path)
            print("✓")
        except Exception as e:
            print(f"✗ ({e})")
            return False
    
    return True

def generate_grpc_stubs(base_dir: str = ".") -> bool:
    """Generate Python gRPC stubs from proto files."""
    proto_file = Path(base_dir) / "aptos" / "internal" / "fullnode" / "v1" / "fullnode_data.proto"
    
    if not proto_file.exists():
        print(f"✗ Proto file not found: {proto_file}")
        return False
    
    print("\nGenerating Python gRPC stubs...")
    print(f"  • fullnode_data_pb2.py...", end=" ", flush=True)
    
    try:
        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "grpc_tools.protoc",
                f"-I{base_dir}",
                f"--python_out={base_dir}",
                f"--grpc_python_out={base_dir}",
                str(proto_file),
            ],
            capture_output=True,
            timeout=30,
        )
        
        if result.returncode == 0:
            print("✓")
            return True
        else:
            print(f"✗")
            print(f"  stderr: {result.stderr.decode()}")
            return False
    except Exception as e:
        print(f"✗ ({e})")
        return False

def main():
    """Main entry point."""
    base_dir = "."
    
    print("=" * 70)
    print("APTOS FULLNODE PROTOBUF STUB GENERATION")
    print("=" * 70)
    print()
    
    # Step 1: Setup directory structure
    print("[1/3] Setting up directory structure...")
    setup_directories(base_dir)
    print("  ✓ Directories created")
    print()
    
    # Step 2: Download proto files
    print("[2/3] Downloading proto files...")
    if not download_proto_files(base_dir):
        print("✗ Failed to download proto files")
        print()
        print("Tip: Make sure you have `curl` installed")
        return False
    print("✓ Proto files downloaded")
    print()
    
    # Step 3: Generate stubs
    print("[3/3] Generating gRPC stubs...")
    if not generate_grpc_stubs(base_dir):
        print("✗ Failed to generate stubs")
        return False
    print("✓ Stubs generated")
    print()
    
    print("=" * 70)
    print("✓ SUCCESS - Ready to use gRPC client")
    print("=" * 70)
    print()
    print("Run listener with:")
    print("  python test_listen_events.py")
    print()
    
    return True

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
