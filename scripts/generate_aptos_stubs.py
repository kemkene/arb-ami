#!/usr/bin/env python3
"""Generate Aptos fullnode protobuf stubs from aptos-core repository."""

import os
import sys
import subprocess
from pathlib import Path

def download_aptos_protos(target_dir: str = ".") -> bool:
    """Download Aptos fullnode protobuf files from GitHub."""
    target_path = Path(target_dir) / "aptos" / "internal" / "fullnode" / "v1"
    os.makedirs(target_path, exist_ok=True)
    
    # Create __init__.py files
    (Path(target_dir) / "aptos").mkdir(exist_ok=True)
    (Path(target_dir) / "aptos" / "__init__.py").touch()
    (Path(target_dir) / "aptos" / "internal").mkdir(exist_ok=True)
    (Path(target_dir) / "aptos" / "internal" / "__init__.py").touch()
    (Path(target_dir) / "aptos" / "internal" / "fullnode").mkdir(exist_ok=True)
    (Path(target_dir) / "aptos" / "internal" / "fullnode" / "__init__.py").touch()
    (target_path).mkdir(exist_ok=True)
    (target_path / "__init__.py").touch()
    
    print("Downloading Aptos fullnode protobuf definitions...")
    
    # Base URL for Aptos protos
    base_url = "https://raw.githubusercontent.com/aptos-labs/aptos-core/main/protos/aptos/internal/fullnode/v1"
    
    files = [
        "fullnode_data.proto",
        "move_types.proto",
        "common.proto",
    ]
    
    for filename in files:
        url = f"{base_url}/{filename}"
        filepath = target_path / filename
        
        print(f"  Downloading {filename}...")
        try:
            subprocess.run(
                ["curl", "-fsSL", url, "-o", str(filepath)],
                check=True,
                capture_output=True,
            )
            print(f"    ✓ {filename}")
        except subprocess.CalledProcessError as e:
            print(f"    ✗ Failed to download {filename}: {e}")
            return False
    
    return True

def generate_stubs(proto_dir: str = ".") -> bool:
    """Generate Python gRPC stubs from protobuf files."""
    proto_path = Path(proto_dir) / "aptos" / "internal" / "fullnode" / "v1"
    
    if not proto_path.exists():
        print(f"Error: Proto directory not found: {proto_path}")
        return False
    
    print("\nGenerating Python gRPC stubs...")
    
    # Change to project directory to generate relative imports
    original_dir = os.getcwd()
    os.chdir(proto_dir)
    
    try:
        subprocess.run(
            [
                sys.executable,
                "-m",
                "grpc_tools.protoc",
                "-I.",
                "--python_out=.",
                "--grpc_python_out=.",
                str(proto_path / "fullnode_data.proto"),
            ],
            check=True,
        )
        print("  ✓ Generated fullnode_data_pb2.py")
        print("  ✓ Generated fullnode_data_pb2_grpc.py")
        return True
    except subprocess.CalledProcessError as e:
        print(f"  ✗ Failed to generate stubs: {e}")
        return False
    finally:
        os.chdir(original_dir)

def main():
    """Main entry point."""
    project_dir = os.path.dirname(os.path.abspath(__file__))
    
    print("=" * 60)
    print("APTOS FULLNODE PROTOBUF STUB GENERATION")
    print("=" * 60)
    
    # Step 1: Download protos
    if not download_aptos_protos(project_dir):
        print("\n✗ Failed to download protobuf files")
        return False
    
    # Step 2: Generate stubs
    if not generate_stubs(project_dir):
        print("\n✗ Failed to generate stubs")
        return False
    
    print("\n" + "=" * 60)
    print("✓ PROTOBUF STUB GENERATION COMPLETE")
    print("=" * 60)
    print("\nYou can now use in code:")
    print("  from aptos.internal.fullnode.v1 import fullnode_data_pb2")
    print("  from aptos.internal.fullnode.v1 import fullnode_data_pb2_grpc")
    
    return True

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
