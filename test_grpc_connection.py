#!/usr/bin/env python3
"""Diagnostic test for Aptos gRPC connection."""

import grpc
from config.settings import settings
from utils.logger import get_logger

logger = get_logger()


def test_grpc_basic():
    """Test basic gRPC connectivity and service availability."""
    print()
    print("=" * 70)
    print("APTOS GRPC CONNECTION DIAGNOSTIC")
    print("=" * 70)
    print()
    
    # Config
    endpoint = settings.cellana_grpc_endpoint
    api_key = settings.cellana_grpc_api_key
    use_tls = settings.cellana_grpc_use_tls
    
    print(f"Configuration:")
    print(f"  Endpoint:    {endpoint}")
    print(f"  Use TLS:     {use_tls}")
    print(f"  API Key Set: {bool(api_key)}")
    if api_key:
        print(f"  API Key:     {api_key[:30]}...")
    print()
    
    try:
        # Create channel
        print("Step 1: Creating gRPC channel...")
        options = [
            ("grpc.max_receive_message_length", -1),
            ("grpc.keepalive_time_ms", 30000),
            ("grpc.keepalive_timeout_ms", 10000),
        ]
        
        channel = grpc.secure_channel(
            endpoint,
            grpc.ssl_channel_credentials(),
            options=options
        )
        print("✓ Channel created successfully")
        print()
        
        # Try to import proto
        print("Step 2: Importing proto stubs...")
        try:
            from aptos.indexer.v1 import raw_data_pb2
            from aptos.indexer.v1 import raw_data_pb2_grpc
            print("✓ Proto stubs imported successfully")
        except ImportError as e:
            print(f"✗ Failed to import proto stubs: {e}")
            return
        print()
        
        # Create stub
        print("Step 3: Creating RawData stub...")
        stub = raw_data_pb2_grpc.RawDataStub(channel)
        print("✓ Stub created successfully")
        print()
        
        # Test metadata
        print("Step 4: Preparing metadata...")
        metadata = [
            ("authorization", f"Bearer {api_key}"),
            ("x-aptos-request-name", "diagnostic-test"),
        ]
        print(f"✓ Metadata prepared: {len(metadata)} headers")
        print()
        
        # Try to create a request
        print("Step 5: Creating GetTransactionsRequest...")
        request = raw_data_pb2.GetTransactionsRequest(
            starting_version=0,
            transactions_count=1,  # Just get 1 transaction
        )
        print("✓ Request created successfully")
        print()
        
        # Try to call the service
        print("Step 6: Calling GetTransactions RPC...")
        try:
            # Try with timeout to not block forever
            call = stub.GetTransactions(request, metadata=metadata, timeout=10)
            print("✓ RPC call initiated successfully")
            print()
            
            # Try to get first response
            print("Step 7: Waiting for first response...")
            try:
                response = next(iter(call))
                print("✓ Got response successfully!")
                print(f"  - Transactions: {len(response.transactions)}")
                print(f"  - Chain ID: {response.chain_id}")
                print()
                print("SUCCESS! gRPC connection and authentication working!")
            except StopIteration:
                print("✓ Stream closed (expected for test)")
                print("SUCCESS! gRPC connection and authentication working!")
            except Exception as e:
                print(f"✗ Error getting response: {e}")
                print(f"  Error code: {type(e).__name__}")
                
        except grpc.RpcError as e:
            print(f"✗ RPC Error: {e.code()}")
            print(f"  Details: {e.details()}")
            print()
            print("DIAGNOSIS:")
            if "UNIMPLEMENTED" in str(e):
                print("- Service may not be available at this endpoint")
                print("- Or API key may be invalid for this service")
            elif "UNAUTHENTICATED" in str(e):
                print("- API key is invalid or expired")
            elif "PERMISSION_DENIED" in str(e):
                print("- API key doesn't have permission for this RPC")
            else:
                print(f"- {e}")
        
        channel.close()
        
    except Exception as e:
        print(f"✗ Error: {e}")
        import traceback
        traceback.print_exc()
    
    print()
    print("=" * 70)
    print()


if __name__ == "__main__":
    test_grpc_basic()
