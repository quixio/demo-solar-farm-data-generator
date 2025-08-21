import time
import json
import websocket  # websocket-client package

WS_URL = "wss://ws.blockchain.info/inv"
SAMPLE_COUNT = 10
TIMEOUT_SECONDS = 30

def run_connection_test():
    print("Bitcoin Transaction WebSocket Connection Test")
    print("WebSocket URL:", WS_URL)
    print("Sample count:", SAMPLE_COUNT)
    print("Connection timeout:", f"{TIMEOUT_SECONDS}s")
    try:
        # create a synchronous connection (websocket-client)
        ws = websocket.create_connection(WS_URL, timeout=TIMEOUT_SECONDS)
        # subscribe to unconfirmed transactions
        ws.send(json.dumps({"op": "unconfirmed_sub"}))

        samples = []
        start = time.time()

        while len(samples) < SAMPLE_COUNT:
            if time.time() - start > TIMEOUT_SECONDS:
                print(f"Timeout reached after {TIMEOUT_SECONDS}s, collected {len(samples)} samples")
                break
            try:
                msg = ws.recv()  # blocking with the connection-level timeout
            except websocket.WebSocketTimeoutException:
                # continue waiting until overall timeout
                continue
            if not msg:
                continue
            try:
                obj = json.loads(msg)
            except Exception:
                obj = msg
            samples.append(obj)
            print(f"Sample {len(samples)}: {json.dumps(obj)}")

        ws.close()
        print("Connection test finished.")
    except AttributeError as e:
        # Most likely wrong 'websocket' package is installed
        print("ERROR: websocket implementation missing in the 'websocket' module.")
        print("Make sure you have installed the 'websocket-client' package (not 'websocket').")
        raise
    except Exception as e:
        print("Unexpected error during connection test:", str(e))
        raise

if __name__ == "__main__":
    run_connection_test()