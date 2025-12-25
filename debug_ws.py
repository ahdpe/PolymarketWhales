import asyncio
import websockets
import json

URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
# Try a known active token ID or just any string? Better to use a real one.
# I'll fetch one first or hardcode one if I knew it.
# Let's use a dummy one or fetch one.
# 0x2d5ddf657e4a090bc22921bf6865bcdb741a7b96ce45eb583be041756fad04a0 (from previous curl)

TOKEN_ID = "21742633143463906290569050155826241533067272736897614382184511243729881000263" 
# That looks like a decimal ID. CLOB usually uses "0x..." or decimal string?
# Previous curl output: "condition_id": "0x5eed..."
# Token ID is usually 0x...
# Let's fetch one real quick in the script?
# Or just use the one from the log "Fetched 40 active tokens".
# I'll just use a hardcoded one for now.
# Token ID for "Yes" usually. 
# Asset ID: "4288626602370726194474775464197436098045543314412218820420793616238622197607" (Example from docs)
TOKEN = "4288626602370726194474775464197436098045543314412218820420793616238622197607"

async def test_sub(payload):
    print(f"Testing payload: {payload}")
    try:
        async with websockets.connect(URL) as ws:
            await ws.send(json.dumps(payload))
            print("Sent.")
            try:
                msg = await asyncio.wait_for(ws.recv(), timeout=5)
                print(f"Received: {msg}")
            except asyncio.TimeoutError:
                print("Timeout receiving response.")
    except Exception as e:
        print(f"Error: {e}")

async def main():
    # Payload 1: My original attempt
    p1 = {
        "type": "subscribe",
        "channel": "trades",
        "market": TOKEN
    }
    
    # Payload 2: "market" channel with assets_ids
    p2 = {
        "type": "subscribe",
        "channel": "market",
        "assets_ids": [TOKEN]
    }
    
    # Payload 3: Just "market" and assets_ids
    p3 = {
        "type": "market",
        "assets_ids": [TOKEN]
    }

    # Payload 4: "assets_ids" and timestamp?
    
    await test_sub(p1)
    await test_sub(p2)
    await test_sub(p3)

if __name__ == "__main__":
    asyncio.run(main())
