
import asyncio
import websockets


async def hello():
    async with websockets.connect("ws://") as websocket:
        while True:
            await websocket.send("Hello world!")
            await asyncio.sleep(3)    


if __name__=="__main__":
    asyncio.run(hello())
