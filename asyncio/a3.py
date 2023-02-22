#!/usr/bin/env python3
# countasync.py

import asyncio
import time

async def one():
    print(f"One {time.time()}")
    #time.sleep()
    await asyncio.sleep(1)
    print(f"ONE {time.time()}")

async def two():
    print(f"Two {time.time()}")
    time.sleep(3)
    await asyncio.sleep(5)
    print(f"TWO {time.time()}")

async def three():
    print(f"Three {time.time()}")
    time.sleep(3)
    await asyncio.sleep(3)
    print(f"THREE {time.time()}")

async def count_long():
    print(f"One long {time.time()}")
    await asyncio.sleep(15)
    print(f"Two long {time.time()}")

async def main():
    await asyncio.gather(count_long(), one(), two(), three())

if __name__ == "__main__":
    import time
    s = time.perf_counter()
    asyncio.run(main())
    elapsed = time.perf_counter() - s
    print(f"{__file__} executed in {elapsed:0.2f} seconds.")