import os
os.system("python mqtt-backend.py")
import asyncio

loop = asyncio.get_event_loop()
loop.run_forever()