import time

START_TIME = time.time()

def uptime_seconds() -> float:
    return time.time() - START_TIME