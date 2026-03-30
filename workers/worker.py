import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import requests
import time
import random
import threading
from config.constants import HOST, PORT, HEARTBEAT_INTERVAL

TASK_URL = f"http://{HOST}:{PORT}/tasks"
WORKER_URL = f"http://{HOST}:{PORT}/workers"


def register_worker() -> str:
    ram_options = [4, 6, 8, 12, 16, 24, 32, 48]
    cpu_options = [2, 4, 6, 8, 10, 12, 14, 16, 18, 20]

    cpu = random.choice(cpu_options)
    ram = random.choice(ram_options)

    response = requests.post(
        f"{WORKER_URL}/add_worker",
        json={"cpu_cores": cpu, "ram": ram},
    )

    if response.status_code != 201:
        raise RuntimeError(f"Failed to register worker: {response.text}")

    worker_id = response.json()["id"]
    print(f"Registered worker {worker_id}: CPU={cpu}, RAM={ram}")
    return worker_id


# ✅ FIX: independent heartbeat loop
def heartbeat_loop(worker_id: str):
    while True:
        try:
            requests.put(f"{WORKER_URL}/heartbeat/{worker_id}")
        except Exception as e:
            print(f"Heartbeat error: {e}")

        time.sleep(HEARTBEAT_INTERVAL)


def run():
    worker_id = register_worker()
    print(f"Worker started with id: {worker_id}")

    # ✅ start background heartbeat
    threading.Thread(
        target=heartbeat_loop,
        args=(worker_id,),
        daemon=True,
    ).start()

    while True:
        try:
            response = requests.get(
                f"{TASK_URL}/get_task",
                params={"worker_id": worker_id},
            )

            if response.status_code == 429:
                time.sleep(5)
                continue

            if response.status_code == 200:
                task = response.json()
                task_id = task["id"]

                print(f"Processing task: {task_id}")

                # simulate long task
                time.sleep(random.uniform(10, 60))

                new_status = "failed" if random.random() < 0.3 else "completed"

                requests.put(
                    f"{TASK_URL}/update_status/{task_id}",
                    json={"status": new_status},
                )

            else:
                print("No tasks available")

        except Exception as e:
            print(f"Error: {e}")

        time.sleep(random.uniform(1, 3))


if __name__ == "__main__":
    run()
