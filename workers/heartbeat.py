import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import time
from bson import ObjectId
from typing import List
from datetime import datetime, timezone
from db.connection import workers_collection, task_collection
from config.constants import MAX_RETRIES, HEARTBEAT_TIMEOUT


def free_resources(worker_id: str, assigned_tasks: List[str], now: datetime):
    for task_id in assigned_tasks:
        try:
            obj_id = ObjectId(task_id)
        except Exception:
            continue

        task = task_collection.find_one({"_id": obj_id})

        # ✅ extra safety: ensure task still belongs to this worker
        if not task or task.get("status") != "running":
            continue

        if str(task.get("assigned_worker")) != worker_id:
            continue

        cpu = task.get("allocated_cpu", 0)
        ram = task.get("allocated_ram", 0)
        current_retries = task.get("retry_count", 0)

        workers_collection.update_one(
            {"_id": ObjectId(worker_id)},
            {
                "$inc": {"available_cpu": cpu, "available_ram": ram},
                "$pull": {"assigned_tasks": str(task_id)},
            },
        )

        if current_retries < MAX_RETRIES:
            task_collection.update_one(
                {"_id": obj_id},
                {
                    "$set": {
                        "status": "pending",
                        "assigned_worker": None,
                        "started_at": None,
                        "updated_at": now,
                        "allocated_cpu": 0,
                        "allocated_ram": 0,
                    },
                    "$inc": {"retry_count": 1},
                },
            )
        else:
            task_collection.update_one(
                {"_id": obj_id},
                {
                    "$set": {
                        "status": "failed",
                        "assigned_worker": None,
                        "updated_at": now,
                        "allocated_cpu": 0,
                        "allocated_ram": 0,
                    }
                },
            )


def run():
    print("Heartbeat monitor started")

    while True:
        workers = workers_collection.find({"status": "active"})
        now = datetime.now(timezone.utc)

        for worker in workers:
            last_heartbeat = worker.get("last_heartbeat")

            if not last_heartbeat:
                continue

            if last_heartbeat.tzinfo is None:
                last_heartbeat = last_heartbeat.replace(tzinfo=timezone.utc)

            gap = (now - last_heartbeat).total_seconds()

            if gap > HEARTBEAT_TIMEOUT:
                # ✅ atomic death marking
                result = workers_collection.update_one(
                    {"_id": worker["_id"], "status": "active"},
                    {"$set": {"status": "dead"}},
                )

                if result.modified_count == 0:
                    continue  # already processed

                print(f"Worker {worker['_id']} DEAD (gap {gap:.0f}s)")

                free_resources(
                    worker_id=str(worker["_id"]),
                    assigned_tasks=worker.get("assigned_tasks", []),
                    now=now,
                )

        time.sleep(10)


if __name__ == "__main__":
    run()
