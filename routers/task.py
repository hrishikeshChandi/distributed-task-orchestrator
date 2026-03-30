from fastapi import APIRouter, HTTPException, status, Request
from db.connection import task_collection, workers_collection, client
from models.model import Task, TaskCreate, TaskResponse, StatusUpdate
from bson import ObjectId
from pymongo import ReturnDocument
from datetime import datetime, timezone
from core.limiter import limiter

router = APIRouter()


def _task_to_response(task: dict) -> TaskResponse:
    task["id"] = str(task["_id"])
    del task["_id"]
    return TaskResponse(**task)


@router.get(
    "/get_task",
    response_model=TaskResponse,
    status_code=status.HTTP_200_OK,
)
@limiter.limit("20/second")
async def get_task(request: Request, worker_id: str):
    try:
        worker_obj_id = ObjectId(worker_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid worker_id")

    worker = workers_collection.find_one({"_id": worker_obj_id})
    if not worker:
        raise HTTPException(
            status_code=404,
            detail=f"Worker with id: {worker_id} does not exist",
        )

    now = datetime.now(timezone.utc)

    # Use a MongoDB session + transaction to eliminate the TOCTOU race condition
    # between claiming a task and decrementing worker resources.
    with client.start_session() as session:
        with session.start_transaction():
            task = task_collection.find_one_and_update(
                {
                    "status": "pending",
                    "required_cpu": {"$lte": worker["available_cpu"]},
                    "required_ram": {"$lte": worker["available_ram"]},
                },
                {
                    "$set": {
                        "status": "running",
                        "assigned_worker": worker_id,
                        "started_at": now,
                        "updated_at": now,
                    }
                },
                sort=[("priority", -1), ("created_at", 1)],
                return_document=ReturnDocument.AFTER,
                session=session,
            )

            if not task:
                raise HTTPException(
                    status_code=404,
                    detail="No suitable tasks available",
                )

            allocated_cpu = task["required_cpu"]
            allocated_ram = task["required_ram"]

            result = workers_collection.update_one(
                {
                    "_id": worker_obj_id,
                    "available_cpu": {"$gte": allocated_cpu},
                    "available_ram": {"$gte": allocated_ram},
                },
                {
                    "$inc": {
                        "available_cpu": -allocated_cpu,
                        "available_ram": -allocated_ram,
                    },
                    "$push": {"assigned_tasks": str(task["_id"])},
                },
                session=session,
            )

            if result.modified_count == 0:
                # Abort rolls back the task update automatically
                session.abort_transaction()
                raise HTTPException(
                    status_code=409,
                    detail="Worker capacity conflict, retry",
                )

            task_collection.update_one(
                {"_id": task["_id"]},
                {
                    "$set": {
                        "allocated_cpu": allocated_cpu,
                        "allocated_ram": allocated_ram,
                    }
                },
                session=session,
            )

    task["allocated_cpu"] = allocated_cpu
    task["allocated_ram"] = allocated_ram

    return _task_to_response(task)


@router.get(
    "/tasks",
    response_model=list[TaskResponse],
    status_code=status.HTTP_200_OK,
)
async def get_tasks(request: Request):
    tasks = task_collection.find({"status": "pending"})
    return [_task_to_response(task) for task in tasks]


@router.post(
    "/submit",
    response_model=TaskResponse,
    status_code=status.HTTP_201_CREATED,
)
@limiter.limit("20/minute")
async def submit_task(request: Request, inputTask: TaskCreate):
    # Reject tasks that no existing worker can ever satisfy
    capable_worker = workers_collection.find_one(
        {
            "cpu_cores": {"$gte": inputTask.required_cpu},
            "ram": {"$gte": inputTask.required_ram},
        }
    )
    if not capable_worker:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=(
                f"No worker exists with at least {inputTask.required_cpu} CPU "
                f"and {inputTask.required_ram} RAM. Task would never be scheduled."
            ),
        )

    try:
        task = Task(
            data=inputTask.data,
            required_cpu=inputTask.required_cpu,
            required_ram=inputTask.required_ram,
            priority=inputTask.priority,
        )
        result = task_collection.insert_one(task.model_dump())
        return TaskResponse(
            id=str(result.inserted_id),
            **task.model_dump(),
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error submitting task ({str(e)})",
        )


@router.put(
    "/update_status/{task_id}",
    response_model=TaskResponse,
    status_code=status.HTTP_200_OK,
)
@limiter.limit("20/minute")
async def update_task(request: Request, task_id: str, update: StatusUpdate):
    if update.status.lower() not in ["completed", "failed"]:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Only 'completed' or 'failed' are allowed",
        )

    try:
        obj_id = ObjectId(task_id)
    except Exception:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid task id",
        )

    task = task_collection.find_one({"_id": obj_id})
    if not task:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Task not found",
        )

    worker_id = task.get("assigned_worker")
    allocated_cpu = task.get("allocated_cpu", 0)
    allocated_ram = task.get("allocated_ram", 0)
    now = datetime.now(timezone.utc)

    task_collection.update_one(
        {"_id": obj_id},
        {
            "$set": {
                "status": update.status.lower(),
                "updated_at": now,
                "allocated_cpu": 0,
                "allocated_ram": 0,
                "assigned_worker": None,
            },
        },
    )

    if worker_id:
        try:
            worker_obj_id = ObjectId(worker_id)
        except Exception:
            worker_obj_id = worker_id

        workers_collection.update_one(
            {"_id": worker_obj_id},
            {
                "$inc": {
                    "available_cpu": allocated_cpu,
                    "available_ram": allocated_ram,
                },
                "$pull": {"assigned_tasks": task_id},
            },
        )

    updated_task = task_collection.find_one({"_id": obj_id})
    return _task_to_response(updated_task)
