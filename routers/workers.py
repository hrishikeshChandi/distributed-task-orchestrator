from fastapi import APIRouter, status, HTTPException, Request
from db.connection import workers_collection
from models.model import WorkerCreate, WorkerResponse, Worker
from bson import ObjectId
from pymongo import ReturnDocument
from datetime import datetime, timezone
from core.limiter import limiter

router = APIRouter()


@router.get(
    "/get_workers",
    response_model=list[WorkerResponse],
    status_code=status.HTTP_200_OK,
)
@limiter.limit("10/minute")
async def get_workers(request: Request):
    workers = workers_collection.find()
    result = []
    for worker in workers:
        worker["id"] = str(worker["_id"])
        del worker["_id"]
        result.append(WorkerResponse(**worker))
    return result


@router.post(
    "/add_worker",
    response_model=WorkerResponse,
    status_code=status.HTTP_201_CREATED,
)
@limiter.limit("20/minute")
async def add_worker(request: Request, worker: WorkerCreate):
    try:
        worker_obj = Worker(
            cpu_cores=worker.cpu_cores,
            ram=worker.ram,
            available_cpu=worker.cpu_cores,
            available_ram=worker.ram,
        )
        result = workers_collection.insert_one(worker_obj.model_dump())
        return WorkerResponse(
            id=str(result.inserted_id),
            **worker_obj.model_dump(),
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error adding worker ({str(e)})",
        )


@router.put("/heartbeat/{worker_id}", status_code=status.HTTP_200_OK)
@limiter.limit("20/minute")
async def update_heartbeat(request: Request, worker_id: str):
    try:
        worker_obj_id = ObjectId(worker_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid worker_id")

    now = datetime.now(timezone.utc)

    worker = workers_collection.find_one_and_update(
        {"_id": worker_obj_id},
        {"$set": {"last_heartbeat": now, "status": "active"}},
        return_document=ReturnDocument.AFTER,
    )

    if not worker:
        raise HTTPException(
            status_code=404,
            detail=f"Worker with id {worker_id} does not exist",
        )

    return {"message": "heartbeat updated"}
