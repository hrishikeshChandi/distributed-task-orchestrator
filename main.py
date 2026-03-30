from fastapi import FastAPI
from routers import task, workers
from core.limiter import limiter
import uvicorn
from slowapi.middleware import SlowAPIMiddleware
from slowapi.errors import RateLimitExceeded
from slowapi import _rate_limit_exceeded_handler
from config.constants import HOST, MODULE, PORT

app = FastAPI()

app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)
app.add_middleware(SlowAPIMiddleware)

app.include_router(task.router, prefix="/tasks", tags=["tasks"])
app.include_router(workers.router, prefix="/workers", tags=["workers"])

if __name__ == "__main__":
    uvicorn.run(
        MODULE,
        host=HOST,
        port=PORT,
        reload=True,
    )
