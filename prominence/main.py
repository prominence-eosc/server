from fastapi import FastAPI
from .routers import jobs

metadata = [
    {
        "name": "jobs",
        "description": "Operations with jobs"
    }
]

app = FastAPI(
    title='PROMINENCE',
    description='Run containerised jobs across many clouds',
    version='0.0.0',
    openapi_tags=metadata
)

app.include_router(jobs.router)
