"""API endpoint for managing jobs"""
import os
from fastapi import APIRouter, Body, HTTPException, status
from fastapi.responses import Response, JSONResponse
from fastapi.encoders import jsonable_encoder
from fastapi.responses import PlainTextResponse
from typing import List
import time
import shortuuid

from prominence.models import Job, JobOutput
from prominence.utilities import config
import prominence.database as database

router = APIRouter(
    prefix="/jobs",
    tags=["jobs"]
)

db = database.Database()

@router.post("/", response_description="Create a job")
async def create_job(job: Job = Body(...)):
    """
    Create a job
    """
    job = jsonable_encoder(job)
    job['id'] = shortuuid.uuid()
    job['status'] = 'pending'
    job['events'] = {}
    job['events']['createTime'] = time.time()
    job['execution'] = {}
    db.create_job(job)
    return JSONResponse(status_code=status.HTTP_201_CREATED, content={'id': job['id']})

@router.get(
    "/",
    response_description="List jobs",
    response_model=List[JobOutput],
    response_model_exclude_none=True,
)
def list_jobs():
    """
    List jobs
    """
    jobs_list = db.list_jobs()
    return jobs_list

@router.get(
    "/{id}",
    response_description="Get a single job",
    response_model=JobOutput,
    response_model_exclude_none=True
)
async def describe_job(id: str):
    """
    Describe a single job
    """
    job = db.get_job(id)
    if job:
        return job
    raise HTTPException(status_code=404, detail=f"Job {id} not found")

@router.get(
    "/{id}/stdout",
    response_description="Get job stdout",
    response_class=PlainTextResponse
)
def get_stdout(id: str):
    """
    Return the job standard output
    """
    filename = f"{config().get('job_logger', 'directory')}/job.stdout.{id}"

    if os.path.exists(filename):
        with open(filename, 'rb') as fd:
            return fd.read()
    else:
        return ''

@router.get(
    "/{id}/stderr",
    response_description="Get job stderr",
    response_class=PlainTextResponse
)
def get_stderr(id: str):
    """
    Return the job standard output
    """
    filename = f"{config().get('logger', 'directory')}/job.stderr.{id}"

    if os.path.exists(filename):
        with open(filename, 'rb') as fd:
            return fd.read()
    else:
        return ''

@router.delete("/{id}", response_description="Delete job")
def delete_job(id: str):
    """
    Delete job
    """
    db.delete_job(id)
    return JSONResponse(status_code=200, content={})
