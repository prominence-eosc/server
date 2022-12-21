from pydantic import BaseModel, Field
from bson import ObjectId
from enum import Enum
from typing import Optional, List
from typing import Dict, List, Optional


class JobPolicies(BaseModel):
    """
    Job policies
    """
    maximumRetries: Optional[int] = Field(
        0,
        title='maximumRetries',
        description='Maximum retries'
    )
    maximumTaskRetries: Optional[int] = Field(
        0,
        title='maximumTaskRetries',
        description='Maximum task retries'
    )
    maximumTimeInQueue: Optional[int] = Field(
        0,
        title='maximumTimeInQueue',
        description='Maximum time in queue'
    )
    priority: Optional[int] = Field(
        0,
        title='priority',
        description='Job priority'
    )


class Artifact(BaseModel):
    """
    Artifact
    """
    url: str = Field(..., title="url", description="URL")
    mountpoint: Optional[str] = Field(..., title="mountpoint", description="Mountpoint")
    executable: Optional[bool] = Field(
        False,
        title="executable",
        description="If set to true artifact will be executable",
    )


class JobStatus(str, Enum):
    """
    Possible job states
    """
    pending = "pending"
    assigned = "assigned"
    running = "running"
    completed = "completed"
    failed = "failed"
    deleting = "deleting"
    deleted = "deleted"
    killed = "killed"


class ImagePullStatus(str, Enum):
    """
    Possible image pull states
    """
    completed = "completed"
    cached = "cached"
    failed = "failed"


class Resources(BaseModel):
    """
    Resources required by the job
    """
    cpus: int = Field(..., title="cpus", description="Number of CPU cores", ge=1)
    memory: int = Field(..., title="memory", description="Memory in GB", ge=1)
    disk: int = Field(..., title="disk", description="Disk in GB", ge=1)
    nodes: int = Field(..., title="nodes", description="Number of nodes", ge=1)
    walltime: int = Field(..., title="walltime", description="Wall time in mins", ge=1)


class TaskExecution(BaseModel):
    """
    Details about the execution of a task
    """
    exitCode: int = Field(..., title="exitCode", description="Exit code")
    retries: int = Field(..., title="retries", description="Number of retries")
    imagePullStatus: Optional[ImagePullStatus] = Field(
        ..., title="imagePullStatus", description="Image pull status")
    imagePullTime: Optional[float] = Field(
        ..., title="imagePullTime", description="Image pull time"
    )
    wallTimeUsage: float = Field(
        ..., title="wallTimeUsage", description="Wall time usage in secs"
    )
    cpuTimeUsage: float = Field(
        ..., title="cpuTimeUsage", description="CPU time usage in secs"
    )
    maxResidentSetSizeKB: int = Field(
        ..., title="maxResidentSetSizeKB", description="Maximum resident set size in KB"
    )


class Events(BaseModel):
    """
    Times at which the job/workflow was submitted, started running and ended
    """
    createTime: int = Field(..., title="createTime", description="Creation time")
    startTime: Optional[int] = Field(None, title="startTime", description="Start time")
    endTime: Optional[int] = Field(None, title="endTime", description="End time")


class CpuDetails(BaseModel):
    """
    Details of CPU used
    """
    clock: str = Field(..., title="clock", description="Clock speed in MHz")
    model: str = Field(..., title="model", description="Model")
    vendor: str = Field(..., title="vendor", description="Vendor")


class Execution(BaseModel):
    """
    Details about the execution of a job
    """
    site: Optional[str] = Field(
        None, title="site", description="Site where the job ran"
    )
    cpu: Optional[CpuDetails] = Field(None, title="cpu", description="CPU details")
    retries: Optional[int] = Field(0, title="retries", description="Retries")
    # maxMemoryUsageKB: Optional[int] = Field(..., title='maxMemoryUsageKB', description='Maximum memory usage of the job in KB')
    tasks: Optional[List[TaskExecution]] = Field(
        None, title="tasks", description="Task execution"
    )


class ContainerRuntime(str, Enum):
    """
    Container runtime
    """
    singularity = "singularity"
    udocker = "udocker"


class Task(BaseModel):
    """
    Task description
    """
    image: str = Field(..., title="image", description="Container image")
    runtime: ContainerRuntime = Field(
        ..., title="runtime", description="Container runtime"
    )
    cmd: Optional[str] = Field(None, title="cmd", description="Command")
    workdir: Optional[str] = Field(None, title="workdir", description="Work directory")
    env: Optional[Dict[str, str]] = Field(
        None, title="env", description="Environment variables"
    )
    procsPerNode: Optional[int] = Field(
        None, title="procsPerNode", description="Processes per node"
    )


class Job(BaseModel):
    """
    Base job description
    """
    name: Optional[str] = Field(
        None,
        title="name",
        description="Job name",
        max_length=512,
        regex=r"^[a-zA-Z0-9\-\_\s\/\.]+$",
    )
    tasks: List[Task] = Field(..., title="tasks", description="List of tasks")
    resources: Resources = Field(..., title="resources", description="Resources")
    artifacts: Optional[List[Artifact]] = Field(
        None, title="artifacts", description="Input artifacts"
    )
    policies: Optional[JobPolicies] = Field(
        None, title='policies', description='Job policies'
    )


class JobOutput(Job):
    """
    Job description after submission
    """
    id: str = Field(..., title="id", description="Job id")
    status: JobStatus = Field(..., title="status", description="Job status")
    events: Events = Field(..., title="events", description="Job events")
    execution: Optional[Execution] = Field(
        ..., title="execution", description="Execution details"
    )
