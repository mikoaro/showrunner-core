import asyncio
import logging
import json
import uuid
import time
from typing import List, Dict, Optional, Literal
from enum import Enum
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field, HttpUrl
import redis.asyncio as redis

# --- 1. Configuration & Observability ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("showrunner.core")

# Paved Path Configuration
REDIS_URL = "redis://localhost:6379"  # Use 'redis' if in Docker network

# --- 2. Domain Models (The Contract) ---
class AssetTheme(str, Enum):
    ACTION = "action"
    THRILLER = "thriller"
    DRAMA = "drama"

class TaskStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"

class WorkflowStep(BaseModel):
    name: str
    status: TaskStatus = TaskStatus.PENDING
    logs: List[str] = []
    output_key: Optional[str] = None

class JobState(BaseModel):
    job_id: str
    input_url: str
    theme: AssetTheme
    current_stage: str = "ingest"
    progress: int = 0  # 0 to 100
    # The DAG State
    steps: Dict[str, WorkflowStep] = {
        "ingest": WorkflowStep(name="Ingest & Shard"),
        "vision": WorkflowStep(name="Vision Analysis (ResNet)"),
        "audio": WorkflowStep(name="Audio Transcription (Whisper)"),
        "synthesis": WorkflowStep(name="Agentic Edit"),
        "distribution": WorkflowStep(name="Final Render")
    }
    artifacts: Dict[str, str] = {}

class JobRequest(BaseModel):
    video_url: HttpUrl
    theme: AssetTheme

# --- 3. The Orchestration Plane (Infrastructure Layer) ---
class StateManager:
    """
    Abstration over Redis to ensure persistence and observability.
    This replaces brittle in-memory dicts.
    """
    def __init__(self):
        # Decode responses for easier JSON handling
        self.redis = redis.from_url(REDIS_URL, decode_responses=True)

    async def save_job(self, job: JobState):
        await self.redis.set(f"job:{job.job_id}", job.model_dump_json())

    async def get_job(self, job_id: str) -> Optional[JobState]:
        data = await self.redis.get(f"job:{job_id}")
        return JobState.model_validate_json(data) if data else None

    async def emit_log(self, job_id: str, step: str, message: str):
        """Atomic log appending for real-time frontend streaming"""
        job = await self.get_job(job_id)
        if job and step in job.steps:
            timestamp = time.strftime("%H:%M:%S")
            job.steps[step].logs.append(f"[{timestamp}] {message}")
            await self.save_job(job)

# --- 4. The Worker Plane (Compute Layer) ---
# In production, these run as separate Titus Containers consuming from Kafka.
# Here, we model them as async coroutines to demonstrate the ARCHITECTURE.

class MediaWorkers:
    def __init__(self, state_mgr: StateManager):
        self.state = state_mgr

    async def run_ingest(self, job: JobState):
        step = "ingest"
        await self.state.emit_log(job.job_id, step, "⬇️ Downloading asset from S3 Source...")
        await asyncio.sleep(1.5) # Emulate I/O
        await self.state.emit_log(job.job_id, step, "🔍 Validating codec: ProRes 422 HQ")
        await asyncio.sleep(0.5)
        await self.state.emit_log(job.job_id, step, "✅ Sharding complete: 42 segments created.")

    async def run_vision(self, job: JobState):
        step = "vision"
        await self.state.emit_log(job.job_id, step, "👁️ Loading ResNet-50 weights...")
        await asyncio.sleep(1.0)
        await self.state.emit_log(job.job_id, step, "🏎️ Processing batch 1/42 on GPU-01")
        await asyncio.sleep(2.0) # Emulate GPU compute
        await self.state.emit_log(job.job_id, step, "📊 Vectors generated: 2048 dims")

    async def run_audio(self, job: JobState):
        step = "audio"
        await self.state.emit_log(job.job_id, step, "🎤 Initializing Whisper v3 Large...")
        await asyncio.sleep(1.0)
        await self.state.emit_log(job.job_id, step, "📝 Transcribing dialogue...")
        await asyncio.sleep(1.5)
        await self.state.emit_log(job.job_id, step, "🔍 Detected Keywords: 'Explosion', 'Run', 'Save me'")

    async def run_agentic_synthesis(self, job: JobState):
        step = "synthesis"
        await self.state.emit_log(job.job_id, step, f"🤖 Agent 'Showrunner' activated. Theme: {job.theme}")
        await asyncio.sleep(1.0)
        await self.state.emit_log(job.job_id, step, "📐 Querying Vector DB for high-valence shots...")
        await asyncio.sleep(1.0)
        await self.state.emit_log(job.job_id, step, "🎬 EDL (Edit Decision List) generated.")

# --- 5. The Conductor (Logic Layer) ---
class MediaOrchestrator:
    """
    The Micro-Orchestrator that manages the DAG dependencies.
    """
    def __init__(self):
        self.state_mgr = StateManager()
        self.workers = MediaWorkers(self.state_mgr)

    async def run_workflow(self, job_id: str):
        # 1. Ingest
        await self._update_step(job_id, "ingest", TaskStatus.RUNNING, 10)
        await self.workers.run_ingest(await self.state_mgr.get_job(job_id))
        await self._update_step(job_id, "ingest", TaskStatus.COMPLETED, 20)

        # 2. Fan-Out (Parallel Execution) - Key Architectural Pattern
        await self._update_step(job_id, "vision", TaskStatus.RUNNING, 25)
        await self._update_step(job_id, "audio", TaskStatus.RUNNING, 25)
        
        # Run Vision and Audio concurrently
        job = await self.state_mgr.get_job(job_id)
        await asyncio.gather(
            self.workers.run_vision(job),
            self.workers.run_audio(job)
        )
        
        await self._update_step(job_id, "vision", TaskStatus.COMPLETED, 60)
        await self._update_step(job_id, "audio", TaskStatus.COMPLETED, 60)

        # 3. Agentic Synthesis (Fan-In)
        await self._update_step(job_id, "synthesis", TaskStatus.RUNNING, 80)
        await self.workers.run_agentic_synthesis(await self.state_mgr.get_job(job_id))
        await self._update_step(job_id, "synthesis", TaskStatus.COMPLETED, 95)

        # 4. Finalize
        await self._update_step(job_id, "distribution", TaskStatus.COMPLETED, 100)
        
        # Update Artifacts
        job = await self.state_mgr.get_job(job_id)
        job.artifacts = {
            "trailer_url": f"https://cdn.netflix-internal.com/trailers/{job_id}.mp4",
            "metadata_json": f"s3://assets/{job_id}/meta.json"
        }
        await self.state_mgr.save_job(job)

    async def _update_step(self, job_id: str, step_key: str, status: TaskStatus, progress: int):
        job = await self.state_mgr.get_job(job_id)
        if job:
            job.steps[step_key].status = status
            job.progress = progress
            await self.state_mgr.save_job(job)

# --- 6. FastAPI Setup ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Verify Redis Connection on Startup
    try:
        r = redis.from_url(REDIS_URL)
        await r.ping()
        logger.info("✅ Connected to Redis (State Store)")
    except Exception as e:
        logger.error(f"❌ Redis Connection Failed: {e}")
    yield

app = FastAPI(title="Project Showrunner API", lifespan=lifespan)
orchestrator = MediaOrchestrator()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], # Tighten for prod
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.post("/api/jobs", response_model=JobState)
async def submit_job(request: JobRequest, background_tasks: BackgroundTasks):
    job_id = str(uuid.uuid4())[:8]
    new_job = JobState(
        job_id=job_id,
        input_url=str(request.video_url),
        theme=request.theme
    )
    
    # Save Initial State
    await orchestrator.state_mgr.save_job(new_job)
    
    # Trigger Orchestrator in Background (Simulating Async Worker Queue)
    background_tasks.add_task(orchestrator.run_workflow, job_id)
    
    return new_job

@app.get("/api/jobs/{job_id}", response_model=JobState)
async def get_job_status(job_id: str):
    job = await orchestrator.state_mgr.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return job
