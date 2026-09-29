import uuid
from datetime import datetime, timezone

def generate_run_id() -> str:
    """Return a sortable, unique identifier for a pipeline run."""
    run_id = f"{datetime.now(timezone.utc):%Y%m%d_%H%M%S}_{uuid.uuid4().hex[:8]}"
    return run_id