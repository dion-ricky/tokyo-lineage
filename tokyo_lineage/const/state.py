import enum

class TaskState(enum.Enum):
    UNKNOWN = "unknown"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"