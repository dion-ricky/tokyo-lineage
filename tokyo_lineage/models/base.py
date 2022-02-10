import attr

from tokyo_lineage.const.state import TaskState

@attr.s
class BaseTask:
    task_id: str = attr.ib(init=True, default=None)
    operator: str = attr.ib(init=True, default=None)
    state: TaskState = attr.ib(init=False, default=None, \
                                validator=attr.validators.in_(TaskState))


@attr.s
class BaseJob:
    job_id: str = attr.ib(init=False, default=None)