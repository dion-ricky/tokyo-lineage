import attr

@attr.s
class BaseTask:
    task_id: str = attr.ib(init=True, default=None)