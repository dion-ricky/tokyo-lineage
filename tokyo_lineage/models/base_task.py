import attr

@attr.s
class BaseTask:
    task_id: str = attr.ib(init=False, default=None)