import attr

@attr.s
class BaseJob:
    job_id: str = attr.ib(init=True, default=None)