import attr

from openlineage.client.facet import BaseFacet


@attr.s
class Annotation(BaseFacet):
    annotation: dict = attr.ib(init=False)