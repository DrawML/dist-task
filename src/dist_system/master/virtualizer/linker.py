from jinja2 import Template

from dist_system.master.virtualizer.config import RunConfig

"""
I think there is no need of device parameter.
That information is in config.
"""


def link(obj_code: str, run_config: RunConfig):
    template = Template(obj_code)
    return template.render(**dict(run_config))
