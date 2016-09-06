from jinja2 import Template
from .config import RunConfig


"""
I think there is no need of device parameter.
That information is in config.
"""
def link(obj_code: str, run_config: RunConfig):
    compile_config = run_config
    template = Template(obj_code)
    return template.render(device=run_config.tf_device, **dict(compile_config))
