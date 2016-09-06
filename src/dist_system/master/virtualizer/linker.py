from jinja2 import Template
from .config import RunConfig


def link(obj_code: str, config: dict, device="/cpu:0"):
    compile_config = RunConfig(**config)
    template = Template(obj_code)
    return template.render(device=device, **dict(compile_config))
