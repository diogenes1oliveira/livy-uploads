import functools
import io
from typing import TYPE_CHECKING, Any, Mapping

if TYPE_CHECKING:
    from jinja2 import Environment
else:
    Environment = Any


class JinjaRenderer:
    @functools.cached_property
    def jinja_env(self) -> Environment:
        from jinja2 import Environment, StrictUndefined

        return Environment(undefined=StrictUndefined)

    def render(self, template_content: str, env: Mapping[str, str]) -> io.StringIO:
        template = self.jinja_env.from_string(template_content)
        result = template.render(env=env)
        return io.StringIO(result)
