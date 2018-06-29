from jinja2 import Template

from common  import *

template = Template('Hello {{ name }}!')

print template.render(name='John Doe')
