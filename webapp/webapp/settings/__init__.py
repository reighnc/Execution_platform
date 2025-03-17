import os

from .base import *

env = os.environ.get("ALGO_ENV")
print(env)

if env == "prod":
    from .prod import *

else:
    from .dev import *
