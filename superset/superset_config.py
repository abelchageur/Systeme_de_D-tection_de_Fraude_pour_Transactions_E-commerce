# superset_config.py
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address

def init_app(app):
    print("init_app is called")
    limiter = Limiter(
        key_func=get_remote_address,
        app=app,
        storage_uri="redis://redis:6379/0",
        default_limits=["100 per minute"],
    )
