import os

# Unit tests must not depend on a developer's .env or running services.
os.environ.setdefault("FRAUDX_ENV", "test")
os.environ.setdefault("FRAUDX_LOG_FORMAT", "text")
os.environ.setdefault("MONGO_APP_PASSWORD", "test")
os.environ.setdefault("REDIS_PASSWORD", "test")
os.environ.setdefault("NEO4J_PASSWORD", "test")
