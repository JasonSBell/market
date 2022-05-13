import os
from pprint import pprint
import dotenv

from types import SimpleNamespace

dotenv.load_dotenv()

config = SimpleNamespace(
    port=int(os.environ.get("PORT", "8090")),
    jwt_secret=os.environ.get("JWT_SECRET", "secret"),
    fluentd=SimpleNamespace(
        host=os.environ.get("FLUENTD_HOST", "localhost"),
        port=int(os.environ.get("FLUENTD_PORT", "24224")),
    ),
    postgres=SimpleNamespace(
        host=os.environ.get("POSTGRES_HOST", "localhost"),
        port=int(os.environ.get("POSTGRES_PORT", "5432")),
        user=os.environ.get("POSTGRES_USER", "root"),
        password=os.environ.get("POSTGRES_PASSWORD", "example"),
        database=os.environ.get("POSTGRES_DATABASE", "allokate"),
    ),
    mongo=SimpleNamespace(
        host=os.environ.get("MONGO_HOST", "localhost"),
        port=int(os.environ.get("MONGO_PORT", "27017")),
        user=os.environ.get("MONGO_USER", "root"),
        password=os.environ.get("MONGO_PASSWORD", "password"),
        db=os.environ.get("MONGO_DATABASE", "allokate"),
    ),
)

if __name__ == "__main__":
    pprint(config)
