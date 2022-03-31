from waitress import serve
import logging

from config import config
from app import app


def main():
    logger = logging.getLogger("waitress")
    logger.setLevel(logging.DEBUG)
    serve(app, host="0.0.0.0", port=config.port)


if __name__ == "__main__":
    main()
