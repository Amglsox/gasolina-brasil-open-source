from apiflask import APIFlask


def create_app() -> APIFlask:
    """Construct the core application."""
    app = APIFlask(__name__)

    return app
