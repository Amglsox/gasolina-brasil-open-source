import asyncio
import os

from classes.DownloadClass import DownloadIn
from deps.download_data import get_dados
from flask import jsonify
from flask import make_response
from flask import wrappers

from __init__ import create_app


app = create_app()


@app.route("/", methods=["GET", "POST"])
async def index() -> wrappers.Response:
    await asyncio.sleep(1)
    return make_response(jsonify({"message": "OK"}), 200)


@app.route("/download_combustivel", methods=["POST"])
@app.input(DownloadIn)
async def download_combustivel(request: dict) -> wrappers.Response:
    remote_url = request.get("remote_url")
    local_file = request.get("local_file")
    msg, _ = get_dados(remote_url=remote_url, local_file=local_file)
    if _:
        return make_response(jsonify({"message": msg}), 200)
    else:
        return make_response(jsonify({"message": msg}), 500)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 1010)))
