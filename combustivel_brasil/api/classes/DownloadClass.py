from apiflask import Schema
from apiflask.fields import String
from apiflask.validators import URL


class DownloadIn(Schema):
    remote_url = String(required=True, validate=URL())
    local_file = String(required=True)
