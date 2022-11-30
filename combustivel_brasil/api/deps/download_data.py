import socket
import urllib.error
import urllib.request


def get_dados(remote_url: str, local_file: str) -> tuple(str, bool):
    socket.setdefaulttimeout(1000)
    try:
        urllib.request.urlretrieve(url=remote_url, filename=local_file)
        return "OK", True
    except urllib.error.ContentTooShortError:
        get_dados(remote_url, local_file)
    except OSError as ex:
        return ex.args[1], False
    except Exception as ex:
        return ex.args[1], False
