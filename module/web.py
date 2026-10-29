"""web ui for media download"""

import logging
import os
import threading

from flask import Flask, jsonify, render_template, request
from flask_login import LoginManager, UserMixin, login_required, login_user

import utils
from module.app import Application
from module.download_stat import (
    DownloadState,
    get_download_result,
    get_download_state,
    get_total_download_speed,
    set_download_state,
    delete_task,
    is_task_deleted,
    is_task_paused,
    pause_task,
    resume_task,
)
from utils.crypto import AesBase64
from utils.format import format_byte

log = logging.getLogger("werkzeug")
log.setLevel(logging.ERROR)

_flask_app = Flask(__name__)

_flask_app.secret_key = "tdl"
_login_manager = LoginManager()
_login_manager.login_view = "login"
_login_manager.init_app(_flask_app)
web_login_users: dict = {}
deAesCrypt = AesBase64("1234123412ABCDEF", "ABCDEF1234123412")


class User(UserMixin):
    """Web Login User"""

    def __init__(self):
        self.sid = "root"

    @property
    def id(self):
        """ID"""
        return self.sid


@_login_manager.user_loader
def load_user(_):
    """
    Load a user object from the user ID.

    Returns:
        User: The user object.
    """
    return User()


def get_flask_app() -> Flask:
    """get flask app instance"""
    return _flask_app


def run_web_server(app: Application):
    """
    Runs a web server using the Flask framework.
    """

    get_flask_app().run(
        app.web_host, app.web_port, debug=app.debug_web, use_reloader=False
    )


# pylint: disable = W0603
def init_web(app: Application):
    """
    Set the value of the users variable.

    Args:
        users: The list of users to set.

    Returns:
        None.
    """
    global web_login_users
    if app.web_login_secret:
        web_login_users = {"root": app.web_login_secret}
    else:
        _flask_app.config["LOGIN_DISABLED"] = True
    _flask_app.app_instance = app
    if app.debug_web:
        threading.Thread(target=run_web_server, args=(app,)).start()
    else:
        threading.Thread(
            target=get_flask_app().run, daemon=True, args=(app.web_host, app.web_port)
        ).start()


@_flask_app.route("/login", methods=["GET", "POST"])
def login():
    """
    Function to handle the login route.

    Parameters:
    - No parameters

    Returns:
    - If the request method is "POST" and the username and
      password match the ones in the web_login_users dictionary,
      it returns a JSON response with a code of "1".
    - Otherwise, it returns a JSON response with a code of "0".
    - If the request method is not "POST", it returns the rendered "login.html" template.
    """
    if request.method == "POST":
        username = "root"
        web_login_form = {}
        for key, value in request.form.items():
            if value:
                value = deAesCrypt.decrypt(value)
            web_login_form[key] = value

        if not web_login_form.get("password"):
            return jsonify({"code": "0"})

        password = web_login_form["password"]
        if username in web_login_users and web_login_users[username] == password:
            user = User()
            login_user(user)
            return jsonify({"code": "1"})

        return jsonify({"code": "0"})

    return render_template("login.html")


@_flask_app.route("/")
@login_required
def index():
    """Index html"""
    return render_template(
        "index.html",
        download_state=(
            "pause" if get_download_state() is DownloadState.Downloading else "continue"
        ),
    )


@_flask_app.route("/get_download_status")
@login_required
def get_download_speed():
    """Get download speed"""
    return (
        '{ "download_speed" : "'
        + format_byte(get_total_download_speed())
        + '/s" , "upload_speed" : "0.00 B/s" } '
    )


@_flask_app.route("/set_download_state", methods=["POST"])
@login_required
def web_set_download_state():
    """Set download state"""
    state = request.args.get("state")

    if state == "continue" and get_download_state() is DownloadState.StopDownload:
        set_download_state(DownloadState.Downloading)
        return "pause"

    if state == "pause" and get_download_state() is DownloadState.Downloading:
        set_download_state(DownloadState.StopDownload)
        return "continue"

    return state


# ===================== 单任务控制 API =====================

@_flask_app.route("/pause_task", methods=["POST"])
@login_required
def web_pause_task():
    """Pause a single download task"""
    chat_id = request.args.get("chat")
    message_id = request.args.get("id")
    if chat_id is None or message_id is None:
        return jsonify({"code": "0", "msg": "missing chat or id"})
    try:
        pause_task(int(chat_id), int(message_id))
    except Exception:
        pause_task(chat_id, message_id)
    return jsonify({"code": "1", "msg": "paused"})


@_flask_app.route("/resume_task", methods=["POST"])
@login_required
def web_resume_task():
    """Resume a single download task"""
    chat_id = request.args.get("chat")
    message_id = request.args.get("id")
    if chat_id is None or message_id is None:
        return jsonify({"code": "0", "msg": "missing chat or id"})
    try:
        resume_task(int(chat_id), int(message_id))
    except Exception:
        resume_task(chat_id, message_id)
    return jsonify({"code": "1", "msg": "resumed"})


@_flask_app.route("/delete_task", methods=["POST"])
@login_required
def web_delete_task():
    """Delete a single download task"""
    chat_id = request.args.get("chat")
    message_id = request.args.get("id")
    if chat_id is None or message_id is None:
        return jsonify({"code": "0", "msg": "missing chat or id"})
    try:
        delete_task(int(chat_id), int(message_id))
    except Exception:
        delete_task(chat_id, message_id)
    return jsonify({"code": "1", "msg": "deleted"})


# ===================== 机器人 token 配置 API =====================

@_flask_app.route("/get_bot_config")
@login_required
def get_bot_config():
    """Get current bot token config"""
    _app = getattr(_flask_app, "app_instance", None)
    if _app is None:
        return jsonify({"code": "0", "bot_token": "", "enabled": False})
    token = _app.bot_token
    return jsonify(
        {"code": "1", "bot_token": token, "enabled": bool(token)}
    )


@_flask_app.route("/set_bot_token", methods=["POST"])
@login_required
def set_bot_token():
    """Set/Update the single bot token (takes effect after container restart)"""
    _app = getattr(_flask_app, "app_instance", None)
    if _app is None:
        return jsonify({"code": "0", "msg": "app not ready"})
    data = request.get_json(silent=True) or {}
    token = (data.get("bot_token") or "").strip()
    if not token:
        return jsonify({"code": "0", "msg": "bot_token is empty"})
    _app.set_bot_token(token)
    return jsonify({"code": "1", "msg": "ok"})


@_flask_app.route("/set_chat_max_size", methods=["POST"])
@login_required
def set_chat_max_size():
    """Set per-chat max download file size (MB)"""
    _app = getattr(_flask_app, "app_instance", None)
    if _app is None:
        return jsonify({"code": "0", "msg": "app not ready"})
    chat_id = request.args.get("chat")
    size_mb = request.args.get("size_mb")
    if chat_id is None or size_mb is None:
        return jsonify({"code": "0", "msg": "missing chat or size_mb"})
    try:
        size_mb_int = int(size_mb)
        if size_mb_int < 0:
            raise ValueError
        chat_id_int = int(chat_id)
    except Exception:
        return jsonify({"code": "0", "msg": "invalid chat or size_mb"})
    ok = _app.set_chat_max_file_size(chat_id_int, size_mb_int * 1024 * 1024)
    if not ok:
        return jsonify({"code": "0", "msg": "chat not found in config"})
    return jsonify({"code": "1", "msg": "ok"})


@_flask_app.route("/get_app_version")
def get_app_version():
    """Get telegram_media_downloader version"""
    return utils.__version__


@_flask_app.route("/get_download_list")
@login_required
def get_download_list():
    """get download list"""
    if request.args.get("already_down") is None:
        return "[]"

    already_down = request.args.get("already_down") == "true"

    download_result = get_download_result()
    result = "["
    for chat_id, messages in download_result.items():
        for idx, value in messages.items():
            is_already_down = value["down_byte"] == value["total_size"]

            if already_down and not is_already_down:
                continue

            if result != "[":
                result += ","
            task_state = "deleted" if is_task_deleted(chat_id, idx) else (
                "paused" if is_task_paused(chat_id, idx) else "running"
            )
            # 暂停中的任务速度显示"已暂停",避免旧速度误导
            if task_state == "paused":
                download_speed = "已暂停"
            else:
                download_speed = format_byte(value["download_speed"]) + "/s"
            result += (
                '{ "chat":"'
                + f"{chat_id}"
                + '", "id":"'
                + f"{idx}"
                + '", "filename":"'
                + os.path.basename(value["file_name"])
                + '", "total_size":"'
                + f'{format_byte(value["total_size"])}'
                + '" ,"download_progress":"'
            )
            result += (
                f'{round(value["down_byte"] / value["total_size"] * 100, 1)}'
                + '" ,"download_speed":"'
                + download_speed
                + '" ,"save_path":"'
                + value["file_name"].replace("\\", "/")
                + '" ,"task_state":"'
                + task_state
                + '"}'
            )

    result += "]"
    return result


@_flask_app.route("/get_config")
@login_required
def get_config():
    """Return current running config summary for the web UI"""
    _app = getattr(_flask_app, "app_instance", None)
    if _app is None:
        return jsonify({"code": "0", "msg": "app not ready"})
    return jsonify({"code": "1", "config": _app.get_config_summary()})


@_flask_app.route("/save_config", methods=["POST"])
@login_required
def save_config():
    """Save a full config submitted from the web UI (takes effect after restart)"""
    _app = getattr(_flask_app, "app_instance", None)
    if _app is None:
        return jsonify({"code": "0", "msg": "app not ready"})
    data = request.get_json(silent=True) or {}
    if not data.get("config"):
        return jsonify({"code": "0", "msg": "empty config"})
    res = _app.apply_web_config(data["config"])
    if not res["ok"]:
        return jsonify({"code": "0", "msg": "; ".join(res["errors"])})
    return jsonify({"code": "1", "msg": "saved"})


@_flask_app.route("/restart_app", methods=["POST"])
@login_required
def restart_app():
    """Save config (optional) and trigger container restart to apply it"""
    _app = getattr(_flask_app, "app_instance", None)
    if _app is None:
        return jsonify({"code": "0", "msg": "app not ready"})
    data = request.get_json(silent=True) or {}
    if data.get("config"):
        res = _app.apply_web_config(data["config"])
        if not res["ok"]:
            return jsonify({"code": "0", "msg": "; ".join(res["errors"])})
    _app.request_restart()
    return jsonify({"code": "1", "msg": "restarting"})


