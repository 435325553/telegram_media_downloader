"""Application module"""

import asyncio
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Callable, List, Optional, Union

from loguru import logger
from ruamel import yaml

from module.cloud_drive import CloudDrive, CloudDriveConfig
from module.filter import Filter
from module.language import Language, set_language
from module.speed_limit import limiter
from utils.format import replace_date_time, validate_title
from utils.meta_data import MetaData

_yaml = yaml.YAML()
# pylint: disable = R0902


class DownloadStatus(Enum):
    """Download status"""

    SkipDownload = 1
    SuccessDownload = 2
    FailedDownload = 3
    Downloading = 4


class ForwardStatus(Enum):
    """Forward status"""

    SkipForward = 1
    SuccessForward = 2
    FailedForward = 3
    Forwarding = 4
    StopForward = 5
    CacheForward = 6


class UploadStatus(Enum):
    """Upload status"""

    SkipUpload = 1
    SuccessUpload = 2
    FailedUpload = 3
    Uploading = 4


class TaskType(Enum):
    """Task Type"""

    Download = 1
    Forward = 2
    ListenForward = 3


class QueryHandler(Enum):
    """Query handler"""

    StopDownload = 1
    StopForward = 2
    StopListenForward = 3


@dataclass
class UploadProgressStat:
    """Upload task"""

    file_name: str
    total_size: int
    upload_size: int
    start_time: float
    last_stat_time: float
    upload_speed: float


@dataclass
class CloudDriveUploadStat:
    """Cloud drive upload task"""

    file_name: str
    transferred: str
    total: str
    percentage: str
    speed: str
    eta: str


class QueryHandlerStr:
    """Query handler"""

    _strMap = {
        QueryHandler.StopDownload.value: "stop_download",
        QueryHandler.StopForward.value: "stop_forward",
        QueryHandler.StopListenForward.value: "stop_listen_forward",
    }

    @staticmethod
    def get_str(value):
        """
        Get the string value associated with the given value.

        Parameters:
            value (any): The value for which to retrieve the string value.

        Returns:
            str: The string value associated with the given value.
        """
        return QueryHandlerStr._strMap[value]


class TaskNode:
    """Task node"""

    # pylint: disable = R0913
    def __init__(
        self,
        chat_id: Union[int, str],
        from_user_id: Union[int, str] = None,
        reply_message_id: int = 0,
        replay_message: str = None,
        upload_telegram_chat_id: Union[int, str] = None,
        has_protected_content: bool = False,
        download_filter: str = None,
        limit: int = 0,
        start_offset_id: int = 0,
        end_offset_id: int = 0,
        bot=None,
        task_type: TaskType = TaskType.Download,
        task_id: int = 0,
        topic_id: int = 0,
    ):
        self.chat_id = chat_id
        self.from_user_id = from_user_id
        self.upload_telegram_chat_id = upload_telegram_chat_id
        self.reply_message_id = reply_message_id
        self.reply_message = replay_message
        self.has_protected_content = has_protected_content
        self.download_filter = download_filter
        self.limit = limit
        self.start_offset_id = start_offset_id
        self.end_offset_id = end_offset_id
        self.bot = bot
        self.task_id = task_id
        self.task_type = task_type
        self.total_task = 0
        self.total_download_task = 0
        self.failed_download_task = 0
        self.success_download_task = 0
        self.skip_download_task = 0
        self.last_reply_time = time.time()
        self.last_edit_msg: str = ""
        self.total_download_byte = 0
        self.forward_msg_detail_str: str = ""
        self.upload_user = None
        self.total_forward_task: int = 0
        self.success_forward_task: int = 0
        self.failed_forward_task: int = 0
        self.skip_forward_task: int = 0
        self.is_running: bool = False
        self.client = None
        self.upload_success_count: int = 0
        self.is_stop_transmission = False
        self.media_group_ids: dict = {}
        self.download_status: dict = {}
        self.upload_status: dict = {}
        self.upload_stat_dict: dict = {}
        self.topic_id = topic_id
        self.reply_to_message = None
        self.cloud_drive_upload_stat_dict: dict = {}

    def skip_msg_id(self, msg_id: int):
        """Skip if message id out of range"""
        if self.start_offset_id and msg_id < self.start_offset_id:
            return True

        if self.end_offset_id and msg_id > self.end_offset_id:
            return True

        return False

    def is_finish(self):
        """If is finish"""
        return self.is_stop_transmission or (
            self.is_running
            and self.task_type != TaskType.ListenForward
            and self.total_task == self.total_download_task
        )

    def stop_transmission(self):
        """Stop task"""
        self.is_stop_transmission = True

    def stat(self, status: DownloadStatus):
        """
        Updates the download status of the task.

        Args:
            status (DownloadStatus): The status of the download task.

        Returns:
            None
        """
        self.total_download_task += 1
        if status is DownloadStatus.SuccessDownload:
            self.success_download_task += 1
        elif status is DownloadStatus.SkipDownload:
            self.skip_download_task += 1
        else:
            self.failed_download_task += 1

    def stat_forward(self, status: ForwardStatus, count: int = 1):
        """Stat upload"""
        self.total_forward_task += count
        if status is ForwardStatus.SuccessForward:
            self.success_forward_task += count
        elif status is ForwardStatus.SkipForward:
            self.skip_forward_task += count
        else:
            self.failed_forward_task += count

    def can_reply(self):
        """
        Checks if the bot can reply to a message
            based on the time elapsed since the last reply.

        Returns:
            True if the time elapsed since
                the last reply is greater than 1 second, False otherwise.
        """
        cur_time = time.time()
        if cur_time - self.last_reply_time > 1.0:
            self.last_reply_time = cur_time
            return True

        return False


class LimitCall:
    """Limit call"""

    def __init__(
        self,
        max_limit_call_times: int = 0,
        limit_call_times: int = 0,
        last_call_time: float = 0,
    ):
        """
        Initializes the object with the given parameters.

        Args:
            max_limit_call_times (int): The maximum limit of call times allowed.
            limit_call_times (int): The current limit of call times.
            last_call_time (int): The time of the last call.

        Returns:
            None
        """
        self.max_limit_call_times = max_limit_call_times
        self.limit_call_times = limit_call_times
        self.last_call_time = last_call_time

    async def wait(self, node: TaskNode):
        """
        Wait for a certain period of time before continuing execution.

        This function does not take any parameters.

        This function does not return anything.
        """
        while True:
            now = time.time()
            time_span = now - self.last_call_time
            if node.is_stop_transmission:
                break

            if time_span > 60:
                self.limit_call_times = 0
                self.last_call_time = now

            if self.limit_call_times + 1 <= self.max_limit_call_times:
                self.limit_call_times += 1
                break

            # logger.debug("Waiting for 10 seconds...")
            await asyncio.sleep(1)


class ChatDownloadConfig:
    """Chat Message Download Status"""

    def __init__(self):
        self.ids_to_retry_dict: dict = {}

        # need storage
        self.download_filter: str = None
        self.ids_to_retry: list = []
        self.last_read_message_id = 0
        # 已成功下载的消息id集合(按消息id去重, 持久化到 data.yaml)
        self.already_download_ids: set = set()
        # 失败重试次数统计(按消息id), 超过上限不再重试, 避免每次重启重复下载同一文件
        self.retry_counts: dict = {}
        self.total_task: int = 0
        self.finish_task: int = 0
        self.need_check: bool = False
        self.upload_telegram_chat_id: Union[int, str] = None
        self.node: TaskNode = TaskNode(0)
        # 每频道最大文件大小限制（字节），0 表示不限
        self.max_file_size: int = 0


def get_config(config, key, default=None, val_type=str, verbose=True):
    """
    Retrieves a configuration value from the given `config` dictionary
    based on the specified `key`.

    Args:
        config (dict): A dictionary containing the configuration values.
        key (str): The key of the configuration value to retrieve.
        default (Any, optional): The default value to be returned
            if the `key` is not found.
        val_type (type, optional): The data type of the configuration value.
        verbose (bool, optional): A flag indicating whether to print
            a warning message if the `key` is not found.

    Returns:
        The configuration value associated with the specified `key`,
         converted to the specified `type`. If the `key` is not found,
         the `default` value is returned.
    """
    val = config.get(key, default)
    if isinstance(val, val_type):
        return val

    if verbose:
        logger.warning(f"{key} is not {val_type.__name__}")

    return default


class Application:
    """Application load config and update config."""

    def __init__(
        self,
        config_file: str,
        app_data_file: str,
        application_name: str = "UndefineApp",
    ):
        """
        Init and update telegram media downloader config

        Parameters
        ----------
        config_file: str
            Config file name

        app_data_file: str
            App data file

        application_name: str
            Application Name

        """
        self.config_file: str = config_file
        self.app_data_file: str = app_data_file
        self.application_name: str = application_name
        self.download_filter = Filter()
        self.is_running = True

        self.total_download_task = 0

        self.chat_download_config: dict = {}

        self.save_path = os.path.join(os.path.abspath("."), "downloads")
        self.temp_save_path = os.path.join(os.path.abspath("."), "temp")
        self.api_id: str = ""
        self.api_hash: str = ""
        self.bot_token: str = ""
        self._chat_id: str = ""
        self.media_types: List[str] = []
        self.file_formats: dict = {}
        self.proxy: dict = {}
        self.restart_program = False
        self.config: dict = {}
        self.app_data: dict = {}
        self.file_path_prefix: List[str] = ["chat_title", "media_datetime"]
        self.file_name_prefix: List[str] = ["message_id", "file_name"]
        self.file_name_prefix_split: str = " - "
        self.log_file_path = os.path.join(os.path.abspath("."), "log")
        self.session_file_path = os.path.join(os.path.abspath("."), "sessions")
        self.cloud_drive_config = CloudDriveConfig()
        self.hide_file_name = False
        self.caption_name_dict: dict = {}
        self.caption_entities_dict: dict = {}
        self.max_concurrent_transmissions: int = 1
        self.web_host: str = "0.0.0.0"
        self.web_port: int = 5000
        self.max_download_task: int = 5
        self.language = Language.EN
        # 每频道并发下载数(受限于全局 max_download_task), 0=不限制
        self.max_download_task_per_chat: int = 1
        # 频道昵称缓存: chat_id(str) -> title, 启动时从 Telegram 获取
        self.chat_titles: dict = {}
        # 定时重启: 到点后程序退出, 依赖容器 restart: unless-stopped 自动拉起
        self.auto_restart_enabled: bool = False
        self.auto_restart_interval_hours: float = 6
        # 起始消息ID自动更新: 勾选(默认)后, 频道"起始消息ID"自动更新为最后下载的消息ID(断点续传);
        # 取消后保持手动填写值不变(已下载内容仍按消息id去重跳过, 不会重复下载)
        self.auto_update_last_read_id: bool = True
        # 状态落盘(断点/已下载id/重试列表): 下载过程中定期写回, 防止强杀或容器重启丢状态后重复下载
        self.state_dirty: bool = False
        self.state_save_interval: float = 5.0
        self._last_state_save_ts: float = 0.0
        # 同一消息失败重试上限, 0=不限制
        self.max_retry_times: int = 3
        # 单文件下载超时(分钟), 0=不限
        self.download_timeout: int = 0
        # 全局大小限制(对所有下载生效,含机器人命令下载的文件)
        self.global_max_size_enabled: bool = True
        self.global_max_size_mb: int = 500   # 默认 500MB
        self.global_max_size_bytes: int = 500 * 1024 * 1024
        self.after_upload_telegram_delete: bool = True
        self.web_login_secret: str = ""
        self.debug_web: bool = False
        self.log_level: str = "INFO"
        self.start_timeout: int = 60
        self.allowed_user_ids: yaml.comments.CommentedSeq = yaml.comments.CommentedSeq(
            []
        )
        self.date_format: str = "%Y_%m"
        self.drop_no_audio_video: bool = False
        self.enable_download_txt: bool = False

        self.forward_limit_call = LimitCall(max_limit_call_times=33)

        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)

        self.executor = ThreadPoolExecutor(
            min(32, (os.cpu_count() or 0) + 4), thread_name_prefix="multi_task"
        )

    # pylint: disable = R0915
    def assign_config(self, _config: dict) -> bool:
        """assign config from str.

        Parameters
        ----------
        _config: dict
            application config dict

        Returns
        -------
        bool
        """
        # pylint: disable = R0912
        # TODO: judge the storage if enough,and provide more path
        if _config.get("save_path") is not None:
            self.save_path = _config["save_path"]

        self.api_id = _config["api_id"]
        self.api_hash = _config["api_hash"]
        self.bot_token = _config.get("bot_token", "")

        self.media_types = _config["media_types"]
        self.file_formats = _config["file_formats"]

        self.hide_file_name = _config.get("hide_file_name", False)

        # option
        if _config.get("proxy"):
            self.proxy = _config["proxy"]
        if _config.get("restart_program"):
            self.restart_program = _config["restart_program"]
        if _config.get("file_path_prefix"):
            self.file_path_prefix = _config["file_path_prefix"]
        if _config.get("file_name_prefix"):
            self.file_name_prefix = _config["file_name_prefix"]

        if _config.get("upload_drive"):
            upload_drive_config = _config["upload_drive"]
            if upload_drive_config.get("enable_upload_file"):
                self.cloud_drive_config.enable_upload_file = upload_drive_config[
                    "enable_upload_file"
                ]

            if upload_drive_config.get("rclone_path"):
                self.cloud_drive_config.rclone_path = upload_drive_config["rclone_path"]

            if upload_drive_config.get("remote_dir"):
                self.cloud_drive_config.remote_dir = upload_drive_config["remote_dir"]

            if upload_drive_config.get("before_upload_file_zip"):
                self.cloud_drive_config.before_upload_file_zip = upload_drive_config[
                    "before_upload_file_zip"
                ]

            if upload_drive_config.get("after_upload_file_delete"):
                self.cloud_drive_config.after_upload_file_delete = upload_drive_config[
                    "after_upload_file_delete"
                ]

            if upload_drive_config.get("upload_adapter"):
                self.cloud_drive_config.upload_adapter = upload_drive_config[
                    "upload_adapter"
                ]

        self.file_name_prefix_split = _config.get(
            "file_name_prefix_split", self.file_name_prefix_split
        )
        self.web_host = _config.get("web_host", self.web_host)
        self.web_port = _config.get("web_port", self.web_port)

        # TODO: add check if expression exist syntax error

        self.max_download_task = _config.get(
            "max_download_task", self.max_download_task
        )
        try:
            _per_chat = int(_config.get("max_download_task_per_chat", 1) or 0)
        except (TypeError, ValueError):
            _per_chat = 1
        self.max_download_task_per_chat = max(_per_chat, 0)
        # 定时重启配置
        self.auto_restart_enabled = bool(
            _config.get("auto_restart_enabled", self.auto_restart_enabled)
        )
        try:
            _ar_hours = float(_config.get("auto_restart_interval_hours", 6) or 6)
        except (TypeError, ValueError):
            _ar_hours = 6
        self.auto_restart_interval_hours = max(_ar_hours, 0.01)

        # 全局下载限速(MB/s, 0=不限): 启动时恢复上次持久化的值
        try:
            _speed_limit = float(_config.get("download_speed_limit", 0) or 0)
        except (TypeError, ValueError):
            _speed_limit = 0.0
        limiter.configure(max(_speed_limit, 0.0))
        # 起始消息ID自动更新开关(默认开启)
        self.auto_update_last_read_id = bool(
            _config.get("auto_update_last_read_id", self.auto_update_last_read_id)
        )
        # 全局大小限制: max_size_enabled(开关) + max_size(MB), 缺省默认500MB
        _g_enabled = _config.get("max_size_enabled", self.global_max_size_enabled)
        try:
            _g_mb = int(_config.get("max_size", 500) or 0)
        except (TypeError, ValueError):
            _g_mb = 500
        if _g_mb < 0:
            _g_mb = 0
        self.global_max_size_enabled = bool(_g_enabled)
        self.global_max_size_mb = _g_mb
        self.global_max_size_bytes = (
            _g_mb * 1024 * 1024 if self.global_max_size_enabled else 0
        )

        # 单文件下载超时(分钟), 0=不限
        try:
            _t_out = int(_config.get("download_timeout", 0) or 0)
        except (TypeError, ValueError):
            _t_out = 0
        self.download_timeout = max(_t_out, 0)

        self.max_concurrent_transmissions = self.max_download_task * 5

        self.max_concurrent_transmissions = _config.get(
            "max_concurrent_transmissions", self.max_concurrent_transmissions
        )

        language = _config.get("language", "EN")

        try:
            self.language = Language[language.upper()]
        except KeyError:
            pass

        self.after_upload_telegram_delete = _config.get(
            "after_upload_telegram_delete", self.after_upload_telegram_delete
        )

        self.web_login_secret = str(
            _config.get("web_login_secret", self.web_login_secret)
        )
        self.debug_web = _config.get("debug_web", self.debug_web)
        self.log_level = _config.get("log_level", self.log_level)

        self.start_timeout = get_config(
            _config, "start_timeout", self.start_timeout, int
        )

        self.allowed_user_ids = get_config(
            _config,
            "allowed_user_ids",
            self.allowed_user_ids,
            yaml.comments.CommentedSeq,
        )

        self.date_format = get_config(
            _config,
            "date_format",
            self.date_format,
            str,
        )

        self.drop_no_audio_video = get_config(
            _config, "drop_no_audio_video", self.drop_no_audio_video, bool
        )

        self.enable_download_txt = get_config(
            _config, "enable_download_txt", self.enable_download_txt, bool
        )

        try:
            date = datetime(2023, 10, 31)
            date.strftime(self.date_format)
        except Exception as e:
            logger.warning(f"config date format error: {e}")
            self.date_format = "%Y_%m"

        forward_limit = _config.get("forward_limit", None)
        if forward_limit:
            try:
                forward_limit = int(forward_limit)
                self.forward_limit_call.max_limit_call_times = forward_limit
            except ValueError:
                pass

        if _config.get("chat"):
            chat = _config["chat"]
            for item in chat:
                if "chat_id" in item:
                    self.chat_download_config[item["chat_id"]] = ChatDownloadConfig()
                    self.chat_download_config[
                        item["chat_id"]
                    ].last_read_message_id = item.get("last_read_message_id", 0)
                    self.chat_download_config[
                        item["chat_id"]
                    ].download_filter = item.get("download_filter", "")
                    self.chat_download_config[
                        item["chat_id"]
                    ].upload_telegram_chat_id = item.get(
                        "upload_telegram_chat_id", None
                    )
                    self.chat_download_config[
                        item["chat_id"]
                    ].max_file_size = item.get("max_file_size", 0)
        elif _config.get("chat_id"):
            # Compatible with lower versions
            self._chat_id = _config["chat_id"]

            self.chat_download_config[self._chat_id] = ChatDownloadConfig()

            if _config.get("ids_to_retry"):
                self.chat_download_config[self._chat_id].ids_to_retry = _config[
                    "ids_to_retry"
                ]
                for it in self.chat_download_config[self._chat_id].ids_to_retry:
                    self.chat_download_config[self._chat_id].ids_to_retry_dict[
                        it
                    ] = True

            self.chat_download_config[self._chat_id].last_read_message_id = _config[
                "last_read_message_id"
            ]
            download_filter_dict = _config.get("download_filter", None)

            self.config["chat"] = [
                {
                    "chat_id": self._chat_id,
                    "last_read_message_id": self.chat_download_config[
                        self._chat_id
                    ].last_read_message_id,
                }
            ]

            if download_filter_dict and self._chat_id in download_filter_dict:
                self.chat_download_config[
                    self._chat_id
                ].download_filter = download_filter_dict[self._chat_id]
                self.config["chat"][0]["download_filter"] = download_filter_dict[
                    self._chat_id
                ]

        # pylint: disable = R1733
        for key, value in self.chat_download_config.items():
            self.chat_download_config[key].download_filter = replace_date_time(
                value.download_filter
            )

        return True

    def assign_app_data(self, app_data: dict) -> bool:
        """Assign config from str.

        Parameters
        ----------
        app_data: dict
            application data dict

        Returns
        -------
        bool
        """
        if app_data.get("ids_to_retry"):
            if self._chat_id:
                self.chat_download_config[self._chat_id].ids_to_retry = app_data[
                    "ids_to_retry"
                ]
                for it in self.chat_download_config[self._chat_id].ids_to_retry:
                    self.chat_download_config[self._chat_id].ids_to_retry_dict[
                        it
                    ] = True
                self.app_data.pop("ids_to_retry")
        else:
            if app_data.get("chat"):
                chats = app_data["chat"]
                for chat in chats:
                    if (
                        "chat_id" in chat
                        and chat["chat_id"] in self.chat_download_config
                    ):
                        chat_id = chat["chat_id"]
                        self.chat_download_config[chat_id].ids_to_retry = chat.get(
                            "ids_to_retry", []
                        )
                        for it in self.chat_download_config[chat_id].ids_to_retry:
                            self.chat_download_config[chat_id].ids_to_retry_dict[
                                it
                            ] = True
                        # 加载已下载消息id集合(按消息id去重)
                        _ids = chat.get("already_download_ids", []) or []
                        if isinstance(_ids, list):
                            self.chat_download_config[chat_id].already_download_ids = {
                                int(i)
                                for i in _ids
                                if str(i).lstrip("-").isdigit()
                            }
                        # 断点以 data.yaml 的较大值为准(config.yaml 可能因强杀没来得及更新)
                        # 仅在"起始消息ID自动更新"开启时生效; 关闭时始终用手填的起始ID
                        _lr = chat.get("last_read_message_id", 0) or 0
                        if self.auto_update_last_read_id:
                            try:
                                self.chat_download_config[
                                    chat_id
                                ].last_read_message_id = max(
                                    self.chat_download_config[chat_id].last_read_message_id,
                                    int(_lr),
                                )
                            except (TypeError, ValueError):
                                pass
                        # 恢复失败重试计数(用于重试上限判定)
                        _rc = chat.get("retry_counts", {}) or {}
                        if isinstance(_rc, dict):
                            self.chat_download_config[chat_id].retry_counts = {
                                int(k): int(v)
                                for k, v in _rc.items()
                                if str(k).lstrip("-").isdigit()
                                and str(v).lstrip("-").isdigit()
                            }
        return True

    async def upload_file(
        self,
        local_file_path: str,
        progress_callback: Callable = None,
        progress_args: tuple = (),
    ) -> bool:
        """Upload file"""

        if not self.cloud_drive_config.enable_upload_file:
            return False

        ret: bool = False
        if self.cloud_drive_config.upload_adapter == "rclone":
            ret = await CloudDrive.rclone_upload_file(
                self.cloud_drive_config,
                self.save_path,
                local_file_path,
                progress_callback,
                progress_args,
            )
        elif self.cloud_drive_config.upload_adapter == "aligo":
            ret = await self.loop.run_in_executor(
                self.executor,
                CloudDrive.aligo_upload_file(
                    self.cloud_drive_config, self.save_path, local_file_path
                ),
            )

        return ret

    def get_file_save_path(
        self, media_type: str, chat_title: str, media_datetime: str
    ) -> str:
        """Get file save path prefix.

        Parameters
        ----------
        media_type: str
            see config.yaml media_types

        chat_title: str
            see channel or group title

        media_datetime: str
            media datetime

        Returns
        -------
        str
            file save path prefix
        """

        res: str = self.save_path
        for prefix in self.file_path_prefix:
            if prefix == "chat_title":
                res = os.path.join(res, chat_title)
            elif prefix == "media_datetime":
                res = os.path.join(res, media_datetime)
            elif prefix == "media_type":
                res = os.path.join(res, media_type)
        return res

    def get_file_name(
        self, message_id: int, file_name: Optional[str], caption: Optional[str]
    ) -> str:
        """Get file save path prefix.

        Parameters
        ----------
        message_id: int
            Message id

        file_name: Optional[str]
            File name

        caption: Optional[str]
            Message caption

        Returns
        -------
        str
            File name
        """

        res: str = ""
        for prefix in self.file_name_prefix:
            if prefix == "message_id":
                if res != "":
                    res += self.file_name_prefix_split
                res += f"{message_id}"
            elif prefix == "file_name" and file_name:
                if res != "":
                    res += self.file_name_prefix_split
                res += f"{file_name}"
            elif prefix == "caption" and caption:
                if res != "":
                    res += self.file_name_prefix_split
                res += f"{caption}"
        if res == "":
            res = f"{message_id}"

        return validate_title(res)

    def need_skip_message(
        self, download_config: ChatDownloadConfig, message_id: int
    ) -> bool:
        """if need skip download message.

        Parameters
        ----------
        chat_id: str
            Config.yaml defined

        message_id: int
            Readily to download message id
        Returns
        -------
        bool
        """
        if message_id in download_config.ids_to_retry_dict:
            return True

        return False

    def exec_filter(self, download_config: ChatDownloadConfig, meta_data: MetaData):
        """
        Executes the filter on the given download configuration.

        Args:
            download_config (ChatDownloadConfig): The download configuration object.
            meta_data (MetaData): The meta data object.

        Returns:
            bool: The result of executing the filter.
        """
        if download_config.download_filter:
            self.download_filter.set_meta_data(meta_data)
            return self.download_filter.exec(download_config.download_filter)

        return True

    def _atomic_dump(self, path: str, data):
        """写 yaml: 先写临时文件再 os.replace, 避免进程被强杀时把配置写坏。

        注意: config.yaml/data.yaml 在 docker 里是"单文件 bind mount",
        对挂载点本身做 rename 会报 EBUSY(Resource busy), 这时退回原地写。
        """
        tmp_path = path + ".tmp"
        with open(tmp_path, "w", encoding="utf-8") as yaml_file:
            _yaml.dump(data, yaml_file)
        try:
            os.replace(tmp_path, path)
        except OSError:
            with open(path, "w", encoding="utf-8") as yaml_file:
                _yaml.dump(data, yaml_file)
            try:
                os.remove(tmp_path)
            except OSError:
                pass

    def flush_state(self, force: bool = False) -> bool:
        """把运行状态(断点/已下载消息id/重试列表)写回 config.yaml 与 data.yaml。

        下载过程中频繁调用, 用 state_save_interval 节流;
        force=True 时无条件写(退出/重启前调用, 防止丢断点导致重复下载)。
        """
        if not force:
            if not self.state_dirty:
                return False
            if time.time() - self._last_state_save_ts < self.state_save_interval:
                return False
        try:
            self.update_config(immediate=True)
            self.state_dirty = False
            self._last_state_save_ts = time.time()
            return True
        except Exception as e:  # pylint: disable = W0718
            logger.exception(f"flush_state error: {e}")
            self._last_state_save_ts = time.time()
            return False

    # pylint: disable = R0912
    def update_config(self, immediate: bool = True):
        """update config

        Parameters
        ----------
        immediate: bool
            If update config immediate,default True
        """
        if not isinstance(self.app_data.get("chat"), list):
            self.app_data["chat"] = []
        app_chat = self.app_data["chat"]
        cfg_chats = self.config.get("chat")
        if not isinstance(cfg_chats, list):
            cfg_chats = None
        # 按 chat_id 对齐 config/app_data, 不再依赖列表下标
        # (频道顺序变化时按下标写会把断点/去重集合写到别的频道, 造成重复下载或误跳过)
        # pylint: disable = R1733
        for key, value in self.chat_download_config.items():
            # pylint: disable = W0201
            unfinished_ids = set(value.ids_to_retry)

            for it in value.ids_to_retry:
                if value.node.download_status.get(
                    it, DownloadStatus.FailedDownload
                ) in [DownloadStatus.SuccessDownload, DownloadStatus.SkipDownload]:
                    unfinished_ids.discard(it)

            for _idx, _value in value.node.download_status.items():
                if DownloadStatus.SuccessDownload != _value and DownloadStatus.SkipDownload != _value:
                    unfinished_ids.add(_idx)

            # 失败次数达到上限的消息不再重试(避免每次重启都重复下载同一个失败文件)
            if self.max_retry_times > 0:
                for it in list(unfinished_ids):
                    if int(value.retry_counts.get(it, 0)) >= self.max_retry_times:
                        unfinished_ids.discard(it)

            value.ids_to_retry = sorted(unfinished_ids)
            value.ids_to_retry_dict = {it: True for it in value.ids_to_retry}

            entry = None
            for item in app_chat:
                if isinstance(item, dict) and item.get("chat_id") == key:
                    entry = item
                    break
            if entry is None:
                entry = {"chat_id": key}
                app_chat.append(entry)
            entry["ids_to_retry"] = value.ids_to_retry
            # 持久化已下载消息id集合(按消息id去重, 防跨日期目录重复下载)
            entry["already_download_ids"] = sorted(value.already_download_ids)
            entry["retry_counts"] = {
                str(k): int(v) for k, v in value.retry_counts.items() if int(v) > 0
            }
            # 断点: 仅在"起始消息ID自动更新"开启时持久化扫描断点
            # (关闭时保持 config.yaml 里手填的起始ID, 每次启动从头扫描, 靠已下载id去重跳过)
            if self.auto_update_last_read_id:
                entry["last_read_message_id"] = value.last_read_message_id
            else:
                entry.pop("last_read_message_id", None)

            if cfg_chats is not None:
                cfg_entry = None
                for item in cfg_chats:
                    if isinstance(item, dict) and item.get("chat_id") == key:
                        cfg_entry = item
                        break
                if cfg_entry is None:
                    cfg_entry = {"chat_id": key}
                    cfg_chats.append(cfg_entry)
                # 仅当"起始消息ID自动更新"开启时才把 last_read_message_id 写回 config
                if self.auto_update_last_read_id:
                    cfg_entry["last_read_message_id"] = value.last_read_message_id

        self.config["save_path"] = self.save_path
        self.config["file_path_prefix"] = self.file_path_prefix

        if self.config.get("ids_to_retry"):
            self.config.pop("ids_to_retry")

        if self.config.get("chat_id"):
            self.config.pop("chat_id")

        if self.config.get("download_filter"):
            self.config.pop("download_filter")

        if self.config.get("last_read_message_id"):
            self.config.pop("last_read_message_id")

        self.config["language"] = self.language.name

        if immediate:
            self._atomic_dump(self.config_file, self.config)
            self._atomic_dump(self.app_data_file, self.app_data)

    def set_language(self, language: Language):
        """Set Language"""
        self.language = language
        set_language(language)

    def set_bot_token(self, bot_token: str):
        """Update bot token in config (takes effect after restart)"""
        self.bot_token = bot_token
        self.config["bot_token"] = bot_token
        with open(self.config_file, "w", encoding="utf-8") as yaml_file:
            _yaml.dump(self.config, yaml_file)

    def get_chat_max_file_size(self, chat_id: Union[int, str]) -> int:
        """Get effective max file size limit in bytes for a chat.

        频道单独设置了 max_file_size(>0) 则用频道值;
        否则回落到全局默认大小限制(global_max_size_bytes)。
        0 表示不限。
        """
        config = self.chat_download_config.get(chat_id)
        if config is not None and getattr(config, "max_file_size", 0) > 0:
            return config.max_file_size
        return self.global_max_size_bytes

    def set_chat_max_file_size(self, chat_id: Union[int, str], max_file_size: int):
        """Set per-chat max file size limit in bytes and persist to config"""
        if chat_id not in self.chat_download_config:
            return False
        self.chat_download_config[chat_id].max_file_size = max_file_size
        for item in self.config.get("chat", []):
            if item.get("chat_id") == chat_id:
                item["max_file_size"] = max_file_size
                break
        with open(self.config_file, "w", encoding="utf-8") as yaml_file:
            _yaml.dump(self.config, yaml_file)
        return True

    def apply_web_config(self, new_cfg: dict) -> dict:
        """Apply a full config submitted from the web UI and persist to config.yaml.

        Parameters
        ----------
        new_cfg : dict
            Subset of config.yaml fields edited by the user:
            - chats: [{"chat_id": str, "last_read_message_id": int,
                       "max_file_size": int(bytes), "download_filter": str}]
            - media_types: list[str]
            - language: str (EN/ZH/RU/UA)
            - max_download_task: int
            - save_path: str
            - bot_token: str
            - proxy: {"scheme","hostname","port"} or None
            - web_port: int

        Returns
        -------
        dict
            {"ok": bool, "errors": [str]}
        """
        errors: List[str] = []

        # ---- media_types ----
        if "media_types" in new_cfg:
            mt = new_cfg["media_types"]
            if isinstance(mt, list):
                self.media_types = [str(m) for m in mt]
                self.config["media_types"] = self.media_types
            else:
                errors.append("media_types must be a list")

        # ---- language ----
        if "language" in new_cfg:
            lang = str(new_cfg["language"]).upper()
            try:
                self.language = Language[lang]
                self.config["language"] = self.language.name
            except KeyError:
                errors.append(f"invalid language: {lang}")

        # ---- max_download_task ----
        if "max_download_task" in new_cfg:
            try:
                n = int(new_cfg["max_download_task"])
                if n < 1:
                    raise ValueError
                self.max_download_task = n
                self.config["max_download_task"] = n
            except (TypeError, ValueError):
                errors.append("max_download_task must be a positive integer")

        # ---- max_download_task_per_chat (每频道并发, 0=不限制, 受限于全局) ----
        if "max_download_task_per_chat" in new_cfg:
            try:
                n = int(new_cfg["max_download_task_per_chat"] or 0)
                if n < 0:
                    raise ValueError
                self.max_download_task_per_chat = n
                self.config["max_download_task_per_chat"] = n
            except (TypeError, ValueError):
                errors.append(
                    "max_download_task_per_chat must be a non-negative integer"
                )

        # ---- save_path ----
        if "save_path" in new_cfg and new_cfg["save_path"]:
            self.save_path = str(new_cfg["save_path"])
            self.config["save_path"] = self.save_path

        # ---- bot_token ----
        if "bot_token" in new_cfg:
            token = str(new_cfg["bot_token"] or "").strip()
            self.bot_token = token
            self.config["bot_token"] = token

        # ---- proxy ----
        if "proxy" in new_cfg:
            px = new_cfg["proxy"]
            if px:
                self.proxy = {
                    "scheme": str(px.get("scheme", "http")),
                    "hostname": str(px.get("hostname", "")),
                    "port": int(px.get("port", 0)),
                }
            else:
                self.proxy = {}
            self.config["proxy"] = self.proxy or None

        # ---- web_port ----
        if "web_port" in new_cfg:
            try:
                p = int(new_cfg["web_port"])
                self.web_port = p
                self.config["web_port"] = p
            except (TypeError, ValueError):
                errors.append("web_port must be an integer")

        # ---- 全局大小限制 (max_size_enabled 开关 + max_size MB) ----
        if "max_size_enabled" in new_cfg or "max_size" in new_cfg:
            try:
                g_enabled = bool(new_cfg.get("max_size_enabled", True))
                g_mb = int(new_cfg.get("max_size", 0) or 0)
                if g_mb < 0:
                    g_mb = 0
                self.global_max_size_enabled = g_enabled
                self.global_max_size_mb = g_mb
                self.global_max_size_bytes = (
                    g_mb * 1024 * 1024 if g_enabled else 0
                )
                self.config["max_size_enabled"] = g_enabled
                self.config["max_size"] = g_mb
            except (TypeError, ValueError):
                errors.append("max_size must be a non-negative integer (MB)")

        # ---- 单文件下载超时(分钟, 0=不限) ----
        if "download_timeout" in new_cfg:
            try:
                t_out = int(new_cfg["download_timeout"] or 0)
                if t_out < 0:
                    t_out = 0
                self.download_timeout = t_out
                self.config["download_timeout"] = t_out
            except (TypeError, ValueError):
                errors.append("download_timeout must be a non-negative integer (minutes)")

        # ---- 定时重启 ----
        if "auto_restart_enabled" in new_cfg:
            self.auto_restart_enabled = bool(new_cfg["auto_restart_enabled"])
            self.config["auto_restart_enabled"] = self.auto_restart_enabled
        if "auto_restart_interval_hours" in new_cfg:
            try:
                ar_h = float(new_cfg["auto_restart_interval_hours"] or 6)
                if ar_h <= 0:
                    ar_h = 6
                self.auto_restart_interval_hours = ar_h
                self.config["auto_restart_interval_hours"] = ar_h
            except (TypeError, ValueError):
                errors.append("auto_restart_interval_hours must be a positive number (hours)")

        # ---- 起始消息ID自动更新开关 ----
        if "auto_update_last_read_id" in new_cfg:
            self.auto_update_last_read_id = bool(new_cfg["auto_update_last_read_id"])
            self.config["auto_update_last_read_id"] = self.auto_update_last_read_id

        # ---- 全局下载限速(MB/s, 0=不限速), 配置中心保存时也即时生效 ----
        if "download_speed_limit" in new_cfg:
            try:
                sl = float(new_cfg["download_speed_limit"] or 0)
                if sl < 0:
                    sl = 0.0
                limiter.configure(sl)
                self.config["download_speed_limit"] = sl
            except (TypeError, ValueError):
                errors.append("download_speed_limit must be a non-negative number")

        # ---- chats ----
        if "chats" in new_cfg:
            chats = new_cfg["chats"]
            if not isinstance(chats, list):
                errors.append("chats must be a list")
            else:
                new_chat_cfg = {}
                new_chat_list = []
                for item in chats:
                    chat_id = str(item.get("chat_id", "")).strip()
                    if not chat_id:
                        continue
                    try:
                        chat_id_num = int(chat_id)
                    except ValueError:
                        errors.append(f"invalid chat_id: {chat_id}")
                        continue
                    last_id = 0
                    try:
                        last_id = int(item.get("last_read_message_id", 0) or 0)
                    except (TypeError, ValueError):
                        last_id = 0
                    max_size = 0
                    try:
                        max_size = int(item.get("max_file_size", 0) or 0)
                    except (TypeError, ValueError):
                        max_size = 0
                    cfg = ChatDownloadConfig()
                    # 保留旧配置的已下载消息id集合, 避免Web保存配置后去重失效
                    _old_cfg = self.chat_download_config.get(chat_id_num)
                    if _old_cfg is not None:
                        cfg.already_download_ids = set(
                            _old_cfg.already_download_ids
                        )
                    cfg.last_read_message_id = last_id
                    cfg.max_file_size = max_size
                    cfg.download_filter = str(item.get("download_filter", "") or "")
                    new_chat_cfg[chat_id_num] = cfg
                    new_chat_list.append(
                        {
                            "chat_id": chat_id_num,
                            "last_read_message_id": last_id,
                            "max_file_size": max_size,
                            "download_filter": cfg.download_filter,
                        }
                    )
                if not errors:
                    self.chat_download_config = new_chat_cfg
                    if new_chat_list:
                        self.config["chat"] = new_chat_list
                    else:
                        self.config["chat"] = []
                    # 同步 data.yaml 中已存在的 chat 项,避免 update_config 越界
                    if "chat" in self.app_data and not isinstance(
                        self.app_data["chat"], list
                    ):
                        self.app_data["chat"] = []
                    if "chat" not in self.app_data:
                        self.app_data["chat"] = []
                    app_chat = self.app_data["chat"]
                    while len(app_chat) < len(new_chat_list):
                        app_chat.append({})
                    for idx, item in enumerate(new_chat_list):
                        if idx < len(app_chat):
                            app_chat[idx]["chat_id"] = item["chat_id"]
                            app_chat[idx].pop("ids_to_retry", None)

        # 写入前先用内存中的真实状态刷新断点/去重集合,
        # 否则会用旧的 data.yaml(app_data) 覆盖, 丢掉进程内已下载id → 重启后重复下载
        try:
            self.update_config(immediate=False)
        except Exception as e:  # pylint: disable = W0718
            logger.warning(f"apply_web_config refresh state error: {e}")

        # 写入 config.yaml / data.yaml (原子写, 防强杀写坏)
        try:
            self._atomic_dump(self.config_file, self.config)
            self._atomic_dump(self.app_data_file, self.app_data)
            self.state_dirty = False
            self._last_state_save_ts = time.time()
        except Exception as e:
            errors.append(f"failed to write config: {e}")

        return {"ok": not errors, "errors": errors}

    def request_restart(self):
        """Ask the main loop to exit so the container can be restarted by
        the compose restart policy (takes effect with new config).

        The flag is set on a short delay so the Flask response to the web
        UI is flushed before the main loop notices and the process exits.
        """

        def _set_flag():
            self.restart_program = True

        self.loop.call_later(2.0, _set_flag)


    def set_download_speed_limit(self, mbps: float) -> dict:
        """运行时设置全局下载限速(MB/s)并持久化, 不重启容器。

        只改写 config.yaml 中的限速字段, 不走 update_config(): 后者会重算
        chat 的 last_read_message_id / ids_to_retry, 与本操作无关且有副作用。
        """
        value = max(float(mbps), 0.0)
        limiter.configure(value)
        self.config["download_speed_limit"] = value

        cfg_path = os.path.join(os.path.abspath("."), self.config_file)
        with open(cfg_path, "r", encoding="utf-8") as f:
            disk_cfg = _yaml.load(f.read()) or {}
        disk_cfg["download_speed_limit"] = value
        # config.yaml 是 docker bind mount 的单文件挂载点, os.replace 会报
        # EBUSY(Resource busy); 与项目原有 update_config() 一致, 采用原地写
        with open(cfg_path, "w", encoding="utf-8") as f:
            _yaml.dump(disk_cfg, f)
        return limiter.status()

    def get_config_summary(self) -> dict:
        """Return a human-readable summary of the current config for the web UI.

        Returns
        -------
        dict
            Summary containing media types, per-chat size limits, language,
            proxy status, download state, etc.
        """
        # 媒体类型中文名映射
        media_names = {
            "photo": "图片",
            "video": "视频",
            "audio": "音频(MP3等)",
            "voice": "语音",
            "document": "文档",
            "video_note": "视频笔记",
            "sticker": "贴纸",
        }
        media_list = []
        for mt in self.media_types:
            media_list.append(
                {"key": mt, "name": media_names.get(mt, mt), "enabled": True}
            )

        # 常见媒体类型,标记是否启用
        all_media_keys = [
            "photo",
            "video",
            "audio",
            "voice",
            "document",
            "video_note",
        ]
        media_status = {}
        for k in all_media_keys:
            media_status[k] = k in self.media_types

        chats = []
        for chat_id, cfg in self.chat_download_config.items():
            chats.append(
                {
                    "chat_id": str(chat_id),
                    "title": self.chat_titles.get(str(chat_id), ""),
                    "max_file_size": getattr(cfg, "max_file_size", 0),
                    "download_filter": getattr(cfg, "download_filter", "") or "",
                    "last_read_message_id": getattr(
                        cfg, "last_read_message_id", 0
                    ),
                }
            )

        return {
            "media_types": self.media_types,
            "media_status": media_status,
            "media_list": media_list,
            "chats": chats,
            "language": self.language.name,
            "proxy": self.proxy,
            "bot_token": self.bot_token,
            "bot_token_set": bool(self.bot_token),
            "max_download_task": self.max_download_task,
            "web_port": self.web_port,
            "save_path": self.save_path,
            "max_size_enabled": self.global_max_size_enabled,
            "max_size": self.global_max_size_mb,
            "download_timeout": self.download_timeout,
            "auto_restart_enabled": self.auto_restart_enabled,
            "auto_restart_interval_hours": self.auto_restart_interval_hours,
            "auto_update_last_read_id": self.auto_update_last_read_id,
            "max_download_task_per_chat": self.max_download_task_per_chat,
            "download_speed_limit": limiter.mbps,
        }

    def load_config(self):
        """Load user config"""
        with open(
            os.path.join(os.path.abspath("."), self.config_file), encoding="utf-8"
        ) as f:
            config = _yaml.load(f.read())
            if config:
                self.config = config
                self.assign_config(self.config)

        if os.path.exists(os.path.join(os.path.abspath("."), self.app_data_file)):
            with open(
                os.path.join(os.path.abspath("."), self.app_data_file),
                encoding="utf-8",
            ) as f:
                app_data = _yaml.load(f.read())
                if app_data:
                    self.app_data = app_data
                    self.assign_app_data(self.app_data)

    def pre_run(self):
        """before run application do"""
        self.cloud_drive_config.pre_run()
        if not os.path.exists(self.session_file_path):
            os.makedirs(self.session_file_path)
        set_language(self.language)

    def init_already_download_ids_from_disk(self):
        """启动时扫描 downloads 目录, 把已存在的媒体文件对应的消息id
        并入各频道的 already_download_ids 集合。

        背景: 下载目录可能按"下载日期"分目录(用户补丁), 同一条消息在不同
        日期被重新扫描时目标路径不同, 原有的"文件是否存在"去重会失效,
        导致同一视频被重复下载。这里以消息id为准做去重。
        """
        if not os.path.isdir(self.save_path):
            return
        # 频道目录名候选: chat_id 字符串 或 频道标题(经 validate_title)
        for chat_id, cfg in self.chat_download_config.items():
            dir_candidates = {validate_title(f"{chat_id}")}
            title = self.chat_titles.get(str(chat_id), "")
            if title:
                dir_candidates.add(validate_title(title))

            for chat_dir in os.listdir(self.save_path):
                if chat_dir not in dir_candidates:
                    continue
                chat_path = os.path.join(self.save_path, chat_dir)
                if not os.path.isdir(chat_path):
                    continue
                for date_dir in os.listdir(chat_path):
                    dpath = os.path.join(chat_path, date_dir)
                    if not os.path.isdir(dpath):
                        continue
                    # 递归扫描(目录层级可能不止 频道/日期 两层)
                    for root, _dirs, files in os.walk(dpath):
                        for fn in files:
                            fpath = os.path.join(root, fn)
                            if not os.path.isfile(fpath):
                                continue
                            # 文件名形如 "消息ID - 原始文件名.扩展名"; 兼容无分隔符的纯前缀
                            head = fn.split(" - ")[0].strip()
                            if not head.isdigit():
                                m = re.match(r"^(\d+)", fn)
                                head = m.group(1) if m else ""
                            if head.isdigit():
                                cfg.already_download_ids.add(int(head))
            if cfg.already_download_ids:
                logger.info(
                    f"chat[{chat_id}] 已从磁盘初始化 "
                    f"{len(cfg.already_download_ids)} 个已下载消息id(去重)"
                )

    def set_caption_name(
        self, chat_id: Union[int, str], media_group_id: Optional[str], caption: str
    ):
        """set caption name map

        Parameters
        ----------
        chat_id: str
            Unique identifier for this chat.

        media_group_id: Optional[str]
            The unique identifier of a media message group this message belongs to.

        caption: str
            Caption for the audio, document, photo, video or voice, 0-1024 characters.
        """
        if not media_group_id:
            return

        if chat_id in self.caption_name_dict:
            self.caption_name_dict[chat_id][media_group_id] = caption
        else:
            self.caption_name_dict[chat_id] = {media_group_id: caption}

    def get_caption_name(
        self, chat_id: Union[int, str], media_group_id: Optional[str]
    ) -> Optional[str]:
        """set caption name map
                media_group_id: Optional[str]
            The unique identifier of a media message group this message belongs to.

        caption: str
            Caption for the audio, document, photo, video or voice, 0-1024 characters.
        """

        if (
            not media_group_id
            or chat_id not in self.caption_name_dict
            or media_group_id not in self.caption_name_dict[chat_id]
        ):
            return None

        return str(self.caption_name_dict[chat_id][media_group_id])

    def set_caption_entities(
        self, chat_id: Union[int, str], media_group_id: Optional[str], caption_entities
    ):
        """
        set caption entities map
        """
        if not media_group_id:
            return

        if chat_id in self.caption_entities_dict:
            self.caption_entities_dict[chat_id][media_group_id] = caption_entities
        else:
            self.caption_entities_dict[chat_id] = {media_group_id: caption_entities}

    def get_caption_entities(
        self, chat_id: Union[int, str], media_group_id: Optional[str]
    ):
        """
        get caption entities map
        """
        if (
            not media_group_id
            or chat_id not in self.caption_entities_dict
            or media_group_id not in self.caption_entities_dict[chat_id]
        ):
            return None

        return self.caption_entities_dict[chat_id][media_group_id]

    def set_download_id(
        self, node: TaskNode, message_id: int, download_status: DownloadStatus
    ):
        """Set Download status"""
        if download_status is DownloadStatus.SuccessDownload:
            self.total_download_task += 1

        if node.chat_id not in self.chat_download_config:
            return

        # 成功下载的消息id记入去重集合(自动扫描路径)
        if download_status is DownloadStatus.SuccessDownload:
            self.chat_download_config[node.chat_id].already_download_ids.add(
                message_id
            )

        # 失败的消息累计重试次数, 达到上限后不再重试(防每次重启重复下载)
        if download_status is DownloadStatus.FailedDownload:
            cfg = self.chat_download_config[node.chat_id]
            cfg.retry_counts[message_id] = int(cfg.retry_counts.get(message_id, 0)) + 1

        self.chat_download_config[node.chat_id].finish_task += 1

        self.chat_download_config[node.chat_id].last_read_message_id = max(
            self.chat_download_config[node.chat_id].last_read_message_id, message_id
        )
        # 标记状态变化, 由 state_persist_watcher 定期落盘
        self.state_dirty = True
