from typing import Callable
import threading
import logging
import socketserver
import socket
import json

from .Api import Api
from .Bus import EventBus

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

logger = logging.getLogger(__name__)


class _MessageServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True

    def __init__(self, address, handler):
        self._connections = set()
        self._connections_lock = threading.Lock()
        super().__init__(address, handler)

    def get_request(self):
        request, address = super().get_request()
        with self._connections_lock:
            self._connections.add(request)
        return request, address

    def shutdown_request(self, request):
        try:
            super().shutdown_request(request)
        finally:
            with self._connections_lock:
                self._connections.discard(request)

    def server_close(self):
        super().server_close()
        with self._connections_lock:
            connections = tuple(self._connections)
        for connection in connections:
            try:
                connection.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            connection.close()


class _MessageHandler(socketserver.BaseRequestHandler):
    MAX_FRAME_SIZE = 16 * 1024 * 1024  # Bytes before the newline delimiter.

    def handle(self):
        pending = bytearray()
        try:
            while True:
                data = self.request.recv(4096)
                if not data:
                    # EOF is terminal, including a truncated last frame.
                    if pending:
                        logger.warning("Discarding incomplete message at EOF")
                    return
                # Earlier bytes have already been scanned for a delimiter.
                scan_from = len(pending)
                pending.extend(data)
                while True:
                    newline = pending.find(b"\n", scan_from)
                    frame_size = newline if newline >= 0 else len(pending)
                    if frame_size > self.MAX_FRAME_SIZE:
                        logger.warning("Closing connection: message exceeds %s bytes",
                                       self.MAX_FRAME_SIZE)
                        return
                    if newline < 0:
                        break
                    frame = pending[:newline]
                    del pending[:newline + 1]
                    scan_from = 0
                    self.request.sendall(b"200 OK")
                    try:
                        message = json.loads(frame.decode("utf-8"))
                        self.server.robot._receive_callback(message)
                    except Exception:
                        # A malformed frame must not discard subsequent frames.
                        logger.exception("Failed to process incoming message")
        except OSError:
            # Peer disconnects and stop() both interrupt blocking socket I/O.
            return


class WeChatRobot:
    BASE_PATH = "C:\\Users\\user\\My Documents\\WeChat Files"

    def __init__(self , ip : str = "0.0.0.0" , port : int = 23456, api_host : str = "127.0.0.1", api_port : int = 18888):
        self.ip = ip
        self.port = port
        self.api_host = api_host
        self.api_port = api_port
        self.api = Api(host=api_host, port=api_port)
        self._bus = EventBus()
        self._server = None
        self._server_thread = None
        self._hooks = []
        self._lifecycle_lock = threading.RLock()

        self.url = "http://{}:{}/".format(ip , port)

    def on(self, *event_type: str) -> Callable:
        def deco(func: Callable) -> Callable:
            for event in event_type:
                self._bus.subscribe(event, func)
            return func
        return deco

    def _receive_callback(self , msg):
        type_dict = {
            0      : 'eventnotify', # 成员变更
            1      : 'text',
            3      : 'image',
            9      : 'scancashmoney',            # 面对面付款
            34     : 'voice',
            35     : 'qqmail',
            37     : 'friendrequest',
            42     : 'card',
            43     : 'video',
            47     : 'animatedsticker',
            48     : 'location',
            49     : 'share',
            50     : 'voip',
            51     : 'phone',
            106    : 'sysnotify',   # system notification 修改群名称
            1009   : 'eventnotify', # 减少成员
            1010   : 'eventnotify', # 添加成员
            2000   : 'transfer',
            2001   : 'redpacket',
            2002   : 'miniprogram',
            2003   : 'groupinvite',
            2004   : 'file',
            2005   : 'revokemsg',
            2006   : 'groupannouncement',
            10000  : 'sysmsg',      # 拍一拍 语音消息 撤回消息 邀请入群 移出群
            10002  : 'other'        # multivoip , taptap , ClientCheckConsistency, 邀请加入群聊并分享历史消息
        }

        msg['type'] = type_dict.get(msg['type'] , 'unhandled'+str(msg['type']))

        if msg["type"] == "friendrequest":
            self._bus.emit("frdver_msg" , msg)
        elif msg["type"] == "card":
            self._bus.emit("card_msg" , msg)
        elif '<sysmsg type="revokemsg">' in msg["message"]:
            self._bus.emit("revoke_msg", msg)
        elif "微信转账" in msg["message"] and "<paysubtype>1</paysubtype>" in msg["message"]:
            self._bus.emit("transfer_msg", msg)
        elif (1 == msg["isSendMsg"]):
            if 1 == msg["isSendByPhone"]:
                self._bus.emit("self_msg", msg)
            else:
                self._bus.emit("sent_msg", msg)  # API 发出的消息,供 slave 拿 msgid(撤回/编辑用)
        elif "chatroom" in msg["sender"]:
            self._bus.emit("group_msg", msg)
        else:
            self._bus.emit("friend_msg", msg)

    def run(self, main_thread: bool = True):
        with self._lifecycle_lock:
            if self._server is not None:
                return self._server_thread.ident
            try:
                # Bind/listen before enabling hooks: the hook may connect immediately.
                server = _MessageServer((self.ip, self.port), _MessageHandler)
                server.robot = self
                self._server = server
                thread = threading.Thread(
                    target=server.serve_forever, kwargs={"poll_interval": 0.05},
                    name="wechatrobot-receiver", daemon=True,
                )
                self._server_thread = thread
                thread.start()
                for start, stop, params in (
                    (self.StartMsgHook, self.StopMsgHook,
                     {"port": server.server_address[1]}),
                    (self.StartImageHook, self.StopImageHook,
                     {"save_path": self.BASE_PATH}),
                    (self.StartVoiceHook, self.StopVoiceHook,
                     {"save_path": self.BASE_PATH}),
                ):
                    # A timed-out start can still have enabled the remote hook.
                    self._hooks.append(stop)
                    result = start(**params)
                    if isinstance(result, dict) and result.get("result") == "ERROR":
                        raise RuntimeError("Hook startup failed: %r" % result)
            except (Exception, KeyboardInterrupt):
                logger.exception("Failed to start message receiver")
                self.stop()
                return None
        if not main_thread:
            return thread.ident
        try:
            thread.join()
        except KeyboardInterrupt:
            pass
        finally:
            with self._lifecycle_lock:
                if self._server is server:
                    self.stop()
        return None

    def stop(self, timeout: float = 1.0):
        """Close local sockets, then disable hooks with short HTTP I/O timeouts."""
        with self._lifecycle_lock:
            server, thread = self._server, self._server_thread
            self._server = self._server_thread = None
            if server is not None:
                # shutdown() requires a running serve_forever(), never its thread.
                if thread is not None and thread.is_alive():
                    server.shutdown()
                    thread.join()
                server.server_close()
            hooks, self._hooks = self._hooks, []
            # Keep startup serialized with cleanup, so an old stop cannot disable
            # newly started hooks. Local sockets are already closed at this point.
            for stop_hook in reversed(hooks):
                try:
                    result = stop_hook(timeout=timeout)
                    if isinstance(result, dict) and result.get("result") == "ERROR":
                        logger.warning("Failed to disable message hook: %r", result)
                except Exception:
                    logger.exception("Failed to disable message hook")

    def get_base_path(self):
        return self.BASE_PATH

    def __getattr__(self , item : str):
        return self.api.exec_command(item)
