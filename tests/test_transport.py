import json
import logging
import queue
import socket
import threading
import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

import requests

from wechatrobot import Api, WeChatRobot
from wechatrobot.Bus import EventBus
from wechatrobot.WeChatRobot import _MessageHandler


def frame(text="hello", **fields):
    message = dict(type=1, message=text, sender="friend", isSendMsg=0,
                   isSendByPhone=0)
    message.update(fields)
    return json.dumps(message, ensure_ascii=False).encode("utf-8") + b"\n"


class FakeSocket:
    def __init__(self, chunks):
        self.chunks = iter(chunks)
        self.acks = []
        self.reads = 0

    def recv(self, size):
        self.reads += 1
        # An unexpected read after EOF must fail the test, not spin forever.
        return next(self.chunks)

    def sendall(self, data):
        self.acks.append(data)


class FramingTests(unittest.TestCase):
    def handle(self, chunks, robot=None):
        robot = robot or Mock()
        connection = FakeSocket(chunks)
        _MessageHandler(connection, ("127.0.0.1", 1), SimpleNamespace(robot=robot))
        return connection, robot

    def test_fragmented_utf8_and_coalesced_frames(self):
        first, second, third = frame("你好"), frame("second"), frame("third")
        split = first.index("你".encode()) + 1
        connection, robot = self.handle([
            first[:split], first[split:] + second + third[:12], third[12:], b"",
        ])
        self.assertEqual(
            [c.args[0]["message"] for c in robot._receive_callback.call_args_list],
            ["你好", "second", "third"],
        )
        self.assertEqual(connection.acks, [b"200 OK"] * 3)

    def test_missing_newline_is_capped_at_16_mib(self):
        self.assertEqual(_MessageHandler.MAX_FRAME_SIZE, 16 * 1024 * 1024)
        chunks = (b"x" * 4096 for _ in range(4097))
        with self.assertLogs("wechatrobot.WeChatRobot", logging.WARNING):
            connection, robot = self.handle(chunks)
        self.assertEqual(connection.reads, 4097)
        self.assertEqual(connection.acks, [])
        robot._receive_callback.assert_not_called()

    def test_exact_limit_frame_accepts_delimiter_in_next_read(self):
        data = frame("at limit")
        with patch.object(_MessageHandler, "MAX_FRAME_SIZE", len(data) - 1):
            connection, robot = self.handle([data[:-1], b"\n", b""])
        self.assertEqual(connection.acks, [b"200 OK"])
        robot._receive_callback.assert_called_once()

    def test_limit_applies_per_frame_in_coalesced_read(self):
        data = frame()
        with patch.object(_MessageHandler, "MAX_FRAME_SIZE", len(data) - 1):
            connection, robot = self.handle([data * 3, b""])
        self.assertEqual(connection.acks, [b"200 OK"] * 3)
        self.assertEqual(robot._receive_callback.call_count, 3)

    def test_oversized_terminated_frame_closes_without_ack(self):
        with patch.object(_MessageHandler, "MAX_FRAME_SIZE", 32):
            with self.assertLogs("wechatrobot.WeChatRobot", logging.WARNING):
                connection, robot = self.handle([b"x" * 33 + b"\n" + frame()])
        self.assertEqual(connection.acks, [])
        robot._receive_callback.assert_not_called()

    def test_valid_frame_is_delivered_before_oversized_tail_closes_connection(self):
        data = frame()
        with patch.object(_MessageHandler, "MAX_FRAME_SIZE", len(data) - 1):
            with self.assertLogs("wechatrobot.WeChatRobot", logging.WARNING):
                connection, robot = self.handle([data + b"x" * len(data)])
        self.assertEqual(connection.acks, [b"200 OK"])
        robot._receive_callback.assert_called_once()

    def test_eof_exits_without_acknowledging_empty_message(self):
        connection, robot = self.handle([b""])
        self.assertEqual(connection.reads, 1)
        self.assertEqual(connection.acks, [])
        robot._receive_callback.assert_not_called()

    def test_truncated_frame_is_discarded_at_eof(self):
        with self.assertLogs("wechatrobot.WeChatRobot", logging.WARNING):
            connection, robot = self.handle([frame()[:-1], b""])
        self.assertEqual(connection.reads, 2)
        self.assertEqual(connection.acks, [])
        robot._receive_callback.assert_not_called()

    def test_bad_frames_do_not_discard_valid_tail(self):
        robot = WeChatRobot()
        callback = Mock()
        robot.on("friend_msg")(callback)
        with self.assertLogs("wechatrobot.WeChatRobot", logging.ERROR):
            connection, _ = self.handle([
                b"bad json\n\xff\n[]\n{}\n" + frame("valid"), b"",
            ], robot)
        self.assertEqual(connection.acks, [b"200 OK"] * 5)
        callback.assert_called_once()
        self.assertEqual(callback.call_args.args[0]["message"], "valid")

    def test_callback_exception_preserves_connection_and_other_subscribers(self):
        robot = WeChatRobot()
        bad, good = Mock(side_effect=RuntimeError("callback failed")), Mock()
        robot.on("friend_msg")(bad)
        robot.on("friend_msg")(good)
        with self.assertLogs("wechatrobot.Bus", logging.ERROR):
            self.handle([frame("one") + frame("two"), b""], robot)
        self.assertEqual(bad.call_count, 2)
        self.assertEqual(good.call_count, 2)

    def test_socket_errors_exit_cleanly(self):
        for operation in ("recv", "sendall"):
            with self.subTest(operation=operation):
                connection = Mock()
                connection.recv.return_value = frame()
                getattr(connection, operation).side_effect = OSError("disconnected")
                robot = Mock()
                _MessageHandler(connection, ("127.0.0.1", 1),
                                SimpleNamespace(robot=robot))
                robot._receive_callback.assert_not_called()


class SubscriptionTests(unittest.TestCase):
    def test_decorator_returns_original_function_and_instances_are_isolated(self):
        first, second = WeChatRobot(), WeChatRobot()
        callback = Mock()
        self.assertIs(first.on("friend_msg", "group_msg")(callback), callback)
        second._receive_callback(json.loads(frame()))
        callback.assert_not_called()
        first._receive_callback(json.loads(frame()))
        first._receive_callback(json.loads(frame(sender="group@chatroom")))
        self.assertEqual(callback.call_count, 2)

    def test_subscription_during_emit_uses_snapshot(self):
        bus = EventBus()
        added = Mock()
        bus.subscribe("event", lambda: bus.subscribe("event", added))
        bus.emit("event")
        added.assert_not_called()
        bus.emit("event")
        added.assert_called_once()


class LifecycleTests(unittest.TestCase):
    def setUp(self):
        self.robot = WeChatRobot(ip="127.0.0.1", port=0)
        for name in ("StartMsgHook", "StartImageHook", "StartVoiceHook",
                     "StopMsgHook", "StopImageHook", "StopVoiceHook"):
            setattr(self.robot, name, Mock(return_value={}))
        self.addCleanup(self.robot.stop)

    def connect(self):
        connection = socket.create_connection(self.robot._server.server_address,
                                              timeout=1)
        connection.settimeout(1)
        self.addCleanup(connection.close)
        return connection

    def test_hook_can_send_immediately_on_start_and_repeat_run_is_idempotent(self):
        received = queue.Queue()
        self.robot.on("friend_msg")(received.put)

        def start_hook(port):
            with socket.create_connection(("127.0.0.1", port), timeout=1) as conn:
                conn.sendall(frame("during startup"))
                self.assertEqual(conn.recv(6), b"200 OK")

        self.robot.StartMsgHook.side_effect = start_hook
        ident = self.robot.run(main_thread=False)
        self.assertIsNotNone(ident)
        self.assertEqual(received.get(timeout=1)["message"], "during startup")
        self.assertEqual(self.robot.run(main_thread=False), ident)
        self.robot.StartMsgHook.assert_called_once()

    def test_bind_failure_does_not_enable_hooks(self):
        with socket.socket() as occupied:
            occupied.bind(("127.0.0.1", 0))
            occupied.listen()
            self.robot.port = occupied.getsockname()[1]
            with self.assertLogs("wechatrobot.WeChatRobot", logging.ERROR):
                self.assertIsNone(self.robot.run(main_thread=False))
        self.robot.StartMsgHook.assert_not_called()
        self.robot.StopMsgHook.assert_not_called()
        self.assertIsNone(self.robot._server)

    def test_hook_failure_rolls_back_and_allows_restart(self):
        self.robot.StartImageHook.side_effect = requests.Timeout("hook stalled")
        with self.assertLogs("wechatrobot.WeChatRobot", logging.ERROR):
            self.assertIsNone(self.robot.run(main_thread=False))
        self.assertIsNone(self.robot._server)
        self.robot.StopMsgHook.assert_called_once()
        self.robot.StopImageHook.assert_called_once()
        self.robot.StopVoiceHook.assert_not_called()
        self.robot.StartVoiceHook.assert_not_called()
        self.robot.StartImageHook.side_effect = None
        self.assertIsNotNone(self.robot.run(main_thread=False))

    def test_thread_start_failure_closes_listener_without_enabling_hooks(self):
        with patch.object(threading.Thread, "start", side_effect=RuntimeError("no threads")):
            with self.assertLogs("wechatrobot.WeChatRobot", logging.ERROR):
                self.assertIsNone(self.robot.run(main_thread=False))
        self.assertIsNone(self.robot._server)
        self.robot.StartMsgHook.assert_not_called()
        self.assertIsNotNone(self.robot.run(main_thread=False))

    def test_explicit_error_at_each_start_hook_rolls_back_and_allows_restart(self):
        starts = ("StartMsgHook", "StartImageHook", "StartVoiceHook")
        stops = ("StopMsgHook", "StopImageHook", "StopVoiceHook")
        for failed_index, failed_name in enumerate(starts):
            with self.subTest(hook=failed_name):
                robot = WeChatRobot(ip="127.0.0.1", port=0)
                self.addCleanup(robot.stop)
                for name in starts + stops:
                    setattr(robot, name, Mock(return_value={"result": "OK"}))
                failed = getattr(robot, failed_name)
                captured = []

                def fail(**params):
                    captured.append((robot._server, robot._server_thread))
                    return {"result": "ERROR", "msg": "hook refused startup"}

                failed.side_effect = fail
                with self.assertLogs("wechatrobot.WeChatRobot", logging.ERROR):
                    self.assertIsNone(robot.run(main_thread=False))
                self.assertIsNone(robot._server)
                server, thread = captured[0]
                self.assertEqual(server.socket.fileno(), -1)
                self.assertFalse(thread.is_alive())
                for index, name in enumerate(stops):
                    if index <= failed_index:
                        getattr(robot, name).assert_called_once_with(timeout=1.0)
                    else:
                        getattr(robot, name).assert_not_called()
                        getattr(robot, starts[index]).assert_not_called()
                failed.side_effect = None
                robot.port = server.server_address[1]
                self.assertIsNotNone(robot.run(main_thread=False))
                robot.stop()

    def test_stop_unblocks_idle_handler(self):
        entered, finished = threading.Event(), threading.Event()
        with patch.object(_MessageHandler, "setup", side_effect=entered.set):
            with patch.object(_MessageHandler, "finish", side_effect=finished.set):
                self.robot.run(main_thread=False)
                connection = self.connect()
                self.assertTrue(entered.wait(1))
                self.robot.stop()
                self.assertTrue(finished.wait(1))
                self.assertEqual(connection.recv(1), b"")

    def test_stop_closes_active_socket_and_releases_port(self):
        received = queue.Queue()
        self.robot.on("friend_msg")(received.put)
        self.robot.run(main_thread=False)
        server, thread = self.robot._server, self.robot._server_thread
        address = server.server_address
        connection = self.connect()
        connection.sendall(frame())
        self.assertEqual(connection.recv(6), b"200 OK")
        received.get(timeout=1)
        self.robot.stop()
        self.assertFalse(thread.is_alive())
        self.assertEqual(connection.recv(1), b"")
        self.assertEqual(server.socket.fileno(), -1)
        self.robot.stop()
        for name in ("StopMsgHook", "StopImageHook", "StopVoiceHook"):
            getattr(self.robot, name).assert_called_once()
        self.robot.port = address[1]
        self.assertIsNotNone(self.robot.run(main_thread=False))

    def test_foreground_run_returns_after_external_stop(self):
        started = threading.Event()
        self.robot.StartVoiceHook.side_effect = lambda **kw: started.set()
        foreground = threading.Thread(target=self.robot.run)
        foreground.start()
        try:
            self.assertTrue(started.wait(1))
            self.robot.stop()
            foreground.join(1)
            self.assertFalse(foreground.is_alive())
        finally:
            self.robot.stop()
            foreground.join(1)

    def test_blocked_callback_does_not_hold_up_stop_or_other_connections(self):
        entered, release, completed = threading.Event(), threading.Event(), threading.Event()

        def callback(message):
            if message["message"] == "block":
                entered.set()
                try:
                    release.wait(2)
                finally:
                    completed.set()

        self.robot.on("friend_msg")(callback)
        self.robot.run(main_thread=False)
        connection = self.connect()
        connection.sendall(frame("block"))
        self.assertEqual(connection.recv(6), b"200 OK")
        self.assertTrue(entered.wait(1))
        try:
            other = self.connect()
            other.sendall(frame("other"))
            self.assertEqual(other.recv(6), b"200 OK")
            stopped = threading.Event()
            stop_thread = threading.Thread(target=lambda: (self.robot.stop(), stopped.set()))
            stop_thread.start()
            self.assertTrue(stopped.wait(1))
            stop_thread.join(1)
        finally:
            release.set()
            self.assertTrue(completed.wait(1))

    def test_cleanup_continues_when_stop_hook_fails(self):
        self.robot.run(main_thread=False)
        thread = self.robot._server_thread
        self.robot.StopVoiceHook.side_effect = requests.Timeout("hook stalled")
        with self.assertLogs("wechatrobot.WeChatRobot", logging.ERROR):
            self.robot.stop()
        self.assertFalse(thread.is_alive())
        self.robot.StopImageHook.assert_called_once()
        self.robot.StopMsgHook.assert_called_once()

    def test_stop_closes_locally_before_short_http_cleanup_and_continues_on_timeout(self):
        self.robot.run(main_thread=False)
        server, thread = self.robot._server, self.robot._server_thread

        def timed_out(**params):
            self.assertEqual(params, {"timeout": 1.0})
            self.assertEqual(server.socket.fileno(), -1)
            self.assertFalse(thread.is_alive())
            raise requests.Timeout("stop hook stalled")

        stops = ("StopMsgHook", "StopImageHook", "StopVoiceHook")
        for name in stops:
            getattr(self.robot, name).side_effect = timed_out
        with self.assertLogs("wechatrobot.WeChatRobot", logging.ERROR):
            self.robot.stop()
        for name in stops:
            getattr(self.robot, name).assert_called_once_with(timeout=1.0)

    def test_stop_accepts_custom_timeout_and_logs_explicit_errors(self):
        self.robot.run(main_thread=False)
        self.robot.StopVoiceHook.return_value = {"result": "ERROR"}
        with self.assertLogs("wechatrobot.WeChatRobot", logging.WARNING):
            self.robot.stop(timeout=0.25)
        for name in ("StopMsgHook", "StopImageHook", "StopVoiceHook"):
            getattr(self.robot, name).assert_called_once_with(timeout=0.25)


class HttpTests(unittest.TestCase):
    @patch.object(requests, "post")
    def test_set_version_timeout_is_passed_to_http_not_request_body(self, post):
        post.return_value.content = b"{}"
        robot = WeChatRobot()
        robot.SetVersion(version="3.9.12.55", timeout=5.0)
        self.assertEqual(post.call_args.kwargs["timeout"], 5.0)
        self.assertEqual(json.loads(post.call_args.kwargs["data"]),
                         {"version": "3.9.12.55"})
        robot.SetVersion(version="3.9.12.55")
        self.assertEqual(post.call_args.kwargs["timeout"], 60.0)
        self.assertEqual(json.loads(post.call_args.kwargs["data"]),
                         {"version": "3.9.12.55"})

    @patch.object(requests, "post")
    def test_stop_hook_timeouts_are_passed_to_http_not_request_body(self, post):
        post.return_value.content = b"{}"
        api = Api()
        for name in ("StopMsgHook", "StopImageHook", "StopVoiceHook"):
            with self.subTest(hook=name):
                stop = getattr(api, name)
                stop(timeout=0.25)
                self.assertEqual(post.call_args.kwargs["timeout"], 0.25)
                self.assertEqual(json.loads(post.call_args.kwargs["data"]), {})
                stop()
                self.assertEqual(post.call_args.kwargs["timeout"], 60.0)

    def test_exec_command_obeys_attribute_protocol_and_does_not_evaluate_text(self):
        api = Api()
        self.assertEqual(api.exec_command("SendText"), api.SendText)
        with self.assertRaises(AttributeError):
            api.exec_command("missing_command")
        self.assertFalse(hasattr(WeChatRobot(), "missing_command"))
        with patch.object(api, "SendText") as send:
            with self.assertRaises(AttributeError):
                api.exec_command('SendText(wxid="friend", msg="hello")')
            send.assert_not_called()

    @patch.object(requests, "post")
    def test_send_has_timeout_and_status_is_checked_before_json(self, post):
        api = Api()
        post.return_value.content = b'{"data": "ok"}'
        self.assertEqual(api.SendText(wxid="friend", msg="hello"), {"data": "ok"})
        self.assertEqual(post.call_args.kwargs["timeout"], 60.0)
        post.return_value.raise_for_status.assert_called_once()
        post.return_value.content = b"upstream unavailable"
        post.return_value.raise_for_status.side_effect = requests.HTTPError("503")
        with self.assertRaises(requests.HTTPError):
            api.SendText(wxid="friend", msg="hello")
        self.assertEqual(post.call_count, 2)  # Never automatically retry a send.

    @patch.object(requests, "post")
    def test_shorter_lookup_timeout_is_preserved(self, post):
        post.return_value.content = b"{}"
        Api().GetChatMsgBySvrId(msgid="123")
        self.assertEqual(post.call_args.kwargs["timeout"], 10.0)

    @patch.object(requests, "post")
    def test_qrcode_timeout_and_status(self, post):
        post.return_value.content = b"image"
        self.assertEqual(Api().GetQrcodeImage(), b"image")
        self.assertEqual(post.call_args.kwargs["timeout"], 60.0)
        post.return_value.raise_for_status.assert_called_once()
        post.return_value.raise_for_status.side_effect = requests.HTTPError("503")
        with self.assertRaises(requests.HTTPError):
            Api().GetQrcodeImage()

    @patch.object(requests, "post", side_effect=requests.Timeout("stalled"))
    def test_send_timeout_propagates_without_retry(self, post):
        with self.assertRaises(requests.Timeout):
            Api().SendText(wxid="friend", msg="hello")
        post.assert_called_once()


if __name__ == "__main__":
    unittest.main()
