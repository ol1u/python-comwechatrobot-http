```
from wechatrobot import WeChatRobot


bot = WeChatRobot()

@bot.on("friend_msg")
def on_friend_msg(msg):
    bot.SendText(wxid = msg['sender'], msg = msg['message'])

@bot.on("group_msg")
def on_group_msg(msg):
    print(f"on_group_msg: {msg}")

@bot.on("self_msg")
def on_self_msg(msg):
    print(f"on_self_msg: {msg}")

bot.run()
```

For a background receiver, use `bot.run(main_thread=False)`. Call `bot.stop()`
to close the listener and connected sockets and disable the hooks started by
that instance. Stop requests use a 1-second HTTP connect/read timeout per hook;
override it with `bot.stop(timeout=...)`. These are I/O timeouts, not an overall
wall-clock deadline. Local sockets close before remote hook cleanup, which stays
serialized with startup. Startup exceptions and explicit `result: ERROR` responses
clean up the listener so `run()` can be retried.
Callbacks run sequentially on each connection; keep them short or hand work to
your own worker. A callback exception is logged without stopping other callbacks
or subsequent messages.
Frames may contain up to 16 MiB before the newline. Oversized frames close the
connection without acknowledgment, including when the newline never arrives.

Run transport regression tests (HTTP/Hook calls are mocked; sockets use loopback):

```sh
python -B -m unittest discover -s tests -v
```
