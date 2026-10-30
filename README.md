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

企业微信（`@openim`）语音只要 `msgid` 即可下载；动态表情需要把接收回调里的
原始 `message` XML 一起传给接口，因为下载地址和密钥都在该 XML 中：

```python
voice = bot.GetVoiceMsg(
    msgid=str(msg["msgid"]),
)
emoji = bot.GetEmojiMsg(
    msgid=str(msg["msgid"]),
    content=msg["message"],
)
```

成功响应为 `{"msg": 1, "result": "OK", "path": "..."}`，失败响应为
`{"msg": 0, "result": "ERROR", "err_msg": "..."}`。语音会保存为 Native
实际识别出的音频扩展名（企业微信通常为 `.silk`），不会在下载接口中转码。
动态表情支持 `cdnurl`、`cdnthumburl`、`encrypturl`，以及企业微信使用的
`tpurl` + `tpauthkey` + `aeskey` 加密下载链路。普通 Unicode emoji 或没有
可下载媒体地址的文本表情会返回错误。

这两个接口仅用于企业微信媒体，并且需要包含 `type=53/54` 的增强版 Native
Hook；私人微信的语音和表情仍使用原有接口与流程。
