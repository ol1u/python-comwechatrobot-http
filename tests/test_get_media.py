import json
import sys
import unittest
from pathlib import Path
from unittest.mock import patch


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from wechatrobot.Api import Api
from wechatrobot.Modles import WECHAT_GET_EMOJI_MSG, WECHAT_GET_VOICE_MSG


api_module = sys.modules["wechatrobot.Api"]


class FakeResponse:
    def __init__(self, payload):
        self.content = json.dumps(payload).encode("utf-8")


class GetMediaApiTest(unittest.TestCase):
    def test_voice_allows_msgid_only(self):
        api = Api()
        payload = {"msg": 1, "result": "OK", "path": "C:/voice.silk"}

        with patch.object(
            api_module.requests,
            "post",
            return_value=FakeResponse(payload),
        ) as mocked:
            response = api.GetVoiceMsg(msgid="123456")

        self.assertEqual(response, payload)
        self.assertTrue(
            mocked.call_args.args[0].endswith(
                f"/api/?type={WECHAT_GET_VOICE_MSG}"
            )
        )
        self.assertEqual(
            json.loads(mocked.call_args.kwargs["data"]),
            {
                "msgid": "123456",
                "content": None,
                "clientmsgid": None,
            },
        )
        self.assertEqual(mocked.call_args.kwargs["timeout"], 90.0)

    def test_emoji_passes_original_xml_and_optional_url(self):
        api = Api()
        payload = {"msg": 1, "result": "OK", "path": "C:/emoji.gif"}
        content = (
            '<msg><emoji tpurl="https://wwfile.work.weixin.qq.com/file?a=1&amp;b=2" '
            'tpauthkey="test-auth" aeskey="aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"/></msg>'
        )

        with patch.object(
            api_module.requests,
            "post",
            return_value=FakeResponse(payload),
        ) as mocked:
            response = api.GetEmojiMsg(
                msgid="987654",
                content=content,
                url="https://cdn/override.gif",
            )

        self.assertEqual(response, payload)
        self.assertTrue(
            mocked.call_args.args[0].endswith(
                f"/api/?type={WECHAT_GET_EMOJI_MSG}"
            )
        )
        request_body = json.loads(mocked.call_args.kwargs["data"])
        self.assertEqual(request_body["msgid"], "987654")
        self.assertEqual(request_body["content"], content)
        self.assertEqual(request_body["url"], "https://cdn/override.gif")
        self.assertEqual(mocked.call_args.kwargs["timeout"], 90.0)


if __name__ == "__main__":
    unittest.main()
