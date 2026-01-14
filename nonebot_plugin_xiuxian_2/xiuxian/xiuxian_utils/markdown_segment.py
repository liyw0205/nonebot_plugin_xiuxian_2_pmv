"""OneBot v11 消息类型 markdown扩充。

FrontMatter:
    sidebar_position: 5
    description: onebot.v11.message 模块
"""
from urllib.parse import quote

from nonebot.adapters.onebot.v11 import MessageSegment
from typing_extensions import Self


class MessageSegmentPlus(MessageSegment):
    @classmethod
    def markdown_template(
            cls,
            md_id: str,
            msg_body: list,
            button_id: str = ""
    ) -> Self:
        """
        markdown模板
        :param md_id: 模板id
        :param msg_body: 模板参数
        :param button_id: 按钮id，为空时不添加键盘
        :return:
        """
        data = {
            "markdown": {
                "custom_template_id": md_id,
                "params": msg_body
            }
        }
        
        if button_id:
            data["keyboard"] = {"id": button_id}
            
        return cls("markdown", {"data": data})

    @classmethod
    def markdown(
            cls,
            msg_body: list,
    ) -> Self:
        """
        原生markdown
        :param msg_body: 消息内容
        :return:
        """
        return cls(
            "markdown",
            {
                "data": {
                    "markdown": {
                        "content": msg_body
                    }
                }
            }
        )


def markdown_param(key, value):
    """
    markdown模板参数定义
    :param key:
    :param value:
    :return:
    """
    return {"key": key,
            "values": [value]}


def cmd_urlencoded(cmd_str):
    """
    in line cmd编码urlencoded工具
    :param cmd_str:
    :return:
    """
    return quote(cmd_str)


"""
构造范例

markdown源码：

{{.title}}

---
``
{{.text}}
``

---
>[{{.cmd_1}}](mqqapi://aio/inlinecmd?command={{.cmd_1_url}}&enter=false&reply=false)

---
>[{{.cmd_2}}](mqqapi://aio/inlinecmd?command={{.cmd_2_url}}&enter=false&reply=false)

---
>[{{.cmd_3}}](mqqapi://aio/inlinecmd?command={{.cmd_3_url}}&enter=false&reply=false)

---
>更多：[{{.connect_cmd}}](mqqapi://aio/inlinecmd?command={{.connect_cmd_url}}&enter=false&reply=false)


params = [markdown_param("title", "标题"),
          markdown_param("text", "内容"),
          markdown_param("cmd_1", "命令1"),
          markdown_param("cmd_1_url", cmd_urlencoded("命令1")),
          markdown_param("cmd_2", "命令2"),
          markdown_param("cmd_2_url", cmd_urlencoded("命令2")),
          markdown_param("cmd_3", "命令3"),
          markdown_param("cmd_3_url", cmd_urlencoded("命令3")),
          markdown_param("connect_cmd", "命令4"),
          markdown_param("connect_cmd_url", cmd_urlencoded("命令4"))]
          
markdown = MessageSegmentPlus.markdown_template("123456_789456", params)
# bot.send(event, markdown)

"""
