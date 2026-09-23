"""本地协议编码接入：寄存器风格帧与结构化报文。"""

from .base import ProtocolDecoder
from .register import RegisterFrameDecoder, encode_register_frame
from .structured import StructuredDecoder, encode_structured

__all__ = [
    "ProtocolDecoder",
    "RegisterFrameDecoder",
    "encode_register_frame",
    "StructuredDecoder",
    "encode_structured",
]
