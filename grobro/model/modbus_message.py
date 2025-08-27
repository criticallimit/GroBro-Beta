from typing import Optional
from datetime import datetime
import struct
import logging
from pydantic.main import BaseModel
from enum import Enum

LOG = logging.getLogger(__name__)

HEADER_STRUCT = ">HHHBB30s"


class GrowattModbusBlock(BaseModel):
    start: int
    end: int
    values: bytes

    @staticmethod
    def parse_grobro(buffer) -> Optional["GrowattModbusBlock"]:
        try:
            (start, end) = struct.unpack(">HH", buffer[0:4])
            num_blocks = end - start + 1
            result = GrowattModbusBlock(
                start=start,
                end=end,
                values=buffer[4 : 4 + num_blocks * 2]
            )
            if len(result.values) != num_blocks * 2:
                LOG.warning("Unexpected GrowattModbusBlock size")
                return None
            return result
        except Exception as e:
            LOG.warning("Parsing GrowattModbusBlock failed: %s", e)
            return None

    def build_grobro(self) -> bytes:
        return struct.pack(">HH", self.start, self.end) + self.values

    def size(self):
        return 4 + len(self.values)


class GrowattModbusFunction(int, Enum):
    READ_HOLDING_REGISTER = 3
    READ_INPUT_REGISTER = 4
    READ_SINGLE_REGISTER = 5
    PRESET_SINGLE_REGISTER = 6
    PRESET_MULTIPLE_REGISTER = 16


class GrowattMetadata(BaseModel):
    device_sn: str
    timestamp: Optional[datetime]

    def size(self):
        return 37

    @staticmethod
    def parse_grobro(buffer) -> Optional["GrowattMetadata"]:
        try:
            device_serial_raw = struct.unpack(">30s", buffer[0:30])[0]
            device_serial = device_serial_raw.decode("ascii", errors="ignore").strip("\x00")
            year, month, day, hour, minute, second, millis = struct.unpack(">7B", buffer[30:37])
            timestamp = datetime(year + 2000, month, day, hour, minute, second, microsecond=millis*1000)
            return GrowattMetadata(device_sn=device_serial, timestamp=timestamp)
        except Exception:
            return GrowattMetadata(device_sn=device_serial, timestamp=None)

    def build_grobro(self) -> bytes:
        return struct.pack(
            ">30s7B",
            self.device_sn.encode("ascii").ljust(30, b"\x00"),
            self.timestamp.year - 2000,
            self.timestamp.month,
            self.timestamp.day,
            self.timestamp.hour,
            self.timestamp.minute,
            self.timestamp.second,
            int(self.timestamp.microsecond / 1000)
        )


class GrowattModbusMessage(BaseModel):
    unknown: int
    device_id: str
    metadata: Optional[GrowattMetadata] = None
    function: GrowattModbusFunction
    register_blocks: list[GrowattModbusBlock]

    @property
    def msg_len(self):
        result = 32
        if self.metadata:
            result += self.metadata.size()
        for block in self.register_blocks:
            result += block.size()
        return result

    def get_data(self, pos):
        for block in self.register_blocks:
            if block.start > pos.register_no or block.end < pos.register_no:
                continue
            block_pos = (pos.register_no - block.start) * 2 + pos.offset
            return block.values[block_pos : block_pos + pos.size]
        return None

    @staticmethod
    def parse_grobro(buffer) -> Optional["GrowattModbusMessage"]:
        try:
            (unknown, constant_7, msg_len, constant_1, function, device_id_raw) = struct.unpack(
                HEADER_STRUCT, buffer[0:38]
            )
            device_id = device_id_raw.decode("ascii", errors="ignore").strip("\x00")
            if function not in [e.value for e in GrowattModbusFunction]:
                LOG.info("Unknown modbus function for %s: %s", device_id, function)
                return None

            offset = 38
            metadata = None
            if function == GrowattModbusFunction.READ_INPUT_REGISTER:
                metadata = GrowattMetadata.parse_grobro(buffer[offset:])
                offset += metadata.size()

            register_blocks = []
            while offset + 4 <= len(buffer):
                block = GrowattModbusBlock.parse_grobro(buffer[offset:])
                if not block:
                    break
                register_blocks.append(block)
                offset += block.size()

            return GrowattModbusMessage(
                unknown=unknown,
                metadata=metadata,
                device_id=device_id,
                function=GrowattModbusFunction(function),
                register_blocks=register_blocks
            )
        except Exception as e:
            LOG.exception("Parsing GrowattModbusMessage failed")
            return None

    def build_grobro(self) -> bytes:
        result = struct.pack(
            HEADER_STRUCT,
            self.unknown,
            7,
            self.msg_len,
            1,
            self.function.value,
            self.device_id.encode("ascii").ljust(30, b"\x00"),
        )
        if self.metadata:
            result += self.metadata.build_grobro()
        for block in self.register_blocks:
            result += block.build_grobro()
        return result

