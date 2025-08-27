from rope.base import serializer
from typing import Optional
from datetime import datetime
import struct
import logging
from pydantic.main import BaseModel
from enum import Enum
from pylint.checkers.base import register
from grobro.model.growatt_registers import GrowattRegisterPosition

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
                start=start, end=end, values=buffer[4 : 4 + num_blocks * 2]
            )
            assert len(result.values) == num_blocks * 2
            return result
        except Exception as e:
            LOG.warn("Parsing GrowattModbusBlock: %s", e)

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
        offset = 0
        device_serial_raw = struct.unpack(">30s", buffer[offset : offset + 30])[0]
        device_serial = device_serial_raw.decode("ascii", errors="ignore").strip("\x00")
        offset += 30
        year, month, day, hour, minute, second, millis = struct.unpack(
            ">7B", buffer[offset : offset + 7]
        )
        timestamp = None
        try:
            timestamp = datetime(
                year + 2000, month, day, hour, minute, second, microsecond=millis * 1000
            )
        except Exception:
            pass
        return GrowattMetadata(device_sn=device_serial, timestamp=timestamp)

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
            int(self.timestamp.microsecond / 1000),
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

    def get_data(self, pos: GrowattRegisterPosition):
        for block in self.register_blocks:
            if block.start > pos.register_no or block.end < pos.register_no:
                continue
            block_pos = (pos.register_no - block.start) * 2 + pos.offset
            return block.values[block_pos : block_pos + pos.size]
        return None

    def get_bat2_serial(self) -> Optional[str]:
        """Liest die Bat2-Seriennummer aus den Registern 33–36 (4 Register à 2 Bytes)"""
        bat2_pos = GrowattRegisterPosition(register_no=33, offset=0, size=8)
        raw_bytes = self.get_data(bat2_pos)
        if raw_bytes:
            return raw_bytes.decode("ascii", errors="ignore").strip("\x00")
        return None

    @staticmethod
    def parse_grobro(buffer) -> Optional["GrowattModbusMessage"]:
        try:
            (unknown, constant_7, msg_len, constant_1, function, device_id_raw) = struct.unpack(
                HEADER_STRUCT,
                buffer[0:38],
            )
            if msg_len != len(buffer[8:]):
                return None
            device_id = device_id_raw.decode("ascii", errors="ignore").strip("\x00")
            if function not in [e.value for e in GrowattModbusFunction]:
                LOG.info("Unknown modbus function for %s: %s", device_id, function)
                return None

            register_blocks = []
            offset = 38
            metadata = None
            if function == GrowattModbusFunction.READ_INPUT_REGISTER:
                metadata = GrowattMetadata.parse_grobro(buffer[offset:])
                offset += metadata.size()

            while len(buffer) > offset + 6:
                block = GrowattModbusBlock.parse_grobro(buffer[offset:])
                register_blocks.append(block)
                offset += block.size()

            return GrowattModbusMessage(
                unknown=unknown,
                metadata=metadata,
                device_id=device_id,
                function=function,
                register_blocks=register_blocks,
            )
        except Exception as e:
            LOG.warn("parsing GrowattModbusMessage: %s", e)

    def build_grobro(self) -> bytes:
        result = struct.pack(
            HEADER_STRUCT,
            self.unknown,
            7,
            self.msg_len,
            1,
            self.function,
            self.device_id.encode("ascii").ljust(30, b"\x00"),
        )
        if self.metadata:
            result += self.metadata.build_grobro()
        for block in self.register_blocks:
            result += block.build_grobro()
        return result
