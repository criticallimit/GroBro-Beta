from typing import Optional, List
from datetime import datetime
from enum import Enum
import struct
import logging
from pydantic import BaseModel
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
                start=start,
                end=end,
                values=buffer[4 : 4 + num_blocks * 2]
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


class GrowattModbusFunctionMultipleSerial(BaseModel):
    device_id: str
    function: GrowattModbusFunction
    start: int
    end: int
    value: str

    def build_grobro(self) -> bytes:
        value_bytes = self.value.encode("ascii").ljust((self.end - self.start + 1) * 2, b"\x00")
        return struct.pack(">HH", self.start, self.end) + value_bytes

    def size(self):
        return 4 + (self.end - self.start + 1) * 2


class GrowattModbusMessage(BaseModel):
    unknown: int
    device_id: str
    metadata: Optional[GrowattMetadata] = None
    function: GrowattModbusFunction
    register_blocks: List[GrowattModbusBlock] = []
    multiple_serial_blocks: List[GrowattModbusFunctionMultipleSerial] = []

    @property
    def msg_len(self):
        length = 32  # 2 byte unknown + 30 byte device id
        if self.metadata:
            length += self.metadata.size()
        for block in self.register_blocks:
            length += block.size()
        for block in self.multiple_serial_blocks:
            length += block.size()
        return length

    def get_data(self, pos: GrowattRegisterPosition):
        # Normale Registerblöcke prüfen
        for block in self.register_blocks:
            if block.start <= pos.register_no <= block.end:
                block_pos = (pos.register_no - block.start) * 2 + pos.offset
                return block.values[block_pos : block_pos + pos.size]
        # MultipleSerial-Blocks prüfen
        for block in self.multiple_serial_blocks:
            if block.start <= pos.register_no <= block.end:
                block_bytes = block.value.encode("ascii")
                block_pos = (pos.register_no - block.start) * 2 + pos.offset
                return block_bytes[block_pos : block_pos + pos.size]
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
            register_blocks = []
            multiple_serial_blocks = []

            if function == GrowattModbusFunction.READ_INPUT_REGISTER:
                metadata = GrowattMetadata.parse_grobro(buffer[offset:])
                offset += metadata.size()

            while len(buffer) > offset + 4:
                start, end = struct.unpack(">HH", buffer[offset : offset + 4])
                num_regs = end - start + 1
                block_bytes = buffer[offset + 4 : offset + 4 + num_regs * 2]

                if all(32 <= b <= 126 or b == 0 for b in block_bytes):
                    value_str = block_bytes.decode("ascii", errors="ignore").strip("\x00")
                    multiple_serial_blocks.append(
                        GrowattModbusFunctionMultipleSerial(
                            device_id=device_id,
                            function=GrowattModbusFunction(function),
                            start=start,
                            end=end,
                            value=value_str
                        )
                    )
                else:
                    register_blocks.append(
                        GrowattModbusBlock(start=start, end=end, values=block_bytes)
                    )

                offset += 4 + num_regs * 2

            return GrowattModbusMessage(
                unknown=unknown,
                device_id=device_id,
                function=GrowattModbusFunction(function),
                metadata=metadata,
                register_blocks=register_blocks,
                multiple_serial_blocks=multiple_serial_blocks
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
        for block in self.multiple_serial_blocks:
            result += block.build_grobro()
        return result
