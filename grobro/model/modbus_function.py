from grobro.model.modbus_message import GrowattModbusFunction
import struct
from pydantic import BaseModel
from enum import Enum
from pylint.checkers.base import register
from typing import Optional

MODBUS_COMMAND_STRUCT = ">HHHBB30sHH"


class GrowattModbusFunctionMultiple(BaseModel):
    """
    Represents a message that can be sent to the inverter
    to read or write multiple registers.

    Structure:
        - H - 2 byte unknown
        - H - 2 byte constant 7
        - H - 2 byte message length (excluding register count, constant and message length)
        - B - 1 byte modbus device address (seems to be constant 1 in mqtt)
        - B - 1 byte function
        - 30s - 30 byte zero-padded device id
        - H - 2 byte start register
        - H - 2 byte end register
        - N x H - N bytes values
    """

    device_id: str
    function: GrowattModbusFunction
    start: int
    end: int
    values: bytes

    @staticmethod
    def parse_grobro(buffer) -> Optional["GrowattModbusFunctionMultiple"]:
        (
            constant_1,
            constant_7,
            msg_len,
            constant_1_b,
            function,
            device_id_raw,
            start,
            end,
        ) = struct.unpack(MODBUS_COMMAND_STRUCT, buffer[0:42])

        device_id = device_id_raw.decode("ascii", errors="ignore").strip("\x00")
        values = buffer[42:]

        return GrowattModbusFunctionMultiple(
            device_id=device_id,
            function=function,
            start=start,
            end=end,
            values=values,
        )

    def build_grobro(self) -> bytes:
        header = struct.pack(
            MODBUS_COMMAND_STRUCT,
            1,
            7,
            36 + len(self.values),
            1,
            self.function,
            self.device_id.encode("ascii").ljust(30, b"\x00"),  # device_id
            self.start,
            self.end,
        )
        return header + self.values


class GrowattModbusFunctionMultipleSerial(BaseModel):
    """
    Liest mehrere Register auf einmal (z. B. komplette Seriennummer).
    Kombiniert die Werte als ASCII-String.
    """

    device_id: str
    function: GrowattModbusFunction
    start: int
    end: int
    value: str  # gesamter String, z.B. Seriennummer

    @staticmethod
    def parse_grobro(buffer) -> Optional["GrowattModbusFunctionMultipleSerial"]:
        # Header auspacken (wie bisher)
        (
            constant_1,
            constant_7,
            msg_len,
            constant_1_b,
            function,
            device_id_raw,
            start,
            end,
        ) = struct.unpack(">HHHBB30sHH", buffer[0:42])

        device_id = device_id_raw.decode("ascii", errors="ignore").strip("\x00")
        values_raw = buffer[42:]

        # Alle Werte als ASCII-String zusammenfassen
        value = values_raw.decode("ascii", errors="ignore").strip("\x00")

        return GrowattModbusFunctionMultipleSerial(
            device_id=device_id,
            function=function,
            start=start,
            end=end,
            value=value,
        )

    def build_grobro(self) -> bytes:
        # Header + Werte zusammenpacken
        values_bytes = self.value.encode("ascii").ljust((self.end - self.start + 1) * 2, b"\x00")
        header = struct.pack(
            ">HHHBB30sHH",
            1,
            7,
            36 + len(values_bytes),
            1,
            self.function,
            self.device_id.encode("ascii").ljust(30, b"\x00"),
            self.start,
            self.end,
        )
        return header + values_bytes


class GrowattModbusFunctionSingle(BaseModel):
    """
    Represents a message that can be sent to the inverter
    to read or write single registers.

    Structure:
        - H - 2 byte unknown
        - H - 2 byte constant 7
        - H - 2 byte message length (excluding register count, constant and message length)
        - B - 1 byte modbus device address (seems to be constant 1 in mqtt)
        - B - 1 byte function
        - 30s - 30 byte zero-padded device id
        - H - 2 byte register
        - H - 2 byte either: register (again) for READ_SINGLE_REGISTER or value for PRESET_SINGLE_REGISTER
    """

    device_id: str
    function: GrowattModbusFunction
    register: int
    value: int

    @staticmethod
    def parse_grobro(buffer) -> Optional["GrowattModbusFunctionSingle"]:
        (
            constant_1,
            constant_7,
            msg_len,
            constant_1_b,
            function,
            device_id_raw,
            register,
            value,
        ) = struct.unpack(MODBUS_COMMAND_STRUCT, buffer[0:42])

        device_id = device_id_raw.decode("ascii", errors="ignore").strip("\x00")

        return GrowattModbusFunctionSingle(
            device_id=device_id,
            function=function,
            register=register,
            value=value,
        )

    def build_grobro(self) -> bytes:
        return struct.pack(
            MODBUS_COMMAND_STRUCT,
            1,
            7,
            36,
            1,
            self.function,
            self.device_id.encode("ascii").ljust(30, b"\x00"),  # device_id
            self.register,
            self.value,
        )
