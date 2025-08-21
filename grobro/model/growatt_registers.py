from typing import Optional, Union, Callable
from enum import Enum
from pydantic import BaseModel
import importlib.resources as resources
import json
import struct


class GrowattRegisterDataTypes(str, Enum):
    ENUM = "ENUM"
    STRING = "STRING"
    FLOAT = "FLOAT"
    INT = "INT"
    TIME_HHMM = "TIME_HHMM"


class GrowattRegisterEnumTypes(str, Enum):
    INT_MAP = "INT_MAP"
    BITFIELD = "BITFIELD"


class GrowattRegisterFloatOptions(BaseModel):
    delta: float = 1
    multiplier: float = 1


class GrowattRegisterEnumOptions(BaseModel):
    enum_type: GrowattRegisterEnumTypes
    values: dict[int, str]


class GrowattRegisterDataType(BaseModel):
    data_type: GrowattRegisterDataTypes
    float_options: Optional[GrowattRegisterFloatOptions] = None
    enum_options: Optional[GrowattRegisterEnumOptions] = None

    def parse(self, data_raw: bytes):
        if not data_raw:
            return None
        unpack_type = {1: "!B", 2: "!H", 4: "!I"}[len(data_raw)]
        if self.data_type == GrowattRegisterDataTypes.FLOAT:
            opts = self.float_options
            value = struct.unpack(unpack_type, data_raw)[0]
            value *= opts.multiplier
            value += opts.delta
            return round(value, 3)
        elif self.data_type == GrowattRegisterDataTypes.TIME_HHMM:
            value = struct.unpack(unpack_type, data_raw)[0]
            h = value // 256
            m = value % 256
            return (h * 100) + m
        elif self.data_type == GrowattRegisterDataTypes.INT:
            value = struct.unpack(unpack_type, data_raw)[0]
            return value
        elif self.data_type == GrowattRegisterDataTypes.ENUM:
            opts = self.enum_options
            value = struct.unpack(unpack_type, data_raw)[0]
            if opts.enum_type == GrowattRegisterEnumTypes.BITFIELD:
                return None  # TODO: implement
            elif opts.enum_type == GrowattRegisterEnumTypes.INT_MAP:
                enum_value = opts.values.get(int(value), None)
                if not enum_value:
                    return None
                return value
        elif self.data_type == GrowattRegisterDataTypes.STRING:
            value = data_raw.decode("ascii", errors="ignore").strip("\x00")
            return value


class GrowattRegisterPosition(BaseModel):
    register_no: int
    offset: int = 0
    size: int = 2


class GrowattInputRegister(BaseModel):
    position: GrowattRegisterPosition
    data: GrowattRegisterDataType


class HomeAssistantHoldingRegister(BaseModel):
    name: str
    publish: bool
    type: str
    min: Optional[int] = None
    max: Optional[int] = None
    step: Optional[int] = None
    state_class: Optional[str] = None
    device_class: Optional[str] = None
    unit_of_measurement: Optional[str] = None
    icon: Optional[str] = None

    class Config:
        extra = "forbid"


class HomeassistantInputRegister(BaseModel):
    name: str
    publish: bool
    state_class: Optional[str] = None
    device_class: Optional[str] = None
    unit_of_measurement: Optional[str] = None
    icon: Optional[str] = None


class HomeAssistantHoldingRegisterValue(BaseModel):
    name: str
    value: Union[str, float, int]
    register: HomeAssistantHoldingRegister


class HomeAssistantHoldingRegisterInput(BaseModel):
    device_id: str
    payload: list[HomeAssistantHoldingRegisterValue] = []


class HomeAssistantInputRegister(BaseModel):
    device_id: str
    payload: dict[str, Union[str, float, int]] = {}


class GroBroInputRegister(BaseModel):
    growatt: GrowattInputRegister
    homeassistant: HomeassistantInputRegister


class GroBroHoldingRegister(BaseModel):
    growatt: Optional[GrowattInputRegister] = None
    homeassistant: HomeAssistantHoldingRegister


class GroBroRegisters(BaseModel):
    input_registers: dict[str, GroBroInputRegister]
    holding_registers: dict[str, GroBroHoldingRegister]


# -----------------------------
# Load known registers
# -----------------------------
with resources.files(__package__).joinpath("growatt_neo_registers.json").open("rb") as f:
    KNOWN_NEO_REGISTERS = GroBroRegisters.parse_obj(json.load(f))

with resources.files(__package__).joinpath("growatt_noah_registers.json").open("rb") as f:
    KNOWN_NOAH_REGISTERS = GroBroRegisters.parse_obj(json.load(f))

with resources.files(__package__).joinpath("growatt_nexa_registers.json").open("rb") as f:
    KNOWN_NEXA_REGISTERS = GroBroRegisters.parse_obj(json.load(f))


# -----------------------------
# Noah Firmware-Register intern
# -----------------------------
CONTROL_FW_HIGH = GrowattInputRegister(
    position=GrowattRegisterPosition(register_no=0x0C),
    data=GrowattRegisterDataType(data_type=GrowattRegisterDataTypes.INT),
)
CONTROL_FW_MID = GrowattInputRegister(
    position=GrowattRegisterPosition(register_no=0x0D),
    data=GrowattRegisterDataType(data_type=GrowattRegisterDataTypes.INT),
)
CONTROL_FW_LOW = GrowattInputRegister(
    position=GrowattRegisterPosition(register_no=0x0E),
    data=GrowattRegisterDataType(data_type=GrowattRegisterDataTypes.INT),
)


# -----------------------------
# Noah Firmware-Entity für Home Assistant
# -----------------------------
def read_noah_firmware(read_raw_register: Callable[[int, int], bytes]) -> str:
    """
    Liest High/Mid/Low-Register und erzeugt Firmware-Version als String.
    read_raw_register(register_no: int, size: int) -> bytes
    """
    high_raw = read_raw_register(CONTROL_FW_HIGH.position.register_no, CONTROL_FW_HIGH.position.size)
    mid_raw = read_raw_register(CONTROL_FW_MID.position.register_no, CONTROL_FW_MID.position.size)
    low_raw = read_raw_register(CONTROL_FW_LOW.position.register_no, CONTROL_FW_LOW.position.size)

    high = CONTROL_FW_HIGH.data.parse(high_raw) or 0
    mid = CONTROL_FW_MID.data.parse(mid_raw) or 0
    low = CONTROL_FW_LOW.data.parse(low_raw) or 0

    return f"{high}.{mid}.{low}"


class NoahFirmwareEntity(BaseModel):
    name: str = "firmware_version"
    publish: bool = True
    state_class: str = "measurement"
    device_class: str = "firmware"
    _read_func: Optional[Callable[[], str]] = None

    def value(self) -> str:
        if self._read_func:
            return self._read_func()
        return "0.0.0"


# -----------------------------
# Firmware direkt in Home Assistant verfügbar
# -----------------------------
NOAH_FIRMWARE = NoahFirmwareEntity(_read_func=lambda: read_noah_firmware(lambda reg_no, size: b"\x01\x02"))  # Platzhalter: echte Lese-Funktion einsetzen

KNOWN_NOAH_REGISTERS.input_registers["firmware_version"] = GroBroInputRegister(
    growatt=CONTROL_FW_HIGH,  # nur intern
    homeassistant=HomeassistantInputRegister(
        name=NOAH_FIRMWARE.name,
        publish=NOAH_FIRMWARE.publish,
        state_class=NOAH_FIRMWARE.state_class,
        device_class=NOAH_FIRMWARE.device_class,
    ),
)

# Die Low/Mid/High Register selbst werden nicht veröffentlicht
KNOWN_NOAH_REGISTERS.input_registers["control_fw_high"] = GroBroInputRegister(
    growatt=CONTROL_FW_HIGH,
    homeassistant=HomeassistantInputRegister(name="control_fw_high", publish=False),
)
KNOWN_NOAH_REGISTERS.input_registers["control_fw_mid"] = GroBroInputRegister(
    growatt=CONTROL_FW_MID,
    homeassistant=HomeassistantInputRegister(name="control_fw_mid", publish=False),
)
KNOWN_NOAH_REGISTERS.input_registers["control_fw_low"] = GroBroInputRegister(
    growatt=CONTROL_FW_LOW,
    homeassistant=HomeassistantInputRegister(name="control_fw_low", publish=False),
)
