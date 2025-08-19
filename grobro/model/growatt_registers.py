from typing import Optional, Union
from enum import Enum
from pydantic import BaseModel, Field
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
        value = struct.unpack(unpack_type, data_raw)[0]

        if self.data_type == GrowattRegisterDataTypes.FLOAT:
            opts = self.float_options
            return round(value * opts.multiplier + opts.delta, 3)

        elif self.data_type == GrowattRegisterDataTypes.TIME_HHMM:
            h, m = divmod(value, 256)
            return h * 100 + m

        elif self.data_type == GrowattRegisterDataTypes.INT:
            return value

        elif self.data_type == GrowattRegisterDataTypes.ENUM:
            opts = self.enum_options
            if opts.enum_type == GrowattRegisterEnumTypes.INT_MAP:
                return opts.values.get(int(value), None)

            elif opts.enum_type == GrowattRegisterEnumTypes.BITFIELD:
                result = {}
                for bit_index, name in opts.values.items():
                    result[name] = bool((value >> bit_index) & 1)
                return result

        elif self.data_type == GrowattRegisterDataTypes.STRING:
            return data_raw.decode("ascii", errors="ignore").strip("\x00")


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


# ⚡ Alias für alte Nutzung, V2-konform
class HomeAssistantHoldingRegisterValue(BaseModel):
    name: str
    value: Union[str, float, int]
    register_value: HomeAssistantHoldingRegister = Field(..., alias="register")

    class Config:
        validate_by_name = True

    @property
    def register(self):
        return self.register_value

    @register.setter
    def register(self, value):
        self.register_value = value


class HomeAssistantHoldingRegisterInput(BaseModel):
    device_id: str
    payload: list[HomeAssistantHoldingRegisterValue] = []


class HomeAssistantInputRegisterV2(BaseModel):
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


# Daten laden
with resources.files(__package__).joinpath("growatt_neo_registers.json").open("rb") as f:
    KNOWN_NEO_REGISTERS = GroBroRegisters.parse_obj(json.load(f))
with resources.files(__package__).joinpath("growatt_noah_registers.json").open("rb") as f:
    KNOWN_NOAH_REGISTERS = GroBroRegisters.parse_obj(json.load(f))
with resources.files(__package__).joinpath("growatt_nexa_registers.json").open("rb") as f:
    KNOWN_NEXA_REGISTERS = GroBroRegisters.parse_obj(json.load(f))
