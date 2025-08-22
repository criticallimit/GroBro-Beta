import json
import logging
from typing import Optional

import paho.mqtt.client as mqtt

from .growatt_modbus import (
    GrowattModbusFunction,
    GrowattModbusFunctionSingle,
)
from .registers import (
    KNOWN_NEO_REGISTERS,
    KNOWN_NOAH_REGISTERS,
    KNOWN_NEXA_REGISTERS,
    GroBroRegisters,
)

LOG = logging.getLogger(__name__)

HA_BASE_TOPIC = "homeassistant"
MAX_SLOTS = 5


# ---------------------------
# Hilfsfunktionen
# ---------------------------

def get_known_registers(device_id: str) -> Optional[GroBroRegisters]:
    """Erkennt den passenden Registertyp anhand der Seriennummer."""
    if device_id.startswith("QMN"):
        return KNOWN_NEO_REGISTERS
    elif device_id.startswith("0PVP"):
        return KNOWN_NOAH_REGISTERS
    elif device_id.startswith("0HVR"):
        return KNOWN_NEXA_REGISTERS
    return None


def map_enum_value(reg, value):
    """Mapped ENUM-Werte auf die Klartextdarstellung für HA."""
    try:
        data = getattr(reg.growatt, "data", None)
        if not data or data.data_type != "ENUM":
            return value
        enum_opts = getattr(data, "enum_options", None)
        if not enum_opts or enum_opts.enum_type != "INT_MAP":
            return value
        return enum_opts.values.get(str(value), enum_opts.values.get(value, str(value)))
    except Exception as e:
        LOG.warning("Enum mapping failed for %s=%s: %s", reg, value, e)
        return value


def make_modbus_command(device_id, func, register_no, value=None):
    """Hilfsfunktion zum Erstellen eines Modbus-Kommandos."""
    return GrowattModbusFunctionSingle(
        device_id=device_id,
        function=func,
        register=register_no,
        value=value if value is not None else register_no,
    )


# ---------------------------
# Hauptklasse
# ---------------------------

class Client:
    def __init__(self, mqtt_client: mqtt.Client, on_command):
        self._client = mqtt_client
        self.on_command = on_command
        self._discovery_cache = []
        self._discovery_payload_cache = {}
        self._config_cache = {}

    # ---------------------------
    # Konfiguration
    # ---------------------------

    def set_config(self, configs: dict):
        self._config_cache = {conf.device_id: conf for conf in configs}
        for device_id in self._config_cache:
            self.__publish_device_discovery(device_id)

    def __device_info_from_config(self, device_id: str):
        return {
            "identifiers": [device_id],
            "name": f"Growatt {device_id}",
            "manufacturer": "Growatt",
            "serial_number": device_id,
        }

    # ---------------------------
    # Discovery
    # ---------------------------

    def __publish_device_discovery(self, device_id):
        known_registers = get_known_registers(device_id)
        if not known_registers:
            LOG.info("Unable to publish unknown device type: %s", device_id)
            return

        self.__migrate_entity_discovery(device_id, known_registers)
        topic = f"{HA_BASE_TOPIC}/device/{device_id}/config"

        payload = {
            "dev": self.__device_info_from_config(device_id),
            "avty_t": f"{HA_BASE_TOPIC}/grobro/{device_id}/availability",
            "o": {"name": "grobro", "url": "https://github.com/robertzaage/GroBro"},
            "cmps": {},
        }

        # Commands
        for cmd_name, cmd in known_registers.holding_registers.items():
            if not cmd.homeassistant.publish:
                continue
            if cmd_name.startswith("slot"):
                try:
                    if int(cmd_name[4]) > MAX_SLOTS:
                        continue
                except ValueError:
                    continue
            unique_id = f"grobro_{device_id}_cmd_{cmd_name}"
            cmd_type = cmd.homeassistant.type
            payload["cmps"][unique_id] = {
                "command_topic": f"{HA_BASE_TOPIC}/{cmd_type}/grobro/{device_id}/{cmd_name}/set",
                "state_topic": f"{HA_BASE_TOPIC}/{cmd_type}/grobro/{device_id}/{cmd_name}/get",
                "platform": cmd_type,
                "unique_id": unique_id,
                **cmd.homeassistant.dict(exclude_none=True),
            }

        # Read-All Button
        payload["cmps"][f"grobro_{device_id}_cmd_read_all"] = {
            "command_topic": f"{HA_BASE_TOPIC}/button/grobro/{device_id}/read_all/read",
            "platform": "button",
            "unique_id": f"grobro_{device_id}_cmd_read_all",
            "name": "Read All Values",
        }

        # States
        for state_name, state in known_registers.input_registers.items():
            if not state.homeassistant.publish:
                continue
            unique_id = f"grobro_{device_id}_{state_name}"
            payload["cmps"][unique_id] = {
                "platform": "sensor",
                "name": state.homeassistant.name,
                "state_topic": f"{HA_BASE_TOPIC}/grobro/{device_id}/state",
                "value_template": f"{{{{ value_json['{state_name}'] }}}}",
                "unique_id": unique_id,
                "object_id": f"{device_id}_{state_name}",
                "device_class": state.homeassistant.device_class,
                "state_class": state.homeassistant.state_class,
                "unit_of_measurement": state.homeassistant.unit_of_measurement,
                "icon": state.homeassistant.icon,
            }

        # NEW: Serial Number Entity
        payload["cmps"][f"grobro_{device_id}_serial"] = {
            "platform": "sensor",
            "name": "Serial Number",
            "state_topic": f"{HA_BASE_TOPIC}/grobro/{device_id}/serial",
            "unique_id": f"grobro_{device_id}_serial",
            "object_id": f"{device_id}_serial",
            "icon": "mdi:identifier",
        }

        payload_str = json.dumps(payload, sort_keys=True, separators=(",", ":"))
        if self._discovery_payload_cache.get(device_id) == payload_str:
            LOG.debug("Discovery unchanged for %s, skipping", device_id)
            self._discovery_cache.append(device_id)
            return

        LOG.info("Publishing updated discovery for %s", device_id)
        self._client.publish(topic, "", retain=True)
        self._client.publish(topic, payload_str, retain=True)
        self._discovery_payload_cache[device_id] = payload_str
        self._discovery_cache.append(device_id)

        # Publish the serial number value immediately
        self._client.publish(
            f"{HA_BASE_TOPIC}/grobro/{device_id}/serial",
            device_id,
            retain=True,
        )

    def __migrate_entity_discovery(self, device_id, known_registers):
        """Stubs – falls du später alte Entitäten bereinigen willst."""
        pass

    # ---------------------------
    # State Publish
    # ---------------------------

    def publish_input_register(self, state):
        payload = dict(state.payload)
        known_registers = get_known_registers(state.device_id)

        if known_registers:
            for key, value in list(payload.items()):
                reg = known_registers.input_registers.get(key)
                if reg:
                    payload[key] = map_enum_value(reg, value)

        self._client.publish(
            f"{HA_BASE_TOPIC}/grobro/{state.device_id}/state",
            json.dumps(payload, separators=(",", ":")),
        )

    # ---------------------------
    # Commands
    # ---------------------------

    def __on_message(self, client, userdata, msg):
        parts = msg.topic.split("/")
        if len(parts) == 5 and parts[0] in {"number", "button", "switch"}:
            cmd_type, _, device_id, cmd_name, action = parts
        else:
            return

        known_registers = get_known_registers(device_id)
        if not known_registers:
            LOG.warning("Unknown device_id: %s", device_id)
            return

        pos = known_registers.holding_registers.get(cmd_name)
        if not pos:
            LOG.warning("Unknown command name: %s", cmd_name)
            return

        if cmd_type == "button" and action == "read":
            self.on_command(make_modbus_command(
                device_id, GrowattModbusFunction.READ_SINGLE_REGISTER, pos.register_no
            ))
        elif cmd_type in {"number", "switch"} and action == "set":
            try:
                payload = int(msg.payload.decode())
            except ValueError:
                payload = str(msg.payload.decode())
            self.on_command(make_modbus_command(
                device_id, GrowattModbusFunction.WRITE_SINGLE_REGISTER, pos.register_no, payload
            ))
