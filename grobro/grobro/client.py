"""
Client for the grobro mqtt side, handling messages from/to
* growatt cloud
* growatt devices
"""

import os
import struct
import logging
import ssl
from typing import Callable

import paho.mqtt.client as mqtt
from paho.mqtt.client import MQTTMessage

from grobro import model
from grobro.grobro import parser
from grobro.grobro.builder import append_crc, scramble
from grobro.model.modbus_function import GrowattModbusFunctionSingle
from grobro.model.modbus_message import GrowattModbusFunction, GrowattModbusMessage
from grobro.model.mqtt_config import MQTTConfig
from grobro.model.growatt_registers import (
    GrowattRegisterDataType,
    GrowattRegisterDataTypes,
    GrowattRegisterEnumTypes,
    HomeAssistantHoldingRegisterInput,
    HomeAssistantHoldingRegisterValue,
    HomeAssistantInputRegister,
    KNOWN_NEO_REGISTERS,
    KNOWN_NOAH_REGISTERS,
    KNOWN_NEXA_REGISTERS,
)

LOG = logging.getLogger(__name__)
HA_BASE_TOPIC = os.getenv("HA_BASE_TOPIC", "homeassistant")

# Updated growatt cloud forwarding config
GROWATT_CLOUD = os.getenv("GROWATT_CLOUD", "false")
if GROWATT_CLOUD.lower() == "true":
    GROWATT_CLOUD_ENABLED = True
    GROWATT_CLOUD_FILTER = set()
elif GROWATT_CLOUD:
    GROWATT_CLOUD_ENABLED = True
    GROWATT_CLOUD_FILTER = set(map(str.strip, GROWATT_CLOUD.split(",")))
else:
    GROWATT_CLOUD_ENABLED = False
    GROWATT_CLOUD_FILTER = set()

DUMP_MESSAGES = os.getenv("DUMP_MESSAGES", "false").lower() == "true"
DUMP_DIR = os.getenv("DUMP_DIR", "/dump")

# Property to flag messages forwarded from growatt cloud
MQTT_PROP_FORWARD_GROWATT = mqtt.Properties(mqtt.PacketTypes.PUBLISH)
MQTT_PROP_FORWARD_GROWATT.UserProperty = [("forwarded-for", "growatt")]

# Property to flag messages forwarded from ha
MQTT_PROP_FORWARD_HA = mqtt.Properties(mqtt.PacketTypes.PUBLISH)
MQTT_PROP_FORWARD_HA.UserProperty = [("forwarded-for", "ha")]

# Property to flag messages as dry-run for debugging purposes
MQTT_PROP_DRY_RUN = mqtt.Properties(mqtt.PacketTypes.PUBLISH)
MQTT_PROP_DRY_RUN.UserProperty = [("dry-run", "true")]


def get_property(msg: MQTTMessage, name: str):
    """Retrieve a user property from MQTT message if it exists."""
    if msg.properties and msg.properties.UserProperty:
        for key, value in msg.properties.UserProperty:
            if key == name:
                return value
    return None


def dump_message_binary(topic: str, payload: bytes):
    """Dump raw MQTT payload to a binary file for debugging."""
    os.makedirs(DUMP_DIR, exist_ok=True)
    filename = os.path.join(DUMP_DIR, topic.replace("/", "_") + ".bin")
    with open(filename, "ab") as f:
        f.write(payload)
        f.write(b"\n")


class Client:
    on_config: Callable[[model.DeviceConfig], None]
    on_input_register: Callable[[HomeAssistantInputRegister], None]
    on_holding_register_input: Callable[[HomeAssistantHoldingRegisterInput], None]

    _client: mqtt.Client
    _forward_mqtt_config: model.MQTTConfig
    _forward_clients = {}

    def __init__(self, grobro_mqtt: MQTTConfig, forward_mqtt: MQTTConfig):
        LOG.info(
            f"Connecting to GroBro broker at '{grobro_mqtt.host}:{grobro_mqtt.port}'"
        )
        self._client = mqtt.Client(
            client_id="grobro-grobro",
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            protocol=mqtt.MQTTv5,
        )
        if grobro_mqtt.username and grobro_mqtt.password:
            self._client.username_pw_set(grobro_mqtt.username, grobro_mqtt.password)
        if grobro_mqtt.use_tls:
            self._client.tls_set(cert_reqs=ssl.CERT_NONE)
            self._client.tls_insecure_set(True)
        self._client.connect(grobro_mqtt.host, grobro_mqtt.port, 60)
        self._client.on_message = self.__on_message
        self._client.on_connect = self.__on_connect
        self._forward_mqtt_config = forward_mqtt

    def start(self):
        LOG.debug("GroBro: Start")
        self._client.loop_start()

    def stop(self):
        LOG.debug("GroBro: Stop")
        self._client.loop_stop()
        self._client.disconnect()
        for key, client in self._forward_clients.items():
            client.loop_stop()
            client.disconnect()

    def send_command(self, cmd: GrowattModbusFunctionSingle):
        scrambled = scramble(cmd.build_grobro())
        final_payload = append_crc(scrambled)

        topic = f"s/33/{cmd.device_id}"
        LOG.debug("Send command: %s: %s: %s", type(cmd).__name__, topic, cmd)

        result = self._client.publish(
            topic,
            final_payload,
            properties=MQTT_PROP_FORWARD_HA,
        )
        status = result[0]
        if status != 0:
            LOG.warning("Sending failed: %s", result)

    def __on_connect(self, client, userdata, flags, reason_code, properties):
        LOG.debug(f"Connected with result code {reason_code}")
        self._client.subscribe("c/#")      

    def __on_message(self, client, userdata, msg: MQTTMessage):
        # check for forwarded messages and ignore them
        forwarded_for = get_property(msg, "forwarded-for")
        if forwarded_for and forwarded_for in ["ha", "growatt"]:
            LOG.debug("Message forwarded from %s. Skipping...", forwarded_for)
            return

        file = get_property(msg, "file")
        LOG.debug(f"Received message (%s): %s: %s", file, msg.topic, msg.payload)
        if DUMP_MESSAGES:
            dump_message_binary(msg.topic, msg.payload)
        try:
            device_id = msg.topic.split("/")[-1]
            if GROWATT_CLOUD_ENABLED:
                if GROWATT_CLOUD == "true" or device_id in GROWATT_CLOUD_FILTER:
                    forward_client = self.__connect_to_growatt_server(device_id)
                    forward_client.publish(
                        msg.topic,
                        payload=msg.payload,
                        qos=msg.qos,
                        retain=msg.retain,
                    )

            unscrambled = parser.unscramble(msg.payload)
            LOG.debug(f"Received: %s %s", msg.topic, unscrambled.hex(" "))

            modbus_message = GrowattModbusMessage.parse_grobro(unscrambled)
            LOG.debug("Received modbus message: %s", modbus_message)
            if modbus_message:
                known_registers = None
                if device_id.startswith("QMN"):
                    known_registers = KNOWN_NEO_REGISTERS
                elif device_id.startswith("0PVP"):
                    known_registers = KNOWN_NOAH_REGISTERS
                elif device_id.startswith("0HVR"):
                    known_registers = KNOWN_NEXA_REGISTERS
                if not known_registers:
                    LOG.info("Modbus message from unknown device type: %s", device_id)
                    return

                if modbus_message.function == GrowattModbusFunction.READ_SINGLE_REGISTER:
                    state = HomeAssistantHoldingRegisterInput(device_id=device_id)
                    
                    for name, register in known_registers.holding_registers.items():
                        data_raw = modbus_message.get_data(register.growatt.position)
                        value = register.growatt.data.parse(data_raw)
                        if value is None:
                            continue
                        if register.homeassistant.type == "switch":
                            value = "ON" if value == 1 else "OFF"
                        state.payload.append(
                            HomeAssistantHoldingRegisterValue(
                                name=name,
                                value=value,
                                register=register.homeassistant,
                            )
                        )
                    self.on_holding_register_input(state)

                if modbus_message.function == GrowattModbusFunction.READ_INPUT_REGISTER:
                    state = HomeAssistantInputRegister(device_id=device_id)
                    
                    for name, register in known_registers.input_registers.items():
                        data_raw = modbus_message.get_data(register.growatt.position)
                        value = register.growatt.data.parse(data_raw)
                        # workaround for bad night-time messages
                        if name == "Ppv" and value > 1000000:
                            LOG.debug("Dropping bad payload: %s", device_id)
                            return
                        state.payload[name] = value
                    self.on_input_register(state)
                    return

                return

            msg_type = struct.unpack_from(">H", unscrambled, 4)[0]

            # TODO: implement proper response handling for NOAH message types
        except Exception:
            LOG.exception("Error handling message %s", msg.topic)

    def __connect_to_growatt_server(self, device_id: str) -> mqtt.Client:
        if device_id in self._forward_clients:
            return self._forward_clients[device_id]

        client = mqtt.Client(client_id=f"grobro-forward-{device_id}")
        if self._forward_mqtt_config.username and self._forward_mqtt_config.password:
            client.username_pw_set(
                self._forward_mqtt_config.username, self._forward_mqtt_config.password
            )
        if self._forward_mqtt_config.use_tls:
            client.tls_set(cert_reqs=ssl.CERT_NONE)
            client.tls_insecure_set(True)
        client.connect(self._forward_mqtt_config.host, self._forward_mqtt_config.port, 60)
        client.loop_start()
        self._forward_clients[device_id] = client
        return client
