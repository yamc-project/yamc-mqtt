# -*- coding: utf-8 -*-
# @author: Tomas Vitvar, https://vitvar.com, tomas@vitvar.com

import time
import json
import logging
import threading

import paho.mqtt.client as mqtt

from yamc.providers import EventProvider, Topic
from yamc.writers import Writer
from yamc.component import WorkerComponent, BaseComponent
from yamc.utils import Map


class MQTTBase(BaseComponent):
    """
    MQTTBase is a base mqtt client class.
    """

    def __init__(self, config, component_id):
        super().__init__(config, component_id)
        self.name = None
        self.address = None
        self.port = None
        self.keepalive = None
        self.reconnect_after = None
        self.loop_timeout = None
        self.client = None
        self.connected = False

    def init_config(self, config):
        self.name = config.value_str("name")
        self.address = config.value_str("address")
        self.port = config.value_int("port", default=1883)
        self.keepalive = config.value_int("keepalive", default=60)
        self.reconnect_after = config.value_int("reconnect_after", default=30)
        self.loop_timeout = config.value_int("loop_timeout", default=1)

    def on_connect(self, client, userdata, flags, rc):
        self.connected = True
        self.log.info(f"Connected to the MQTT broker at {self.address}:{self.port}")

    def on_disconnect(self, client, userdata, rc):
        self.log.info(f"Disconnected from the MQTT broker.")
        if rc != 0:
            self.log.error("The client was disconnected unexpectedly.")
        self.connected = False

    def on_message(self, client, userdata, message):
        pass

    def init_client(self):
        self.client = mqtt.Client(self.name)
        self.client.on_connect = self.on_connect
        self.client.on_disconnect = self.on_disconnect
        self.client.on_message = self.on_message

    def wait_for_connection(self, exit_event, reconnect=False):
        if reconnect or self.client is None or not self.connected:
            if self.client is not None:
                self.client.disconnect()
                self.connected = False
            self.init_client()
            while not exit_event.is_set():
                try:
                    self.client.connect(self.address, port=self.port, keepalive=self.keepalive)
                    break
                except Exception as e:
                    self.log.error(
                        f"Cannot connect to the MQTT broker at {self.address}:{self.port}. {str(e)}. "
                        + f"Will attemmpt to reconnect after {self.reconnect_after} seconds."
                    )
                    exit_event.wait(self.reconnect_after)

    def connection_loop(self, exit_event):
        self.wait_for_connection(exit_event)
        try:
            while not exit_event.is_set():
                try:
                    self.client.loop(timeout=self.loop_timeout, max_packets=1)
                    if not self.connected:
                        self.wait_for_connection(exit_event)
                except Exception as e:
                    self.log.error(f"Error occurred in the MQTT loop. {str(e)}")
                    self.wait_for_connection(exit_event, reconnect=True)
        finally:
            if self.connected:
                self.client.disconnect()


class MQTTProvider(EventProvider, WorkerComponent, MQTTBase):
    """
    MQTT provider subsribes to topics with MQTT broker and handles on_message events
    from the topics.
    """

    def __init__(self, config, component_id):
        super().__init__(config, component_id)
        self.init_config(self.config)

    def on_connect(self, client, userdata, flags, rc):
        super().on_connect(client, userdata, flags, rc)
        for topic_id in self.topics:
            self.log.info(f"Subscribing to the topic {topic_id}")
            self.client.subscribe(topic_id)

    def on_message(self, client, userdata, message):
        try:
            super().on_message(client, userdata, message)
            topic_id = message._topic.decode("utf-8")
            topic = self.select_one(topic_id)
            if topic:
                data = Map(json.loads(str(message.payload.decode("utf-8"))))
                self.log.info(f"--> recv: {topic_id}: {data}")
                topic.update(data)
        except Exception as e:
            self.log.error(str(e))

    def worker(self, exit_event):
        self.connection_loop(exit_event)


class MQTTWriter(Writer, MQTTBase):
    """
    MQTTWriter writes data to topics in MQTT broker.
    """

    def __init__(self, config, component_id):
        super().__init__(config, component_id)
        self.init_config(self.config)
        self.write_empty = False

    def healthcheck(self):
        super().healthcheck()
        return self.connected

    def do_write(self, items):
        for data in items:
            topic, data = data["data"]["topic"], json.dumps(data["data"]["data"])
            self.log.info(f"<-- send: {topic}: {data}")
            self.client.publish(topic, data)

    def start(self, exit_event):
        super().start(exit_event)
        threading.Thread(target=self.connection_loop, args=(exit_event,), daemon=True).start()
