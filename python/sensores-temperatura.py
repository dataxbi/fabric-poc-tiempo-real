"""Simula el envío de datos desde varios sensores de temperatura.
Este script es parte de una POC de Microsoft Fabric en Tiempo Real.
"""
import random
import json
import time
from datetime import datetime

from azure.eventhub import EventHubProducerClient, EventData
from rich import print
from rich.panel import Panel
from rich.table import Table

NUMBER_SENSORS = 60

MIN_TEMPERATURE = 15
MAX_TEMPERATURE = 30
TEMPERATURE_CHANGE = 0.1

# Modificar estas variables usando la configuración de una Aplicaión Personalizada de EventStream
EVENT_HUB_CONNECTION_STR = "Endpoint=sb://eventstream-xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx.servicebus.windows.net/;SharedAccessKeyName=key_xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx;SharedAccessKey=xxxxxxxxxxxxxxxxxxx;EntityPath=xx_xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
EVENT_HUB_NAME = "xx_xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"

producer = None
console = None


class TemperatureSensor:
    """
    Temperature sensor class.
    Min and max temperature values are in Celsius.
    """

    def __init__(
        self,
        sensor_id: int,
        min_temperature: int,
        max_temperature: int,
        temperature_change: float = TEMPERATURE_CHANGE,
        current_temperature: float = None,
    ) -> None:
        self.sensor_id = sensor_id
        self.min_temperature = min_temperature
        self.max_temperature = max_temperature
        self.temperature_change = temperature_change

        if current_temperature is None:
            current_temperature = (max_temperature + min_temperature) / 2
        self.current_temperature = current_temperature

    def generate_data(self) -> float:
        """
        Generate data to be published to the broker

        :return: Data
        """
        self.current_temperature += self.temperature_change

        if self.current_temperature >= self.max_temperature:
            self.current_temperature = self.max_temperature - random.uniform(0, 1)
            self.temperature_change = -abs(self.temperature_change)

        if self.current_temperature <= self.min_temperature:
            self.current_temperature = self.min_temperature + random.uniform(0, 1)
            self.temperature_change = abs(self.temperature_change)

        return self.current_temperature


def get_sensor_data(sensor: TemperatureSensor):
    """Extrae los datos de un sensor"""

    return {
        "sensor_id": sensor.sensor_id,
        "temperatura": sensor.generate_data(),
        "fecha_hora": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }


def send_event_data_batch(sensor_data_list):
    """Envía los datos a Event Hub"""
    event_data_batch = producer.create_batch()

    for data in sensor_data_list:
        data_json = json.dumps(data)
        event_data_batch.add(EventData(data_json))

    producer.send_batch(event_data_batch)


def print_table(sensor_data_list):
    """Dibuja una tabla en la terminal con los datos capturados"""
    table = Table(title="Enviando datos")
    table.add_column("Sensor")
    table.add_column("Hora")
    table.add_column("Temperatura")

    for data in sensor_data_list:
        table.add_row(
            f"{data['sensor_id']}", f"{data['fecha_hora']}", f"{data['temperatura']}"
        )
    print(table)


try:
    producer = EventHubProducerClient.from_connection_string(
        conn_str=EVENT_HUB_CONNECTION_STR, eventhub_name=EVENT_HUB_NAME
    )

    print(Panel("Pulse CTRL + C para terminar"))

    sensor_list = []
    for sensor_id in range(1, NUMBER_SENSORS + 1):
        min_temperature = random.uniform(MIN_TEMPERATURE, MIN_TEMPERATURE + 5)
        max_temperature = min_temperature + random.uniform(
            MAX_TEMPERATURE - MIN_TEMPERATURE - 10,
            MAX_TEMPERATURE - MIN_TEMPERATURE - 5,
        )
        sensor = TemperatureSensor(
            sensor_id=sensor_id,
            min_temperature=min_temperature,
            max_temperature=max_temperature,
        )
        sensor_list.append(sensor)

    while True:
        sensor_data_list = []
        for sensor in sensor_list:
            sensor_data = get_sensor_data(sensor)
            sensor_data_list.append(sensor_data)

        print_table(sensor_data_list)
        send_event_data_batch(sensor_data_list)
        time.sleep(1)


except KeyboardInterrupt:
    exit()
