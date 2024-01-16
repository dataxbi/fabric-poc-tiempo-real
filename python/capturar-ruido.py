
"""Lee la entrada de audio y envía los datos a Azure Event Hub.
Este script es parte de una POC de Microsoft Fabric en Tiempo Real.
"""
import json
from datetime import datetime

import sounddevice as sd
from azure.eventhub import EventHubProducerClient, EventData
from rich import print
from rich.panel import Panel
from rich.table import Table

DEVICE = 1

# Modificar estas variables usando la configuración de una Aplicaión Personalizada de EventStream
EVENT_HUB_CONNECTION_STR = "Endpoint=sb://eventstream-xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx.servicebus.windows.net/;SharedAccessKeyName=key_xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx;SharedAccessKey=xxxxxxxxxxxxxxxxxxx;EntityPath=xx_xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
EVENT_HUB_NAME = "xx_xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"

producer = None
console = None


def audio_callback(indata, frames, time, status):
    """Captura el audio"""
    audio_data = process_audio_data(indata)
    send_event_data_batch(audio_data)
    print_table(audio_data)


def send_event_data_batch(audio_data):
    """Envía los datos a Event Hub"""
    event_data_batch = producer.create_batch()

    for data in audio_data:
        data_json = json.dumps(data)
        event_data_batch.add(EventData(data_json))

    producer.send_batch(event_data_batch)


def process_audio_data(indata):
    """Procesa los datos de audio para extraer solo el primer canal y agregarle un timestamp a cada elemento de la lista"""
    indata_channel1 = [float(c[0]) for c in indata[::]]
    audio_data = []
    for audio in indata_channel1:
        data = {
            "fecha_hora": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "audio": audio,
        }
        audio_data.append(data)
    return audio_data


def print_table(audio_data):
    """Dibuja una tabla en la terminal con los datos capturados"""
    table = Table(title="Enviando datos")
    table.add_column("Hora")
    table.add_column("Audio")

    for data in audio_data:
        table.add_row(f"{data['fecha_hora']}", f"{data['audio']}")
    print(table)


try:
    producer = EventHubProducerClient.from_connection_string(
        conn_str=EVENT_HUB_CONNECTION_STR, eventhub_name=EVENT_HUB_NAME
    )

    print(Panel("Pulse cualquier tecla para terminar"))

    with sd.InputStream(device=DEVICE, channels=1, callback=audio_callback):
        k = input()

except KeyboardInterrupt:
    exit()
