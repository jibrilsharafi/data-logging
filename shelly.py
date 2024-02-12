import classes
import os
import logging
from pprint import pformat
import time
from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS

FORMAT = '[%(levelname)s | %(asctime)-15s | %(filename)s | %(funcName)s | %(module)s] %(message)s'
logging.basicConfig(level=logging.INFO, format=FORMAT)

DATA_LOGGING_FREQUENCY_SECOND = 1.0

try:
    from dotenv import load_dotenv
    if load_dotenv():
        logging.info("Environment variables loaded from .env file")
    else:
        logging.warning("Couldn't find .env file")
except ModuleNotFoundError:
    logging.warning("dotenv module not found")
    
DICT_SHELLY_LOCATION_ID = {
    "Trieste": "c45bbe562378",
    "Wagner": "c45bbe78ad66"
}

obj_shelly = classes.Shelly3Em(
    url=os.getenv("SHELLY-URL"),
    token=os.getenv("SHELLY-TOKEN")
)

client_influxdb = InfluxDBClient(
    url=os.getenv("INFLUXDB-URL"),
    token=os.getenv("INFLUXDB-TOKEN"),
    org=os.getenv("INFLUXDB-ORG")
)
write_api = client_influxdb.write_api(write_options=SYNCHRONOUS)

if __name__ == "__main__":
    logging.info("Starting main script...")
    
    while True:
        list_point = []
        
        try:
            for location, id in DICT_SHELLY_LOCATION_ID.items():
                point = obj_shelly.get_point_influxdb(id, location)
                list_point.extend(point)
                logging.debug(f"Point: {pformat(point)}")
        except Exception as e:
            logging.error(f"Coulnd't get Shelly data: {e}")
            
        if len(list_point) > 0:
            logging.debug(f"List point: {pformat(list_point)}")
            
            try:
                res = write_api.write(bucket=os.getenv("INFLUXDB-BUCKET"), record=list_point)
                logging.info("Data written to InfluxDB")
            except Exception as e:
                logging.error(f"Couldn't write data to InfluxDB: {e}")