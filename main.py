import classes
import os
import logging
from pprint import pformat
import datetime
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

FORMAT = '[%(levelname)s | %(asctime)-15s | %(filename)s | %(funcName)s | %(module)s] %(message)s'
logging.basicConfig(level=logging.INFO, format=FORMAT)

DICT_DATA_LOGGING_FREQUENCY_SECOND = {
    "co2signal": 3600,
    "shelly": 1,
    "huawei_pv": 60,
    "modbusrtuf4n200": 1
}

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

LIST_CO2SIGNAL_ZONE_CODES = [
    "IT-NO",
    "IT-CSO"
]

obj_co2signal = classes.Co2Signal(
    url=os.getenv("CO2SIGNAL-URL"),
    auth=os.getenv("CO2SIGNAL-AUTH"),
)

obj_shelly = classes.Shelly3Em(
    url=os.getenv("SHELLY-URL"),
    token=os.getenv("SHELLY-TOKEN")
)

obj_huawei_pv = classes.HuaweiPv(
    username=os.getenv("HUAWEI_PV-USERNAME"),
    password=os.getenv("HUAWEI_PV-PASSWORD"),
    subdomain=os.getenv("HUAWEI_PV-SUBDOMAIN"),
    captcha_model_path=os.path.join(os.getcwd(), "utils/captcha_huawei.onnx")
)

obj_modbusrtuf4n200 = classes.ModbusRtuF4N200(
    port=os.getenv("MODBUS_RTU_F4N200-PORT")
)

client_influxdb = InfluxDBClient(
    url=os.getenv("INFLUXDB-URL"),
    token=os.getenv("INFLUXDB-TOKEN"),
    org=os.getenv("INFLUXDB-ORG")
)    
write_api = client_influxdb.write_api(write_options=SYNCHRONOUS)

if __name__ == "__main__":
    logging.info("Starting main script...")
    
    now = datetime.datetime.now()

    while True:
        list_point = []
        
        delta_time = datetime.datetime.now() - now
        
        try:
            if delta_time.seconds >= DICT_DATA_LOGGING_FREQUENCY_SECOND["co2signal"]:
                for zone_code in LIST_CO2SIGNAL_ZONE_CODES:
                    point = obj_co2signal.get_point_influxdb(zone_code)
                    list_point.extend(point)
                    logging.debug(f"Point: {pformat(point)}")
        except Exception as e:
            logging.error(f"Coulnd't get CO2Signal data: {e}")
            
        try:
            if delta_time.seconds >= DICT_DATA_LOGGING_FREQUENCY_SECOND["shelly"]:
                for location, id in DICT_SHELLY_LOCATION_ID.items():
                    point = obj_shelly.get_point_influxdb(id, location)
                    list_point.extend(point)
                    logging.debug(f"Point: {pformat(point)}")
        except Exception as e:
            logging.error(f"Coulnd't get Shelly data: {e}")
                
        try:
            if delta_time.seconds >= DICT_DATA_LOGGING_FREQUENCY_SECOND["huawei_pv"]:
                point = obj_huawei_pv.get_point_influxdb()
                list_point.extend(point)
                logging.debug(f"Point: {pformat(point)}")
        except Exception as e:
            logging.error(f"Coulnd't get Huawei PV data: {e}")
            
        try:
            if delta_time.seconds >= DICT_DATA_LOGGING_FREQUENCY_SECOND["modbusrtuf4n200"]:
                point = obj_modbusrtuf4n200.get_all_points_influxdb()
                list_point.extend(point)
                logging.debug(f"Point: {pformat(point)}")
        except Exception as e:
            logging.error(f"Coulnd't get Modbus RTU F4N200 data: {e}")
        
        if len(list_point) > 0:
            logging.debug(f"List point: {pformat(list_point)}")
            
            try:
                res = write_api.write(bucket=os.getenv("INFLUXDB-BUCKET"), record=list_point)
                logging.info("Data written to InfluxDB")
            except Exception as e:
                logging.error(f"Couldn't write data to InfluxDB: {e}")
                
            now = datetime.datetime.now()