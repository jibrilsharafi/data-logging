import classes
import os
import logging
from pprint import pformat

FORMAT = '[%(levelname)s | %(asctime)-15s | %(filename)s | %(funcName)s | %(module)s] %(message)s'
logging.basicConfig(level=logging.DEBUG, format=FORMAT)

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
    

if __name__ == "__main__":
    logging.info("Starting main script...")
    
    list_point = []
    
    for zone_code in LIST_CO2SIGNAL_ZONE_CODES:
        point = obj_co2signal.get_point_influxdb(zone_code)
        list_point.extend(point)
        logging.debug(f"Point: {pformat(point)}")
        
    for location, id in DICT_SHELLY_LOCATION_ID.items():
        point = obj_shelly.get_point_influxdb(id, location)
        list_point.extend(point)
        logging.debug(f"Point: {pformat(point)}")
    
    point = obj_huawei_pv.get_point_influxdb()
    list_point.extend(point)
    logging.debug(f"Point: {pformat(point)}")    
    
    point = obj_modbusrtuf4n200.get_all_points_influxdb()
    list_point.extend(point)
    logging.debug(f"Point: {pformat(point)}")
        
    logging.info(f"List of all points: {pformat(list_point)}")       
    
    logging.info("Main script finished")