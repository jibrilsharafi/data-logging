import requests
import logging
import minimalmodbus
import datetime
from fusion_solar_py.client import FusionSolarClient

class Co2Signal:
    
    def __init__(self,
        url: str,
        auth: str,
    ) -> None:
        
        INFLUXDB_MEASUREMENT = 'co2'
        
        logging.info(f"Initializing {self.__class__.__name__} class...")
        
        self.url = url
        self.auth = auth
        
        self.influxdb_measurement = INFLUXDB_MEASUREMENT
        
        return None        
        
    def get_data(self,
        zone_code: str,
    ) -> dict:
        url = f"{self.url}?countryCode={zone_code}"
        response = requests.get(url, headers={'auth-token': self.auth})
        
        if response.status_code == 200:
            return response.json()['data']
        else:
            logging.error(f"API call not successful. Status code: {response.status_code}, reason: {response.reason}. Full response: {response.json()}")
            return None
        
    def get_point_influxdb(self,
        zone_code: str,
    ) -> list:
        data = self.get_data(zone_code)
        
        if data:
            return [
                {
                    'measurement': "carbon_intensity",
                    'tags': {
                        'zone_code': zone_code,
                    },
                    'fields': {
                        'value': float(data['carbonIntensity']),
                    },
                    'time': datetime.datetime.utcnow().isoformat(),
                },
                {
                    'measurement': "fossil_fuel_percentage",
                    'tags': {
                        'zone_code': zone_code,
                    },
                    'fields': {
                        'value': float(data['fossilFuelPercentage']),
                    },
                    'time': datetime.datetime.utcnow().isoformat(),
                }
            ]
        else:
            logging.error(f"Coudn't get data for zone code {zone_code}")
            return None
        
class ModbusRtuF4N200:
    
    def __init__(self,
        port: str,
    ) -> None:
                
        BAUDRATE = 19200
        PARITY = 'E' # Even
        SLAVE_ADDRESS = 5
        
        LOCATION = "Laboratory"
        
        DICT_MODBUS_DATA = {
            "voltage-L1" : {
                "type": "long",
                "register_address": 4096,
                "scale": 0.001,
                "signed": False,
                "measurement": "voltage",
                "tags": {
                    "phase": "L1"
                }
            },
            "voltage-L2" : {
                "type": "long",
                "register_address": 4098,
                "scale": 0.001,
                "signed": False,
                "measurement": "voltage",
                "tags": {
                    "phase": "L2"
                }
            },
            "voltage-L3" : {
                "type": "long",
                "register_address": 4100,
                "scale": 0.001,
                "signed": False,
                "measurement": "voltage",
                "tags": {
                    "phase": "L3"
                }
            },
            "active_power-L1" : {
                "type": "long",
                "register_address": 4140,
                "scale": 0.01,
                "signed": False,
                "measurement": "active_power",
                "tags": {
                    "phase": "L1"
                }
            },
            "active_power-L2" : {
                "type": "long",
                "register_address": 4142,
                "scale": 0.01,
                "signed": False,
                "measurement": "active_power",
                "tags": {
                    "phase": "L2"
                }
            },
            "active_power-L3" : {
                "type": "long",
                "register_address": 4144,
                "scale": 0.01,
                "signed": False,
                "measurement": "active_power",
                "tags": {
                    "phase": "L3"
                }
            },
            "active_power-L1-sign" : {
                "type": "register",
                "register_address": 4146,
                "scale": 1.0,
                "signed": True,
                "measurement": "sign_active_power",
                "tags": {
                    "phase": "L1"
                }
            },
            "active_power-L2-sign" : {
                "type": "register",
                "register_address": 4147,
                "scale": 1.0,
                "signed": True,
                "measurement": "sign_active_power",
                "tags": {
                    "phase": "L2"
                }
            },
            "active_power-L3-sign" : {
                "type": "register",
                "register_address": 4148,
                "scale": 1.0,
                "signed": True,
                "measurement": "sign_active_power",
                "tags": {
                    "phase": "L3"
                }
            },
            "power_factor-L1" : {
                "type": "register",
                "register_address": 4164,
                "scale": 0.01,
                "signed": True,
                "measurement": "power_factor",
                "tags": {
                    "phase": "L1"
                }
            },
            "power_factor-L2" : {
                "type": "register",
                "register_address": 4165,
                "scale": 0.01,
                "signed": True,
                "measurement": "power_factor",
                "tags": {
                    "phase": "L2"
                }
            },
            "power_factor-L3" : {
                "type": "register",
                "register_address": 4166,
                "scale": 0.01,
                "signed": True,
                "measurement": "power_factor",
                "tags": {
                    "phase": "L3"
                }
            },
            "active_energy-positive" : {
                "type": "long",
                "register_address": 4688,
                "scale": 1.0,
                "signed": False,
                "measurement": "active_energy",
                "tags": {}
            },
            "reactive_energy-positive" : {
                "type": "long",
                "register_address": 4690,
                "scale": 1.0,
                "signed": False,
                "measurement": "reactive_energy",
                "tags": {}
            },
            "active_energy-negative" : {
                "type": "long",
                "register_address": 4692,
                "scale": 1.0,
                "signed": False,
                "measurement": "active_energy",
                "tags": {}
            },
            "reactive_energy-negative" : {
                "type": "long",
                "register_address": 4694,
                "scale": 1.0,
                "signed": False,
                "measurement": "reactive_energy",
                "tags": {}
            },
        }
              
        logging.info(f"Initializing {self.__class__.__name__} class...")
        
        self.instrument = minimalmodbus.Instrument(port, SLAVE_ADDRESS, mode=minimalmodbus.MODE_RTU)
        self.instrument.serial.baudrate = BAUDRATE
        self.instrument.serial.parity = PARITY
        
        self.dict_modbus_data = DICT_MODBUS_DATA
        
        self.location = LOCATION
        
        return None
    
    def read_data(self,
        type: str,
        register_address: int,
        scale: float = 1.0,
        signed: bool = True,
    ) -> float:
        
        if type == 'long':
            data = self.instrument.read_long(register_address) * scale
        elif type == 'register':
            data = self.instrument.read_register(register_address, signed=signed) * scale
        else:
            logging.error(f"Type {type} not recognized")
            return None
        
        logging.debug(f"Register {register_address} | Type {type} | Scale {scale} | Signed {signed} | Data = {data}")
        return data
    
    def get_point_influxdb(self,
        measurement: str = None
    ) -> dict:
        
        assert measurement in self.dict_modbus_data.keys(), f"Measurement {measurement} not recognized among {self.dict_modbus_data.keys()}"
        
        type = self.dict_modbus_data[measurement]['type']
        register_address = self.dict_modbus_data[measurement]['register_address']
        scale = self.dict_modbus_data[measurement]['scale']
        signed = self.dict_modbus_data[measurement]['signed']
            
        data = self.read_data(type, register_address, scale, signed)
        
        if data is not None:
            return {
                'measurement': self.dict_modbus_data[measurement]['measurement'],
                'tags': {   
                    'location': self.location,
                    **self.dict_modbus_data[measurement]['tags']
                },
                'fields': {
                    'value': float(data),
                },
                'time': datetime.datetime.utcnow().isoformat(),
            }
        else:
            logging.error(f"Coudn't get data for register {register_address}")
            return None
        
    def get_all_points_influxdb(self) -> list:
        
        list_point = []
        for measurement in self.dict_modbus_data.keys():
            if "active_power" in measurement and "sign" not in measurement:
                sign = self.get_point_influxdb(f"{measurement}-sign")
                value = self.get_point_influxdb(measurement)
                if sign and value:
                    value['fields']['value'] = value['fields']['value'] * (-2*sign['fields']['value'] + 1)
                    list_point.append(value)
                    
            elif measurement == "active_energy-positive" or measurement == "reactive_energy-positive":
                positive = self.get_point_influxdb(f"{measurement}")
                negative = self.get_point_influxdb(f"{measurement.replace('positive', 'negative')}")
                if positive and negative:
                    positive['fields']['value'] = positive['fields']['value'] - negative['fields']['value']
                    list_point.append(positive)
                    
            elif "voltage" in measurement or "power_factor" in measurement:
                list_point.append(self.get_point_influxdb(measurement))
                
            else:
                if "sign" not in measurement and "energy" not in measurement:
                    logging.warning(f"Measurement {measurement} not supported.")
            
        return list_point
        
class Shelly3Em:
    
    def __init__(self,
        url: str,
        token: str,
    ) -> None:
        
        LIST_MESUREMENT = [
            "voltage",
            "active_power",
            "power_factor",
            "active_energy",
        ]
        
        DICT_MEASUREMENT_TO_FIELD = {
            "voltage": "voltage",
            "active_power": "power",
            "power_factor": "pf",
            "active_energy": "total",
        }
        
        logging.info(f"Initializing {self.__class__.__name__} class...")
        
        self.url = url
        self.token = token
        
        self._list_measurement = LIST_MESUREMENT
        self._dict_measurement_to_field = DICT_MEASUREMENT_TO_FIELD
        
        return None
    
    def get_data(self,
        id:str  
    ) -> dict:
        response = requests.post(
            self.url, 
            data={
                'auth_key': self.token, 
                'id': id
            }
        )
        
        if response.status_code == 200:
            return response.json()['data']['device_status']['emeters']
        else:
            logging.error(f"API call not successful. Status code: {response.status_code}, reason: {response.reason}. Full response: {response.json()}")
            return None

    def get_point_influxdb(self,
        id:str,
        location: str,
    ) -> list:
        data = self.get_data(id)
        
        list_point = []
        if data:
            for phase, emeter in enumerate(data):
                for measurement in self._list_measurement:
                    list_point.append({
                        'measurement': measurement,
                        'tags': {
                            'location': location,
                            'phase': f"L{phase+1}"
                        },
                        'fields': {
                            'value': float(emeter[self._dict_measurement_to_field[measurement]]),
                        },
                        'time': datetime.datetime.utcnow().isoformat(),
                    })
                    
        return list_point
                
class HuaweiPv:
    
    def __init__(self,
        username: str,
        password: str,
        subdomain: str,
        captcha_model_path: str,
    ) -> None:
        
        LOCATION = "Laboratory"
        
        logging.info(f"Initializing {self.__class__.__name__} class...")
        
        self.username = username
        self.password = password
        self.subdomain = subdomain
        self.captcha_model_path = captcha_model_path
        
        self.client = FusionSolarClient(
            self.username,
            self.password,
            self.subdomain,
            captcha_model_path=self.captcha_model_path)
        
        self.location = LOCATION
        
        return None
    
    def get_data(self) -> dict:
       
        try:
            stats = self.client.get_power_status()
            return {
                'current_power': stats.current_power_kw * 1000.0,
                'total_energy': stats.energy_kwh * 1000.0,
            }
        except Exception as e:
            logging.error(f"API call not working!: {e}")
            return None
        
    def get_point_influxdb(self) -> list:
        data = self.get_data()
        
        if data:
            return [
                {
                    'measurement': 'active_power',
                    'tags': {
                        'location': self.location,
                    },
                    'fields': {
                        'value': float(data['current_power']),
                    },
                    'time': datetime.datetime.utcnow().isoformat(),
                },
                {
                    'measurement': 'active_energy',
                    'tags': {
                        'location': self.location,
                    },
                    'fields': {
                        'value': float(data['total_energy']),
                    },
                    'time': datetime.datetime.utcnow().isoformat(),
                },
            ]
        else:
            logging.error(f"Coudn't get data for Huawei PV")
            return None