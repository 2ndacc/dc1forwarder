# from socket import socket, AF_INET, SOCK_STREAM
import asyncio
import time
import json
from gmqtt import Client as MQTTClient
from datetime import datetime

MQTT_ADDR = '192.168.64.6'
MQTT_PORT = 1883
MQTT_USER = "test"
MQTT_PSWD = "12345678"

DISCOVER_PREFIX = "homeassistant"

def get_uuid():
    uuid = str(int(round(time.time() * 1000)))
    return uuid

class DC1ProtocolServer(asyncio.Protocol):
    def connection_made(self, transport):
        self.transport = transport
        self.heartbeat = True
        self.hbtimeout = 0
        self.buf = b""
        self.mac = ""
        print('{} New Connection'.format(self.getinfo()))

    def getinfo(self):
        return f"{datetime.now():%m-%d %H:%M} {self.transport.get_extra_info('peername')[0]} {self.mac}"

    def sensor_path(self, sensor_type):
        return f"{DISCOVER_PREFIX}/sensor/{self.nodename}/{sensor_type.lower()}/"

    def discovery_sensor(self, sensor_type, unit, icon):
        DISCOVER_SENSOR = {
            "name": f"DC1_{self.snodename}_{sensor_type}",
            "uniq_id": f"{self.nodename}_{sensor_type[0]}",
            "state_topic": self.sensor_path(sensor_type)+'state',
            "unit_of_measurement": f"{unit}",
            "icon": f"mdi:{icon}",
            "value_template": "{{ value_json.%s }}" % (sensor_type.lower())
            }
        return self.mqtt.publish(self.sensor_path(sensor_type)+'config', json.dumps(DISCOVER_SENSOR), retain=True)

    def publish_sensor(self, sensor_type, value):
        SENSOR_VALUE = { sensor_type.lower():value }
        return self.mqtt.publish(self.sensor_path(sensor_type)+'state', json.dumps(SENSOR_VALUE), retain=True)

    def switch_path(self, id):
        return f"{DISCOVER_PREFIX}/switch/{self.nodename}/switch_{id}/"

    def cmd_path(self):
        return f'device/dc1/{self.nodename}/set'

    def pl_cmd(self, id, on):
        return f"set socket {self.nodename} {id} {on}"

    def discovery_switch(self, id):
        DISCOVER_SWITCH = {
            "name": f"DC1_{self.snodename}_Switch_{id}",
            "uniq_id": f"{self.nodename}_s{id}",
            "stat_t": self.switch_path(id)+'state',
            "cmd_t": self.cmd_path(),
            "pl_on": self.pl_cmd(id, 1),
            "pl_off": self.pl_cmd(id, 0)
            }
        return self.mqtt.publish(self.switch_path(id)+'config', json.dumps(DISCOVER_SWITCH), retain=True)

    def publish_switch(self, id, on):
        return self.mqtt.publish(self.switch_path(id)+'state', self.pl_cmd(id, on), retain=True)

    def mqtt_on_connect(self, client, flags, rc, properties):
        client.subscribe(self.cmd_path(), qos=0)
        self.discovery_sensor('Power', 'W', 'power')
        self.discovery_sensor('Voltage', 'V', 'flash')
        self.discovery_switch(0)
        self.discovery_switch(1)
        self.discovery_switch(2)
        self.discovery_switch(3)

    def set_switch(self, id, on):
        id = int(id)
        plug_status = "%04d" % self.result['status']
        plug_status = '%s%s%s'%(plug_status[:id],on,plug_status[id+1:])
        plug_status = int(plug_status)
        payload = bytes('{"action":"datapoint=","params":{"status":' + str(plug_status) + '},"uuid":"' + get_uuid() + '","auth":""}\n', encoding="utf8")
        print("{} Hass set switch %d to %s" % (self.getinfo(), id, on))
        self.transport.write(payload)
        return plug_status

    def mqtt_on_message(self, client, topic, payload, qos, properties):
        try:
            cmd = payload.decode().split(' ')
            id = cmd[3]
            on = cmd[4]
            self.result['status'] = self.set_switch(id, on)
        except:
            raise
        
    def mqtt_init(self):
        self.mqtt = MQTTClient(f"DC1 Forwarder {self.mac}")
        self.mqtt.set_auth_credentials(MQTT_USER, MQTT_PSWD)
        self.mqtt.on_connect = self.mqtt_on_connect
        self.mqtt.on_message = self.mqtt_on_message
        asyncio.create_task(self.mqtt.connect(MQTT_ADDR, MQTT_PORT))

    def mqtt_pub(self):
        self.publish_sensor('Power', self.result['P'])
        self.publish_sensor('Voltage', self.result['V'])

        plug_status = "%04d" % self.result['status']
        self.publish_switch(0, plug_status[0])
        self.publish_switch(1, plug_status[1])
        self.publish_switch(2, plug_status[2])
        self.publish_switch(3, plug_status[3])

    def data_received(self, data):
        self.hbtimeout = 0
        self.buf += data
        while b'\n' in self.buf:
            data, self.buf = tuple(self.buf.split(b'\n', 1))
            message = data.decode()
            try:
                data = json.loads(message)
            except:
                print("{} {}".format(self.getinfo(), message))
                return
            if data.get('action') == "activate=":
                # {'action': 'activate=', 'uuid': 'activate=687', 'auth': '', 'params': {'device_type': 'PLUG_DC1_7', 'mac': '--:--:--:--:--:--'}}
                self.mac = data['params']['mac']
                self.nodename = self.mac.replace(':', '')
                self.snodename = self.nodename[-4:]
                print('{} Session started'.format(self.getinfo()))
                self.mqtt_init()
                asyncio.create_task(self.do_heart_beat())
            elif data.get('msg') == "get datapoint success":
                # {'uuid': 'T1641262103360', 'status': 200, 'result': {'status': 1110, 'I': 0, 'V': 0, 'P': 0}, 'msg': 'get datapoint success'}
                self.result = data['result']
                self.mqtt_pub()
            elif data.get('msg') == "set datapoint success":
                # {'uuid': '1641276757724', 'status': 200, 'result': {'status': 101}, 'msg': 'set datapoint success'}
                self.result['status'] = data['result']['status']
                self.mqtt_pub()
            else:
                # {'action': 'kWh+', 'uuid': 'kWh+64d62447', 'auth': '', 'params': {'detalKWh': 15500}}
                print("{} {}".format(self.getinfo(), data))

    async def do_heart_beat(self):
        while self.heartbeat:
            await asyncio.sleep(1)
            heart_msg = bytes('{"uuid":"T' + get_uuid() + '","params":{},"auth":"","action":"datapoint"}\n', encoding="utf8")
            self.transport.write(heart_msg)
            self.hbtimeout += 1
            if self.hbtimeout > 30:
                self.transport.close()

    def connection_lost(self, exc):
        self.heartbeat = False
        asyncio.create_task(self.mqtt.disconnect())
        print('{} Session ended'.format(self.getinfo()))


loop = asyncio.new_event_loop()
coro = loop.create_server(DC1ProtocolServer, '0.0.0.0', 8000)
server = loop.run_until_complete(coro)
try:
    loop.run_forever()
except KeyboardInterrupt:
    pass

# Close the server
server.close()
loop.run_until_complete(server.wait_closed())
loop.close()