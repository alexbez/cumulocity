import  paho.mqtt.client as mqtt
import  time, random, threading
import  multiprocessing as mp
from decouple import config
import AHT20

aht20 = AHT20.AHT20()
print("Temperature/Humidity sensor is connected")

serverUrl = "mqtt.cumulocity.com"
clientId = "test_mqtt_python_client"
deviceName = "RaspberryPi 4 MQTT device"
serialNum = ""

tenant = config('tenant', default='')
username = config('username', default='')
password = config('password', default='')

task_queue = mp.Queue()

def getSerial():
# Extract serial from cpuinfo file
    cpuserial = "0000000000000000"
    try:
        f = open('/proc/cpuinfo','r')
#        print("'/proc/cpuinfo' opened")
        for line in f:
#            print(line)
            if line[0:6]=='Serial':
                cpuserial = line[line.find(": ")+2:]
#                print("CPU Serial=", cpuserial)
        f.close()
    except:
        cpuserial = "ERROR000000000"
    return cpuserial

def on_message(client, userdata, message):
    payload = message.payload.decode("utf-8")
    print(" < received message " + payload)
    if payload.startswith("510"):
        task_queue.put(perform_restart)

def perform_restart():
    print("Simulating device restart...")
    publish("s/us", "501,c8y_Restart", wait_for_ack = True);
    print("...restarting")
    time.sleep(3)
    publish("s/us", "503,c8y_Restart", wait_for_ack = True);
    print("...restart completed")

def send_measurement():
    print("Measuring temperature and humidity...")
    temperature = aht20.get_temperature()
    humidity = aht20.get_humidity()
    print(temperature, humidity)
    publish("s/us", "211,{}".format(temperature))
    publish("s/us", "200,c8y_Humidity,H,{}".format(humidity))

def publish(topic, message, wait_for_ack = False):
    QoS = 2 if wait_for_ack else 0
    message_info = client.publish(topic, message, QoS)
    if wait_for_ack:
        print(" > awaiting ACK for {}".format(message_info.mid))
        message_info.wait_for_publish()
        print(" < received ACK for {}".format(message_info.mid))

def on_publish(client, userdata, mid):
    print(" > published message: {}".format(mid))

def device_loop():
    while True:
        task_queue.put(send_measurement)
        time.sleep(30)

serialNum = getSerial()
print("Serial number: ", serialNum)

client = mqtt.Client(clientId)
client.username_pw_set(tenant + "/" + username, password)
client.on_message = on_message
client.on_publish = on_publish

print("Connecting to the server " + serverUrl)
client.connect(serverUrl)
client.loop_start()
print("Connected to the server, main loop started")
print("Registering the device " + deviceName)

publish("s/us", "100," + deviceName + ", c8y_MQTTDevice", wait_for_ack = True)
publish("s/us", "110," + serialNum + ", MQTT test model,Rev0.1")
publish("s/us", "114,c8y_Restart")
print("Device registered successfully")

client.subscribe("s/ds")
print("Devise subscribed to the channel")

device_loop_thread = threading.Thread(target = device_loop)
device_loop_thread.daemon = True
device_loop_thread.start()
print("Device loop started")

try:
    while True:
        task = task_queue.get()
        task()
except (KeyboardInterrupt, SystemExit):
    print("Received keyboard interrupt, quitting...")
    exit(0)
