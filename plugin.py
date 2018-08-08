#           MQTT discovery plugin
#           Code copied from / inspired by:
#             https://github.com/nabovarme/MeterLogger/blob/master/user/kamstrup/kmp.c
#             https://github.com/bsdphk/PyKamstrup/blob/master/kamstrup.py
#             https://github.com/RvMp/multical402-4-domoticz/blob/master/multical402-4-domoticz.py
#
"""
<plugin key="KamstrupSonoffMQTT" name="Kamstrup Meter (Sonoff-Tasmota MQTT)" version="0.0.1">
    <description>
      Kamstrup meter, connected to Sonoff-Tasmota device
      MQTT discovery, compatible with home-assistant.<br/><br/>
      Specify MQTT server and port.<br/>
      <br/>
    </description>
    <params>
        <param field="Address" label="MQTT Server address" width="300px" required="true" default="127.0.0.1"/>
        <param field="Port" label="Port" width="300px" required="true" default="1883"/>
        <!-- <param field="Mode5" label="MQTT QoS" width="300px" default="0"/> -->
        <param field="Username" label="Username" width="300px"/>
        <param field="Password" label="Password" width="300px"/>
        <!-- <param field="Mode1" label="CA Filename" width="300px"/> -->

        <param field="Mode2" label="Device topics (comma separated)" width="300px" default="tasmota/sonoff_0FAC39"/>

        <param field="Mode3" label="Options" width="300px"/>
        <param field="Mode6" label="Debug" width="75px">
            <options>
                <option label="Extra verbose" value="Verbose+"/>
                <option label="Verbose" value="Verbose"/>
                <option label="True" value="Debug"/>
                <option label="False" value="Normal"  default="true" />
            </options>
        </param>
    </params>
</plugin>
"""
import Domoticz
import math
from datetime import datetime
from itertools import count, filterfalse
import json
import re
import time
import traceback

class MqttClient:
    Address = ""
    Port = ""
    mqttConn = None
    isConnected = False
    mqttConnectedCb = None
    mqttDisconnectedCb = None
    mqttPublishCb = None

    def __init__(self, destination, port, mqttConnectedCb, mqttDisconnectedCb, mqttPublishCb, mqttSubackCb):
        Domoticz.Debug("MqttClient::__init__")
        self.Address = destination
        self.Port = port
        self.mqttConnectedCb = mqttConnectedCb
        self.mqttDisconnectedCb = mqttDisconnectedCb
        self.mqttPublishCb = mqttPublishCb
        self.mqttSubackCb = mqttSubackCb
        self.Open()

    def __str__(self):
        Domoticz.Debug("MqttClient::__str__")
        if (self.mqttConn != None):
            return str(self.mqttConn)
        else:
            return "None"

    def Open(self):
        Domoticz.Debug("MqttClient::Open")
        if (self.mqttConn != None):
            self.Close()
        self.isConnected = False
        self.mqttConn = Domoticz.Connection(Name=self.Address, Transport="TCP/IP", Protocol="MQTT", Address=self.Address, Port=self.Port)
        self.mqttConn.Connect()

    def Connect(self):
        Domoticz.Debug("MqttClient::Connect")
        if (self.mqttConn == None):
            self.Open()
        else:
            ID = 'Domoticz_'+Parameters['Key']+'_'+str(Parameters['HardwareID'])+'_'+str(int(time.time()))
            Domoticz.Log("MQTT CONNECT ID: '" + ID + "'")
            self.mqttConn.Send({'Verb': 'CONNECT', 'ID': ID})

    def Ping(self):
        Domoticz.Debug("MqttClient::Ping")
        if (self.mqttConn == None or not self.isConnected):
            self.Open()
        else:
            self.mqttConn.Send({'Verb': 'PING'})

    def Publish(self, topic, payload, retain = 0):
        if isinstance(payload, bytearray):
            Domoticz.Debug("MqttClient::Publish " + topic + " (" + ''.join('{:02x}'.format(x) for x in payload) + ")")
        else:
            Domoticz.Debug("MqttClient::Publish " + topic + " (" + payload + ")")
        if (self.mqttConn == None or not self.isConnected):
            self.Open()
        else:
            self.mqttConn.Send({'Verb': 'PUBLISH', 'Topic': topic, 'Payload': payload, 'Retain': retain})

    def Subscribe(self, topics):
        Domoticz.Debug("MqttClient::Subscribe")
        subscriptionlist = []
        for topic in topics:
            subscriptionlist.append({'Topic':topic, 'QoS':0})
        if (self.mqttConn == None or not self.isConnected):
            self.Open()
        else:
            self.mqttConn.Send({'Verb': 'SUBSCRIBE', 'Topics': subscriptionlist})

    def Close(self):
        Domoticz.Log("MqttClient::Close")
        #TODO: Disconnect from server
        self.mqttConn = None
        self.isConnected = False

    def onConnect(self, Connection, Status, Description):
        Domoticz.Debug("MqttClient::onConnect")
        if (Status == 0):
            Domoticz.Log("Successful connect to: "+Connection.Address+":"+Connection.Port)
            self.Connect()
        else:
            Domoticz.Log("Failed to connect to: "+Connection.Address+":"+Connection.Port+", Description: "+Description)

    def onDisconnect(self, Connection):
        Domoticz.Log("MqttClient::onDisonnect Disconnected from: "+Connection.Address+":"+Connection.Port)
        self.Close()
        # TODO: Reconnect?
        if self.mqttDisconnectedCb != None:
            self.mqttDisconnectedCb()

    def onMessage(self, Connection, Data):
        topic = ''
        if 'Topic' in Data:
            topic = Data['Topic']
        payloadStr = ''
        if 'Payload' in Data:
            payloadStr = Data['Payload'].decode('utf8','replace')
            payloadStr = str(payloadStr.encode('unicode_escape'))
        #Domoticz.Debug("MqttClient::onMessage called for connection: '"+Connection.Name+"' type:'"+Data['Verb']+"' topic:'"+topic+"' payload:'" + payloadStr + "'")

        if Data['Verb'] == "CONNACK":
            self.isConnected = True
            if self.mqttConnectedCb != None:
                self.mqttConnectedCb()

        if Data['Verb'] == "SUBACK":
            if self.mqttSubackCb != None:
                self.mqttSubackCb()

        if Data['Verb'] == "PUBLISH":
            if self.mqttPublishCb != None:
                self.mqttPublishCb(topic, Data['Payload'])

class BasePlugin:
    # MQTT settings
    mqttClient = None
    mqttserveraddress = ""
    mqttserverport = ""
    debugging = "Normal"
    cachedDeviceNames = {}
    lastDeviceResponse = {}
    readQueue = {}

    options = {"updateRSSI":False,             # Store Tasmota RSSI
               "updateVCC":False}              # Store Tasmota VCC as battery level

    def copyDevices(self):
        for k, Device in Devices.items():
            self.cachedDeviceNames[k]=Device.Name

    def deviceStr(self, unit):
        name = "<UNKNOWN>"
        if unit in Devices:
            name = Devices[unit].Name
        return format(unit, '03d') + "/" + name

    def getUnit(self, device):
        unit = -1
        for k, dev in Devices.items():
            if dev == device:
                unit = k
        return unit

    def onStart(self):

        # Parse options
        self.debugging = Parameters["Mode6"]
        DumpConfigToLog()
        if self.debugging == "Verbose+":
            Domoticz.Debugging(2+4+8+16+64)
        if self.debugging == "Verbose":
            Domoticz.Debugging(2+4+8+16+64)
        if self.debugging == "Debug":
            Domoticz.Debugging(2+4+8)
        self.mqttserveraddress = Parameters["Address"].replace(" ", "")
        self.mqttserverport = Parameters["Port"].replace(" ", "")
        self.devicetopics = Parameters["Mode2"].split(',')

        options = ""
        try:
            options = json.loads(Parameters["Mode3"])
        except ValueError:
            options = Parameters["Mode3"]

        if type(options) == str or type(options) == int:
            Domoticz.Log("Warning: could not load plugin options '" + Parameters["Mode3"] + "' as JSON object")
        elif type(options) == dict:
            self.options.add(options)
        Domoticz.Log("Plugin options: " + str(self.options))

        # Enable heartbeat
        Domoticz.Heartbeat(10)

        # Connect to MQTT server
        self.prefixpos = 0
        self.topicpos = 0
        self.mqttClient = MqttClient(self.mqttserveraddress, self.mqttserverport, self.onMQTTConnected, self.onMQTTDisconnected, self.onMQTTPublish, self.onMQTTSubscribed)

        #for devicetopic in self.devicetopics:
        #    self.updateDeviceSettings('Meter', devicetopic)

        self.copyDevices()

    def onConnect(self, Connection, Status, Description):
        self.mqttClient.onConnect(Connection, Status, Description)

    def onDisconnect(self, Connection):
        self.mqttClient.onDisconnect(Connection)

    def onMessage(self, Connection, Data):
        self.mqttClient.onMessage(Connection, Data)

    def onMQTTConnected(self):
        Domoticz.Debug("onMQTTConnected")
        self.mqttClient.Subscribe(self.getTopics())

    def onMQTTDisconnected(self):
        Domoticz.Debug("onMQTTDisconnected")

    def onMQTTPublish(self, topic, rawmessage):
        message = ""
        try:
            message = json.loads(rawmessage.decode('utf8'))
        except ValueError:
            try:
                message = rawmessage.decode('utf8')
            except UnicodeDecodeError:
                pass
        topiclist = topic.split('/')
        if self.debugging == "Verbose" or self.debugging == "Verbose+":
            DumpMQTTMessageToLog(topic, rawmessage, 'onMQTTPublish: ')

        if 1 > 0:
            matchingDevices = self.getDevices(topic=topic)
            if not matchingDevices:
                self.addKMPDevice(topic, message)
            else:
                for device in matchingDevices:
                    # Try to update availability
                    self.updateAvailability(device, topic, message)

                    # Try to update tasmota status
                    self.updateTasmotaStatus(device, topic, message)

                    # Try to update register values
                    self.updateKMPDevice(device, topic, message)

            # Special handling of Tasmota STATE message
        #    topic2, matches = re.subn(r"\/STATUS\d+$", '/STATE', topic)
        #    if matches > 0:
        #        topic2, matches = re.subn(r"\/stat\/", '/tele/', topic2)
        #        if matches > 0:
        #            matchingDevices = self.getDevices(topic=topic2)
        #            for device in matchingDevices:
                        # Try to update tasmota settings
        #                self.updateTasmotaSettings(device, topic, message)

    def onMQTTSubscribed(self):
        # (Re)subscribed, refresh device info
        Domoticz.Debug("onMQTTSubscribed");
        matchingDevices = self.getDevices(hasconfigkey='tasmota_tele_topic')
        topics = set()
        for device in matchingDevices:
            # Refresh Tasmota specific data
            try:
                configdict = json.loads(device.Options['config'])
                cmnd_topic = configdict['cmnd_topic']
                if cmnd_topic not in topics: self.refreshConfiguration(cmnd_topic)
                topics.add(cmnd_topic)
            except (ValueError, KeyError, TypeError) as e:
                #Domoticz.Error("onMQTTSubscribed: Error: " + str(e))
                Domoticz.Error(traceback.format_exc())

    def onCommand(self, Unit, Command, Level, sColor):
        Domoticz.Log("onCommand " + self.deviceStr(Unit) + ": Command: '" + str(Command) + "', Level: " + str(Level) + ", Color:" + str(sColor));

    def onDeviceAdded(self, Unit):
        Domoticz.Log("onDeviceAdded " + self.deviceStr(Unit))
        self.copyDevices()
        #TODO: Update subscribed topics

    def onDeviceModified(self, Unit):
        Domoticz.Log("onDeviceModified " + self.deviceStr(Unit))

        if Unit in Devices and Devices[Unit].Name != self.cachedDeviceNames[Unit]:
            Domoticz.Log("Device name changed, new name: " + Devices[Unit].Name + ", old name: " + self.cachedDeviceNames[Unit])
            Device = Devices[Unit]

            try:
                configdict = json.loads(Device.Options['config'])
                if "tasmota_tele_topic" in configdict and Device.SwitchType != 9: # Do not set friendly name for button, they don't have their own friendly name
                    #Tasmota device!
                    device_nbr = ''
                    m = re.match(r".*_(\d)$", str(Device.Options['devicename']))
                    if m:
                        device_nbr = m.group(1)
                    cmnd_topic = configdict['cmnd_topic']
                    self.mqttClient.Publish(cmnd_topic+'/FriendlyName'+str(device_nbr), Device.Name)
            except (ValueError, KeyError, TypeError) as e:
                Domoticz.Debug("onDeviceModified: Error: " + str(e))
                pass

        self.copyDevices()

    def onDeviceRemoved(self, Unit):
        Domoticz.Log("onDeviceRemoved " + self.deviceStr(Unit))
        self.copyDevices()
        #TODO: Update subscribed topics

    def onHeartbeat(self):
        Domoticz.Debug("Heartbeating...")

        # Reconnect if connection has dropped
        if self.mqttClient.mqttConn is None or (not self.mqttClient.mqttConn.Connecting() and not self.mqttClient.mqttConn.Connected() or not self.mqttClient.isConnected):
            Domoticz.Debug("Reconnecting")
            self.mqttClient.Open()
        else:
            self.mqttClient.Ping()

            for devicetopic in self.devicetopics:
                cmnd_topic = devicetopic+'/cmnd'
                matchingDevices = self.getDevices(topic=cmnd_topic)
                if not matchingDevices:
                    Domoticz.Log("Meter with topic '" + devicetopic + "' is unknown, trying to identify meter")
                    self.getType(cmnd_topic)

            for k, device in Devices.items():
                if not k in self.readQueue or not self.readQueue[k] or not k in self.lastDeviceResponse or time.time()-self.lastDeviceResponse[k] > 60:
                    configdict = json.loads(device.Options['config'])
                    if configdict['meter_type'] == 'kamstrup_402_heat':
                        self.readQueue[k] = list(self.kamstrup_402_var.keys())
                        #self.setClock(device, 180808, 112500)
                        nbr = self.readQueue[k].pop()
                        self.getRegister(device, nbr)

    # Pull configuration and status from tasmota device
    def refreshConfiguration(self, Topic):
        Domoticz.Debug("refreshConfiguration for device with topic: '" + Topic + "'");
        # Refresh relay / dimmer configuration
        self.mqttClient.Publish(Topic+"/Status",'11')
        # Refresh sensor configuration
        #self.mqttClient.Publish(Topic+"/Status",'10')
        # Refresh IP configuration
        self.mqttClient.Publish(Topic+"/Status",'5')

    # Returns list of topics to subscribe to
    def getTopics(self):
        topics = set()
        for devicetopic in self.devicetopics:
            topics.add(devicetopic + '/tele/RESULT')

        for key,Device in Devices.items():
            #Domoticz.Debug("getTopics: '" + str(Device.Options) +"'")
            try:
                configdict = json.loads(Device.Options['config'])
                #Domoticz.Debug("getTopics: '" + str(configdict) +"'")
                for key, value in configdict.items():
                    #Domoticz.Debug("getTopics: key:'" + str(key) +"' value: '" + str(value) + "'")
                    try:
                        #if key.endswith('_topic'):
                        if key == 'availability_topic' or key == 'state_topic' or key == 'result_topic':
                            topics.add(value)
                    except (TypeError) as e:
                        Domoticz.Error("getTopics: Error: " + str(e))
                        pass
                if "tasmota_tele_topic" in configdict:
                    #Subscribe to all Tasmota state topics
                    state_topic = re.sub(r"^tele\/", "stat/", configdict['tasmota_tele_topic']) # Replace tele with stat
                    state_topic = re.sub(r"\/tele\/", "/stat/", state_topic) # Replace tele with stat
                    state_topic = re.sub(r"\/STATE", "/#", state_topic) # Replace '/STATE' with /#
                    topics.add(state_topic)
            except (ValueError, KeyError, TypeError) as e:
                Domoticz.Error("getTopics: Error: " + str(e))
                pass
        Domoticz.Debug("getTopics: '" + str(topics) +"'")
        Domoticz.Log("getTopics: '" + str(topics) +"'")
        return list(topics)

    # Returns list of matching devices
    def getDevices(self, key='', configkey='', hasconfigkey='', value='', config='', topic='', type='', channel=''):
        Domoticz.Debug("getDevices key: '" + key + "' configkey: '" + configkey + "' hasconfigkey: '" + hasconfigkey + "' value: '" + value + "' config: '" + config + "' topic: '" + topic + "'")
        matchingDevices = set()
        if key != '':
            for k, Device in Devices.items():
                try:
                    if Device.Options[key] == value:
                        matchingDevices.add(Device)
                except (ValueError, KeyError) as e:
                    pass
        if configkey != '':
            for k, Device in Devices.items():
                try:
                    configdict = json.loads(Device.Options['config'])
                    if configdict[configkey] == value:
                        matchingDevices.add(Device)
                except (ValueError, KeyError) as e:
                    pass
        elif hasconfigkey != '':
            for k, Device in Devices.items():
                try:
                    configdict = json.loads(Device.Options['config'])
                    if hasconfigkey in configdict:
                        matchingDevices.add(Device)
                except (ValueError, KeyError) as e:
                    pass
        elif config != '':
            for k, Device in Devices.items():
                try:
                    if Device.Options['config'] == config:
                        matchingDevices.add(Device)
                except KeyError:
                    pass
        elif topic != '':
            for k, Device in Devices.items():
                try:
                    configdict = json.loads(Device.Options['config'])
                    for key, value in configdict.items():
                        if value == topic:
                            matchingDevices.add(Device)
                except (ValueError, KeyError) as e:
                    pass
        Domoticz.Debug("getDevices found " + str(len(matchingDevices)) + " devices")
        return list(matchingDevices)

    def makeDevice(self, devicename, TypeName, switchTypeDomoticz, config):
        iUnit = next(filterfalse(set(Devices).__contains__, count(1))) # First unused 'Unit'

        Domoticz.Log("Creating device with unit: " + str(iUnit));

        Options = {'config':json.dumps(config),'devicename':devicename}
        DeviceName = 'Meter'
        Domoticz.Device(Name=DeviceName, Unit=iUnit, TypeName=TypeName, Switchtype=switchTypeDomoticz, Options=Options, Used=True).Create()

    def updateDeviceSettings(self, devicename, basetopic, TypeName, MeterType):
        config = {"meter_type": MeterType, "availability_topic": basetopic+"/tele/LWT", "payload_available": "Online", "payload_not_available": "Offline", "state_topic": basetopic+"/stat/RESULT", "result_topic": basetopic+"/tele/RESULT", "tasmota_tele_topic": basetopic+"/tele/STATE", "cmnd_topic": basetopic+"/cmnd"}
        #Domoticz.Debug("updateDeviceSettings devicename: '" + devicename + "' devicetype: '" + devicetype + "' config: '" + str(config) + "'")

        Type = 0
        Subtype = 0
        switchTypeDomoticz = 0 # OnOff
        
        matchingDevices = self.getDevices(key='devicename', value=devicename)
        if len(matchingDevices) == 0:
            Domoticz.Log("updateDeviceSettings: Did not find device with key='devicename', value = '" +  devicename + "'")
            # Unknown device
            Domoticz.Log("updateDeviceSettings: TypeName: '" + TypeName + "' Type: " + str(Type))
            self.makeDevice(devicename, TypeName, switchTypeDomoticz, config)
            # Update subscription list
            self.mqttClient.Subscribe(self.getTopics())
        else:
            # TODO: What do if len(matchingDevices) > 1?
            device = matchingDevices[0]
            oldconfigdict = {}
            try:
                oldconfigdict = json.loads(device.Options['config'])
            except (ValueError, KeyError, TypeError) as e:
                pass
            if oldconfigdict != config:
                Domoticz.Log("updateDeviceSettings: " + self.deviceStr(self.getUnit(device)) + ": Device settings not matching, updating Options['config']")
                Domoticz.Log("updateDeviceSettings: device.Options['config']: " + str(oldconfigdict) + " -> " + str(config))
                nValue = device.nValue
                sValue = device.sValue
                Options = dict(device.Options)
                Options['config'] = json.dumps(config)
                device.Update(nValue=nValue, sValue=sValue, Options=Options, SuppressTriggers=True)
                self.copyDevices()

    def updateAvailability(self, device, topic, message):
        TimedOut=0
        updatedevice = False

        try:
            devicetopics=[]
            configdict = json.loads(device.Options['config'])
            for key, value in configdict.items():
                if value == topic:
                    devicetopics.append(key)
            if "availability_topic" in devicetopics:
                Domoticz.Debug("Got availability_topic")
                payload = message
                if payload == configdict["payload_available"]:
                    updatedevice = True
                    TimedOut = 0
                if payload == configdict["payload_not_available"]:
                    updatedevice = True
                    TimedOut = 1
                Domoticz.Debug("TimedOut: '" + str(TimedOut) + "'")
        except (ValueError, KeyError) as e:
            pass

        if updatedevice:
            nValue = device.nValue
            sValue = device.sValue
            Domoticz.Log(self.deviceStr(self.getUnit(device)) + ": Setting TimedOut: '" + str(TimedOut) + "'")
            device.Update(nValue=nValue, sValue=sValue, TimedOut=TimedOut, SuppressTriggers=True)
            self.copyDevices()

    def updateTasmotaStatus(self, device, topic, message):
        #Domoticz.Debug("updateTasmotaStatus topic: '" + topic + "' message: '" + str(message) + "'")
        nValue = device.nValue
        sValue = device.sValue
        updatedevice = False
        Vcc = 0
        RSSI = 0

        try:
            devicetopics=[]
            configdict = json.loads(device.Options['config'])
            for key, value in configdict.items():
                if value == topic:
                    devicetopics.append(key)
            if "tasmota_tele_topic" in devicetopics:
                Domoticz.Debug("Got tasmota_tele_topic")
                if "Vcc" in message and self.options['updateVCC']:
                    Vcc = int(message["Vcc"]*10)
                    Domoticz.Debug("Set battery level to: " + str(Vcc) + " was:" + str(device.BatteryLevel))
                    updatedevice = True
                if "Wifi" in message and "RSSI" in message["Wifi"] and self.options['updateRSSI']:
                    RSSI = int(message["Wifi"]["RSSI"])
                    Domoticz.Debug("Set SignalLevel to: " + str(RSSI) + " was:" + str(device.SignalLevel))
                    updatedevice = True
            if updatedevice and (device.SignalLevel != RSSI or device.BatteryLevel != Vcc):
                Domoticz.Log(self.deviceStr(self.getUnit(device)) + ": Setting SignalLevel: '" + str(RSSI) + "', BatteryLevel: '" + str(Vcc) + "'")
                device.Update(nValue=nValue, sValue=sValue, SignalLevel=RSSI, BatteryLevel=Vcc, SuppressTriggers=True)
                self.copyDevices()
        except (ValueError, KeyError) as e:
            pass

    def updateTasmotaSettings(self, device, topic, message):
        Domoticz.Debug("updateTasmotaSettings " + self.deviceStr(self.getUnit(device)) + " topic: '" + topic + "' message: '" + str(message) + "'")
        nValue = device.nValue
        sValue = device.sValue
        updatedevice = False
        IPAddress = ""
        Description = ""

        try:
            devicetopics=[]
            configdict = json.loads(device.Options['config'])
            if topic.endswith('STATUS5'):
                if "StatusNET" in message and "IPAddress" in message["StatusNET"]:
                    IPAddress = message["StatusNET"]["IPAddress"]
                    Description = "IP: " + IPAddress + ", Topic: " + configdict['cmnd_topic']
                    updatedevice = True
            if updatedevice and (device.Description != Description):
                Domoticz.Log("updateTasmotaSettings updating description from: '" + device.Description + "' to: '" + Description + "'")
                device.Update(nValue=nValue, sValue=sValue, Description=Description, SuppressTriggers=True)
                self.copyDevices()
        except (ValueError, KeyError) as e:
            pass

    def addKMPDevice(self, topic, message):
        basetopic = re.sub(r"\/tele\/RESULT", "", topic) # Remove '/tele/RESULT'
        if basetopic in self.devicetopics:
            if "SerialReceived" in message:
                s = message["SerialReceived"]
                if s == "06": # Acknowledge
                    pass
                else: # Parse KMP message
                    b = self.recv(s)
                    if b == None:
                        pass
                    elif b[0] == 0x01:   # GetType
                        Domoticz.Log("addKMPDevice: GetType response:")
                        Domoticz.Log("b: " + ''.join('{:02x} '.format(x) for x in b))
                        meterType = b[1]<<8 | b[2]
                        if meterType == 0x1101: # MC 402 – Heat
                            self.updateDeviceSettings('Meter', basetopic, 'kWh', 'kamstrup_402_heat')
                        else:
                            Domoticz.Log("Unknown Meter Type: "+'{:04x} '.format(meterType))

    def updateKMPDevice(self, device, topic, message):
        devicetopics=[]
        configdict = json.loads(device.Options['config'])
        for key, value in configdict.items():
            if value == topic:
                devicetopics.append(key)
        if "result_topic" in devicetopics:
            Domoticz.Debug("Got result_topic")
            if "SerialReceived" in message:
                self.lastDeviceResponse[self.getUnit(device)] = time.time()
                s = message["SerialReceived"]
                if s == "06": # Acknowledge
                    Domoticz.Log("Got acknowledge: '" + s + "'")
                else: # Parse KMP message
                    b = self.recv(s)
                    if b == None:
                        pass
                    elif b[0] == 0x01:   # GetType
                        Domoticz.Log("GetType response:")
                        Domoticz.Log("b: " + ''.join('{:02x} '.format(x) for x in b))
                    elif b[0] == 0x02: # GetSerialNo
                        Domoticz.Log("GetSerialNo response:")
                        Domoticz.Log("b: " + ''.join('{:02x} '.format(x) for x in b))
                    elif b[0] == 0x09: # SetClock
                        Domoticz.Log("SetClock response:")
                        Domoticz.Log("b: " + ''.join('{:02x} '.format(x) for x in b))
                    elif b[0] == 0x10: # GetRegister
                        (reg, x, u) = self.readvar(b);
                        if True:
                            # Debug print
                            s = ""
                            for i in b[:3]:
                                s += " %02x" % i
                            s += " |"
                            for i in b[3:6]:
                                s += " %02x" % i
                            s += " |"
                            for i in b[6:]:
                                s += " %02x" % i

                            regname = 'UNKNOWN'
                            if reg in self.kamstrup_402_var: regname = self.kamstrup_402_var[reg]
                            Domoticz.Debug(s + ' : ' + str(reg) + '(' + regname + ')' + '='+ str(x) + ' ' + self.units[b[3]])

                        self.updateKMPRegister(device, reg, x, u)
                    else:
                        Domoticz.Log("Unknown response:")
                        Domoticz.Log("b: " + ''.join('{:02x} '.format(x) for x in b))
                if (self.readQueue[self.getUnit(device)]):
                    # Request next register
                    reg = self.readQueue[self.getUnit(device)].pop()
                    self.getRegister(device, reg)

    def updateKMPRegister(self, device, reg, x, u):
        nValue = device.nValue
        sValue = device.sValue
        updatedevice = False
        if reg == 60: # TODO get from table..
            sValues = [x.strip() for x in sValue.split(';')]
            sValues[1] = str(x*1000*1000)
            updatedevice = True
            sValue=str(sValues[0] + '; ' + sValues[1])
        if reg == 80: # TODO get from table..
            sValues = [x.strip() for x in sValue.split(';')]
            sValues[0] = str(x*1000)
            updatedevice = True
            sValue=str(sValues[0] + '; ' + sValues[1])
        if updatedevice and (nValue != device.nValue or sValue != device.sValue):
            Domoticz.Log(self.deviceStr(self.getUnit(device)) + " 'Setting nValue: " + str(device.nValue) + "->" + str(nValue) + ", sValue: '" + str(device.sValue) + "'->'" + str(sValue) + "'")
            device.Update(nValue=nValue, sValue=sValue)
            self.copyDevices()

    units = {
        0: '', 1: 'Wh', 2: 'kWh', 3: 'MWh', 4: 'GWh', 5: 'j', 6: 'kj', 7: 'Mj',
        8: 'Gj', 9: 'Cal', 10: 'kCal', 11: 'Mcal', 12: 'Gcal', 13: 'varh',
        14: 'kvarh', 15: 'Mvarh', 16: 'Gvarh', 17: 'VAh', 18: 'kVAh',
        19: 'MVAh', 20: 'GVAh', 21: 'kW', 22: 'kW', 23: 'MW', 24: 'GW',
        25: 'kvar', 26: 'kvar', 27: 'Mvar', 28: 'Gvar', 29: 'VA', 30: 'kVA',
        31: 'MVA', 32: 'GVA', 33: 'V', 34: 'A', 35: 'kV',36: 'kA', 37: 'C',
        38: 'K', 39: 'l', 40: 'm3', 41: 'l/h', 42: 'm3/h', 43: 'm3xC',
        44: 'ton', 45: 'ton/h', 46: 'h', 47: 'hh:mm:ss', 48: 'yy:mm:dd',
        49: 'yyyy:mm:dd', 50: 'mm:dd', 51: '', 52: 'bar', 53: 'RTC',
        54: 'ASCII', 55: 'm3 x 10', 56: 'ton x 10', 57: 'GJ x 10',
        58: 'minutes', 59: 'Bitfield', 60: 's', 61: 'ms', 62: 'days',
        63: 'RTC-Q', 64: 'Datetime'
    }
    
    kamstrup_402_var = {                # Decimal Number in Command
        0x003C: "Heat Energy (E1)",         #60
        0x0050: "Power",                   #80
        #0x0056: "Temp1",                   #86
        #0x0057: "Temp2",                   #87
        #0x0059: "Tempdiff",                #89
        #0x004A: "Flow",                    #74
        #0x0044: "Volume",                  #68
        #0x008D: "MinFlow_M",               #141
        #0x008B: "MaxFlow_M",               #139
        #0x008C: "MinFlowDate_M",           #140
        #0x008A: "MaxFlowDate_M",           #138
        #0x0091: "MinPower_M",              #145
        #0x008F: "MaxPower_M",              #143
        #0x0095: "AvgTemp1_M",              #149
        #0x0096: "AvgTemp2_M",              #150
        #0x0090: "MinPowerDate_M",          #144
        #0x008E: "MaxPowerDate_M",          #142
        #0x007E: "MinFlow_Y",               #126
        #0x007C: "MaxFlow_Y",               #124
        #0x007D: "MinFlowDate_Y",           #125
        #0x007B: "MaxFlowDate_Y",           #123
        #0x0082: "MinPower_Y",              #130
        #0x0080: "MaxPower_Y",              #128
        #0x0092: "AvgTemp1_Y",              #146
        #0x0093: "AvgTemp2_Y",              #147
        #0x0081: "MinPowerDate_Y",          #129
        #0x007F: "MaxPowerDate_Y",          #127
        #0x0061: "Temp1xm3",                #97
        #0x006E: "Temp2xm3",                #110
        #0x0071: "Infoevent",               #113
        0x03EA: "Clock",                   #1002
        0x03EB: "Date",                    #1003
        #0x03EC: "HourCounter",             #1004
    }

    #######################################################################
    # Kamstrup uses the "true" CCITT CRC-16
    #
    def crc_1021(self, message):
        poly = 0x1021
        reg = 0x0000
        for byte in message:
            mask = 0x80
            while(mask > 0):
                reg<<=1
                if byte & mask:
                    reg |= 1
                mask>>=1
                if reg & 0x10000:
                    reg &= 0xffff
                    reg ^= poly
        return reg

    #######################################################################
    # Byte values which must be escaped before transmission
    #

    escapes = {
        0x06: True,
        0x0d: True,
        0x1b: True,
        0x40: True,
        0x80: True,
    }

    def send(self, pfx, msg, topic):
        b = bytearray(msg)

        b.append(0)
        b.append(0)
        c = self.crc_1021(b)
        b[-2] = c >> 8
        b[-1] = c & 0xff

        c = bytearray()
        c.append(pfx)
        for i in b:
            if i in self.escapes:
                c.append(0x1b)
                c.append(i ^ 0xff)
            else:
                c.append(i)
        c.append(0x0d)
        self.mqttClient.Publish(topic, c)

    def recv(self, s):
        b = bytearray()

        # Parse hex string
        for i in range(0, len(s), 2):
            d = int(s[i:i+2], 16)
            #if d == None:
                #return None
            #    Domoticz.Log("recv: Error")
            if d == 0x40: # Start of message
                b = bytearray()
            b.append(d)
            if d == 0x0d: # End of message
                break
        Domoticz.Debug("b: " + ''.join('{:02x} '.format(x) for x in b))
        
        # Discard start and stop
        b = b[1:-1]
        
        # Destuffing
        c = bytearray()
        i = 0;
        while i < len(b):
            if b[i] == 0x1b:
                v = b[i + 1] ^ 0xff
                if v not in self.escapes:
                    Domoticz.Log(
                        "Missing Escape %02x" % v)
                c.append(v)
                i += 2
            else:
                c.append(b[i])
                i += 1
        
        # CRC check
        if self.crc_1021(c):
            Domoticz.Log("CRC error:")
            Domoticz.Log("b: " + ''.join('{:02x} '.format(x) for x in b))
            Domoticz.Log("c: " + ''.join('{:02x} '.format(x) for x in c))
            return None
        
        # Discard address and CRC
        c = c[1:-2]
        
        Domoticz.Debug("c: " + ''.join('{:02x} '.format(x) for x in c))
        Domoticz.Debug("recv: Done " + str(c))
        return c

    def readvar(self, b):
        reg = b[1]<<8 | b[2]

        if b[3] in self.units:
            u = self.units[b[3]]
        else:
            u = None

        # Decode the mantissa
        x = 0
        for i in range(0,b[4]): # TODO: Check length of message
            x <<= 8
            x |= b[i + 6]

        # Decode the exponent
        i = b[5] & 0x3f
        if b[5] & 0x40:
            i = -i
        i = math.pow(10,i)
        if b[5] & 0x80:
            i = -i
        x *= i

        return (reg, x, u)

    def getType(self, cmnd_topic):
        self.send(0x80, (0x3f, 0x01), cmnd_topic + '/serialsend4')

    def getSerialNo(self, device):
        configdict = json.loads(device.Options['config'])
        cmnd_topic = configdict['cmnd_topic']
        self.send(0x80, (0x3f, 0x02), cmnd_topic + '/serialsend4')

    def setClock(self, device, date, time):
        configdict = json.loads(device.Options['config'])
        cmnd_topic = configdict['cmnd_topic']
        self.send(0x80, (0x3f, 0x09, \
                  (date >> 24) & 0xff, (date >> 16) & 0xff, (date >> 8) & 0xff, date & 0xff, \
                  (time >> 24) & 0xff, (time >> 16) & 0xff, (time >> 8) & 0xff, time & 0xff), \
                  cmnd_topic + '/serialsend4')

    def getRegister(self, device, reg):
        configdict = json.loads(device.Options['config'])
        cmnd_topic = configdict['cmnd_topic']
        self.send(0x80, (0x3f, 0x10, 0x01, reg >> 8, reg & 0xff), cmnd_topic + '/serialsend4')

        global _plugin
_plugin = BasePlugin()

def onStart():
    global _plugin
    _plugin.onStart()

def onConnect(Connection, Status, Description):
    global _plugin
    _plugin.onConnect(Connection, Status, Description)

def onDisconnect(Connection):
    global _plugin
    _plugin.onDisconnect(Connection)

def onMessage(Connection, Data):
    global _plugin
    _plugin.onMessage(Connection, Data)

def onCommand(Unit, Command, Level, Color):
    global _plugin
    _plugin.onCommand(Unit, Command, Level, Color)

def onDeviceAdded(Unit):
    global _plugin
    _plugin.onDeviceAdded(Unit)

def onDeviceModified(Unit):
    global _plugin
    _plugin.onDeviceModified(Unit)

def onDeviceRemoved(Unit):
    global _plugin
    _plugin.onDeviceRemoved(Unit)

def onHeartbeat():
    global _plugin
    _plugin.onHeartbeat()

def DumpConfigToLog():
    for x in Parameters:
        if Parameters[x] != "":
            Domoticz.Log( "'" + x + "':'" + str(Parameters[x]) + "'")
    Domoticz.Log("Device count: " + str(len(Devices)))
    for x in Devices:
        Domoticz.Log("Device:           " + str(x) + " - " + str(Devices[x]))
        Domoticz.Log("Device LastLevel: " + str(Devices[x].LastLevel))
        Domoticz.Log("Device Color:     " + str(Devices[x].Color))
        Domoticz.Log("Device Options:   " + str(Devices[x].Options))
    return

def DumpMQTTMessageToLog(topic, rawmessage, prefix=''):
    message = rawmessage.decode('utf8','replace')
    message = str(message.encode('unicode_escape'))
    Domoticz.Log(prefix+topic+":"+message)
