# Kamstrup Meter (Sonoff-Tasmota MQTT) plugin
Domoticz Python plugin which implements support for Kamstrup Meters (district heating, and others) connected to Sonoff-Tasmota device.

### Features:
- Supports multiple meters
  - The plugin has been tested with a single KM402 district heating meter
  - Please open PR or issue for support for other meter or meter type

### Prerequisites:
- Sonoff-Tasmota device connected to Kamstrup meter using IR eye
  - IR eye can be bought or DIY, e.g. http://wiki.hal9k.dk/projects/kamstrup

### Instructions:
- Clone this project into Domoticz 'plugins' folder
- Restart Domoticz
- Create hardware of type "Kamstrup Meter (Sonoff-Tasmota MQTT)"
  - Set MQTT IP and port
  - Set the topic of the Sonoff-Tasmota device connected to your meter.
    - Multiple devices are supported, separate the topics by comma
  - Set "Debug" to "Verbose" for debug log
- Domoticz should now detect any device running Tasmota firmware
