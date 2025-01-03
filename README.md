# mqtt-gateway
The mqtt-gateway is designed to run on a light weight single board computer like the Raspberry Pi Zero W.
It creates mqtt-clients that are configured in a `config.json` file and subscribes on all clients to the wildcard topic `"#"` to receive all messages. The `config.json` should look like the following:
```
{
    "mqtt_clients":[
       {
          "name":"Client1",
          "address":"clientaddress1.com:123",
          "port":123,
          "user":"user1",
          "password":"pw1"
       },
       {
          "name":"Client2",
          "address":"clientaddress1.com:122",
          "port":122,
          "user":"user2",
          "password":"pw2"
       }
    ],
    "usb_drive_path":"/media/usb"
 }
```

The main features of the mqtt-gateway are:
- Receive data from temperature sensors under the topic `"sensor/temperature/"` and write them to an SQLite database on an usb drive, located at `"usb_drive_path"` in `config.json`. 
- On receiveing a `"get"` command under the topic `"datalogger/temperature"` repeat all stored and valid temperature sensor values under the same topic.


Build for Raspberry-Pi Zero using Cross
Install docker engine.
Use command  `"cross run --target arm-unknown-linux-gnueabihf"` to build for raspberry-pi zero target.