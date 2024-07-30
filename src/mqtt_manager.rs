use chrono::{DateTime, Local};
use paho_mqtt::{self as mqtt, AsyncClient, ConnectOptions};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::thread;
use std::time::{Duration, SystemTime};

use crate::sensor_manager::{self, SensorManager};

#[derive(Serialize, Deserialize, Debug)]
struct MqttClientConfig {
    name: String,
    address: String,
    port: u32,
    user: String,
    password: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct MqttClientConfigs {
    mqtt_clients: Vec<MqttClientConfig>,
}

pub struct MqttClient {
    client: mqtt::AsyncClient,
    sensor_manager: SensorManager,
}

impl MqttClient {
    fn new(config: &MqttClientConfig) -> Self {
        let create_opts = MqttClient::create_client_options(&config);

        let client = mqtt::AsyncClient::new(create_opts).unwrap();

        MqttClient {
            client,
            sensor_manager: SensorManager::new(),
        }
    }

    fn create_client_options(config: &MqttClientConfig) -> mqtt::CreateOptions {
        let conn_opts = mqtt::ConnectOptionsBuilder::new()
            .keep_alive_interval(Duration::from_secs(20))
            .user_name(config.user.to_string())
            .password(config.password.to_string())
            .finalize();

        let create_opts = mqtt::CreateOptionsBuilder::new()
            .server_uri(config.address.to_string())
            .client_id(config.name.to_string())
            .user_data(Box::new(conn_opts))
            .finalize();

        return create_opts;
    }

    fn connect(&self) -> Result<(), mqtt::Error> {
        self.client.set_connected_callback(move |client| {
            println!("Client {} connected", client.client_id());
        });
        self.client.set_connection_lost_callback(move |client| {
            println!("Client {} connection lost", client.client_id());
        });
        self.client.set_message_callback(move |client, msg| {
            if let Some(msg) = msg {
                let topic = msg.topic().to_owned();
                let payload_str = msg.payload_str();

                if topic.starts_with("sensor/temperature/") {
                    let system_time = SystemTime::now();
                    let datetime: DateTime<Local> = system_time.into();
                    println!(
                        "{}: {} - {} {}",
                        client.client_id(),
                        topic,
                        payload_str,
                        datetime.format("%H:%M:%S")
                    );
                    println!("ThreadId: {}", thread_id::get());
                    MqttClient::received_sensor_temperature(payload_str.into_owned(), topic);
                } else if topic.starts_with("datalogger/temperature/") {
                    println!("{}: {} - {}", client.client_id(), topic, payload_str);
                    match payload_str.as_ref() {
                        "get" => println!("get"), // TODO: publish all stored sensor values
                        "get_valid" => println!("get_valid"),
                        _ => println!("default"),
                    }
                } else {
                    println!("{} - {}", topic, payload_str);
                }
            }
        });

        println!(
            "{} connecting to the MQTT broker...",
            self.client.client_id()
        );
        println!("ThreadId: {}", thread_id::get());

        let data = self.client.user_data();
        let opts = data.unwrap().downcast_ref::<ConnectOptions>().unwrap();

        self.client
            .connect_with_callbacks(
                opts.clone(),
                MqttClient::on_connect_succeeded,
                MqttClient::on_connect_failed,
            )
            .wait()?;
        Ok(())
    }

    fn on_connect_succeeded(client: &AsyncClient, _: u16) {
        println!("Connection succeeded");
        client.subscribe("#", 1);
    }

    fn on_connect_failed(client: &AsyncClient, _: u16, rc: i32) {
        println!("Connection attempt failed with error code {}.\n", rc);
        thread::sleep(Duration::from_millis(5000));
        client.reconnect_with_callbacks(
            MqttClient::on_connect_succeeded,
            MqttClient::on_connect_failed,
        );
    }

    fn disconnect(&self) -> Result<(), mqtt::Error> {
        self.client.disconnect(None).wait()?;
        Ok(())
    }

    fn received_sensor_temperature(payload_str: String, topic: String) {
        let collection: Vec<&str> = payload_str.split("#").collect();
        let value: f64 = collection[0].parse().unwrap();
        let sensor_id = collection[1].to_owned();
        let table_name: String = "tb_".to_owned() + &sensor_id;
        println!("{} : {} - {}", value, sensor_id, table_name);

        let db_connection = sqlite::open("temperature.db").unwrap();
        let query = format!("CREATE TABLE IF NOT EXISTS {} (id INT AUTO_INCREMENT PRIMARY KEY, Value FLOAT, TimeStamp DATETIME DEFAULT (datetime('now','localtime')))", table_name);
        db_connection.execute(query).unwrap();

        let insert = format!("INSERT INTO {} (Value) VALUES ({});", table_name, value);
        db_connection.execute(insert).unwrap();

        //self.sensor_manager.update_sensor(sensor_id, value, topic)
    }
}

pub struct MqttManager {
    clients: HashMap<String, MqttClient>,
}

impl MqttManager {
    pub fn new() -> Self {
        MqttManager {
            clients: HashMap::new(),
        }
    }

    pub fn add_clients_from_config(&mut self, client_configs: MqttClientConfigs) {
        for config in client_configs.mqtt_clients {
            let client = MqttClient::new(&config);
            self.clients.insert(config.name, client);
        }
    }

    pub fn connect_all(&self) -> Result<(), mqtt::Error> {
        for (id, client) in self.clients.iter() {
            println!("Connecting client: {}", id);
            client.connect()?;
        }
        Ok(())
    }

    pub fn disconnect_all(&self) -> Result<(), mqtt::Error> {
        for (id, client) in self.clients.iter() {
            println!("Disconnecting client: {}", id);
            client.disconnect()?;
        }
        Ok(())
    }
}
