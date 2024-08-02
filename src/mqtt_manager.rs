use chrono::{DateTime, Local};
use log::{debug, error, info, warn};
use paho_mqtt::{self as mqtt, AsyncClient, ConnectOptions, Message, Receiver};
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

const TIMEOUT: Duration = Duration::from_secs(10);

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

    fn connect(&self) -> Result<Receiver<Option<Message>>, mqtt::Error> {
        self.client.set_connected_callback(move |client| {
            info!("Client {} connected", client.client_id());
        });

        let receiver = self.client.start_consuming();

        let data = self.client.user_data();
        let opts = data.unwrap().downcast_ref::<ConnectOptions>().unwrap();
        self.client
            .connect_with_callbacks(
                opts.clone(),
                MqttClient::on_connect_succeeded,
                MqttClient::on_connect_failed,
            )
            .wait_for(TIMEOUT)?;

        Ok(receiver)
    }

    fn on_connect_succeeded(client: &AsyncClient, _: u16) {
        info!("{} connection succeeded", client.client_id());
        client.subscribe("#", 1);
    }

    fn on_connect_failed(client: &AsyncClient, _: u16, rc: i32) {
        error!("{} connect failed, error code: {}", client.client_id(), rc);
        thread::sleep(Duration::from_millis(5000));
        let reconnected = client
            .reconnect_with_callbacks(
                MqttClient::on_connect_succeeded,
                MqttClient::on_connect_failed,
            )
            .wait_for(TIMEOUT);

        match reconnected {
            Ok(_) => {}
            Err(e) => error!("Reconnect failed: {}", e),
        }
    }

    fn disconnect(&self) -> Result<(), mqtt::Error> {
        self.client.disconnect(None).wait_for(TIMEOUT)?;
        Ok(())
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

    pub fn connect_all(&self) -> Result<HashMap<String, Receiver<Option<Message>>>, mqtt::Error> {
        let mut receivers: HashMap<String, Receiver<Option<Message>>> = HashMap::new();
        for (id, client) in self.clients.iter() {
            info!("Connecting client: {}", id);
            let receiver = client.connect()?;
            receivers.insert(id.to_owned(), receiver);
        }
        Ok(receivers)
    }

    pub fn disconnect_all(&self) -> Result<(), mqtt::Error> {
        for (id, client) in self.clients.iter() {
            info!("Disconnecting client: {}", id);
            client.disconnect()?;
        }
        Ok(())
    }

    pub fn received_sensor_temperature(
        &mut self,
        client_id: String,
        sensor_id: String,
        value: f64,
        topic: String,
    ) {
        let client: &mut MqttClient = self.clients.get_mut(&client_id).unwrap();
        client.sensor_manager.update_sensor(sensor_id, value, topic)
    }

    pub fn publish_sensors_of_client(&self, client_id: String) {
        let client = self.clients.get(&client_id).unwrap();
        let sensors = client.sensor_manager.get_sensors();

        for sensor in sensors.iter() {
            let topic = "datalogger/".to_owned() + &sensor.get_topic_string();
            info!(
                "Publishing on {}: {} - {}",
                client_id,
                sensor.get_encoded_string(),
                topic
            );
            let msg = mqtt::MessageBuilder::new()
                .topic(topic)
                .payload(sensor.get_encoded_string())
                .qos(1)
                .finalize();

            if let Err(e) = client.client.try_publish(msg) {
                error!("{} error publishing message: {:?}", client_id, e);
            }
        }
    }
}
