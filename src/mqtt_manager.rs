use mqtt::Message;
use paho_mqtt as mqtt;
use serde::{Deserialize, Serialize};
use std::{process, sync::RwLock, thread, time::Duration};

const DFLT_TOPICS: &[&str] = &["#"];
const QOS: i32 = 1;

type UserTopics = RwLock<Vec<String>>;

#[derive(Clone)]
pub struct MqttManager {
    clients: Vec<mqtt::AsyncClient>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct MqttClientConfigs {
    mqtt_clients: Vec<MqttClientConfig>,
}

#[derive(Serialize, Deserialize, Debug)]
struct MqttClientConfig {
    name: String,
    address: String,
    port: u32,
    user: String,
    password: String,
}

impl MqttManager {
    pub fn new(client_configs: MqttClientConfigs) -> Self {
        let mut clients_vector: Vec<mqtt::AsyncClient> = vec![];

        for config in client_configs.mqtt_clients {
            let client = MqttManager::create_mqtt_client(&config);
            clients_vector.push(client);
        }

        MqttManager {clients: clients_vector}
    }

    fn create_mqtt_client(config: &MqttClientConfig) -> mqtt::AsyncClient{
        let (create_opts, conn_opts) = MqttManager::create_client_options(&config);

            let client = mqtt::AsyncClient::new(create_opts).unwrap_or_else(|e| {
                println!("Error creating the client: {:?}", e);
                process::exit(1);
            });

            client.set_connected_callback(MqttManager::on_connected);
            client.set_connection_lost_callback(MqttManager::on_connection_lost);
            client.set_message_callback(MqttManager::on_message_received);

            println!("{} connecting to the MQTT broker...", client.client_id());
            client.connect_with_callbacks(conn_opts,MqttManager::on_connect_success,MqttManager::on_connect_failure);
            return client;
    }

    fn create_client_options(config: &MqttClientConfig) -> (mqtt::CreateOptions, mqtt::ConnectOptions) {
        let topics: Vec<String> = DFLT_TOPICS.iter().map(|s| s.to_string()).collect();

        let create_opts = mqtt::CreateOptionsBuilder::new()
            .server_uri(config.address.to_string())
            .client_id(config.name.to_string())
            .user_data(Box::new(RwLock::new(topics)))
            .finalize();

        let conn_opts = mqtt::ConnectOptionsBuilder::new()
            .keep_alive_interval(Duration::from_secs(20))
            .user_name(config.user.to_string())
            .password(config.password.to_string())
            .finalize();

        return (create_opts, conn_opts);
    }

    fn on_connection_lost(client: &mqtt::AsyncClient) {
        println!("{} connection lost. Attempting reconnect.", client.client_id());
        thread::sleep(Duration::from_millis(2500));
        client.reconnect_with_callbacks(MqttManager::on_connect_success,MqttManager::on_connect_failure);
    }

    fn on_connected(client: &mqtt::AsyncClient) {
        println!("{} connected.", client.client_id());
    }

    fn on_connect_success(client: &mqtt::AsyncClient, _msgid: u16) {
        println!("Connection succeeded");
        let data = client.user_data().unwrap();

        if let Some(lock) = data.downcast_ref::<UserTopics>() {
            let topics = lock.read().unwrap();
            println!("Subscribing to topics: {:?}", topics);

            // Create a QoS vector, same len as # topics
            let qos = vec![QOS; topics.len()];
            // Subscribe to the desired topic(s).
            client.subscribe_many(&topics, &qos);
            // TODO: This doesn't yet handle a failed subscription.
        }
    }

    fn on_connect_failure(cli: &mqtt::AsyncClient, _msgid: u16, rc: i32) {
        println!("Connection attempt failed with error code {}.\n", rc);
        thread::sleep(Duration::from_millis(2500));
        cli.reconnect_with_callbacks( MqttManager::on_connect_success,MqttManager::on_connect_failure);
    }

    fn on_message_received(client: &mqtt::AsyncClient, msg: Option<Message>) {
        if let Some(msg) = msg {
            let topic = msg.topic();
            let payload_str = msg.payload_str();

            if topic.starts_with("sensor/temperature/") {
                println!("{}: {} - {}", client.client_id(), topic, payload_str);
                MqttManager::received_sensor_temperature(payload_str.into_owned());
            } else if topic.starts_with("datalogger/temperature/")  {
                println!("{}: {} - {}", client.client_id(), topic, payload_str);
                match payload_str.as_ref() {
                    "get" => println!("get"), // TODO: publish stored valid sensor values
                    _ => println!("default"),
                }
            }
            else {
                println!("{} - {}", topic, payload_str);
            }
        }
    }

    fn received_sensor_temperature(payload_str: String){
        let collection: Vec<&str> = payload_str.split("#").collect();
        let value: f64 = collection[0].parse().unwrap();
        let sensor_id = collection[1];
        let table_name:String = "tb_".to_owned() + sensor_id;
        println!("{} : {} - {}", value, sensor_id, table_name);

        let db_connection = sqlite::open("temperature.db").unwrap();
        let query = format!("CREATE TABLE IF NOT EXISTS {} (id INT AUTO_INCREMENT PRIMARY KEY, Value FLOAT, TimeStamp DATETIME DEFAULT (datetime('now','localtime')))", table_name);
        db_connection.execute(query).unwrap();

        let insert = format!("INSERT INTO {} (Value) VALUES ({});", table_name, value);
        db_connection.execute(insert).unwrap();
    }

    pub fn disconnect(&mut self) {
        for client in self.clients.iter().cloned() {
            client.disconnect(None);
        }
    }
}
