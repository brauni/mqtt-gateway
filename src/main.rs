mod mqtt_manager;
mod sensor_manager;

use chrono::{DateTime, Local};
use log::{debug, error, info, warn};
use mqtt_manager::{MqttClientConfigs, MqttManager};
use paho_mqtt::Message;
use std::{
    fs, thread,
    time::{Duration, SystemTime},
};

const DB_TEMPERATURE: &str = "temperature.db";

fn load_mqtt_client_config() -> Result<MqttClientConfigs, serde_json::Error> {
    let config = fs::read_to_string("config.json").expect("Unable to read config.json!");
    info!("{}", config);

    let clients: MqttClientConfigs = serde_json::from_str(&config).unwrap();
    return Ok(clients);
}

fn write_value_to_database(value: f64, table_name: String, db_path: &str) {
    info!("Writing to DB {} - {}", value, table_name);

    let db_connection = sqlite::open(db_path).unwrap();
    let query = format!("CREATE TABLE IF NOT EXISTS {} (id INT AUTO_INCREMENT PRIMARY KEY, Value FLOAT, TimeStamp DATETIME DEFAULT (datetime('now','localtime')))", table_name);
    db_connection.execute(query).unwrap();

    let insert = format!("INSERT INTO {} (Value) VALUES ({});", table_name, value);
    db_connection.execute(insert).unwrap();
}

fn received_sensor_temperature(
    client_id: String,
    payload_str: String,
    topic: String,
    mqtt_manager: &mut MqttManager,
) {
    let collection: Vec<&str> = payload_str.split("#").collect();
    let value: f64 = collection[0].parse().unwrap();
    let sensor_id = collection[1].to_owned();
    let table_name: String = "tb_".to_owned() + &sensor_id;

    write_value_to_database(value, table_name, DB_TEMPERATURE);
    mqtt_manager.received_sensor_temperature(client_id, sensor_id, value, topic)
}

fn received_mqtt_message(msg: Message, client_id: String, mqtt_manager: &mut MqttManager) {
    let topic = msg.topic().to_owned();
    let payload_str = msg.payload_str();

    if topic.starts_with("sensor/temperature") {
        let datetime: DateTime<Local> = SystemTime::now().into();
        info!(
            "{}: {} - {} {}",
            client_id,
            topic,
            payload_str,
            datetime.format("%H:%M:%S")
        );
        debug!("ThreadId: {}", thread_id::get());
        received_sensor_temperature(client_id, payload_str.into_owned(), topic, mqtt_manager);
    } else if topic.starts_with("datalogger/temperature/command") {
        info!("{}: {} - {}", client_id, topic, payload_str);
        match payload_str.as_ref() {
            "get" => mqtt_manager.publish_sensors_of_client(client_id),
            "get_valid" => warn!("get_valid not yet implemented!"),
            unknown_cmd => error!("Received unknown command: {}", unknown_cmd),
        }
    } else {
        info!("{}: {} - {}", client_id, topic, payload_str);
    }
}

fn main() {
    std::env::set_var("RUST_LOG", "info");
    env_logger::init();
    debug!("ThreadId: {}", thread_id::get());

    let clients_config =
        load_mqtt_client_config().expect("Error loading mqtt client config from config.json!");

    let mut mqtt_manager = mqtt_manager::MqttManager::new();
    mqtt_manager.add_clients_from_config(clients_config);
    let receivers = mqtt_manager.connect_all().unwrap();

    // ctrlc::set_handler(move || {
    //     thread::sleep(Duration::from_millis(100));
    //     process::exit(0);
    // })
    // .expect("Error setting Ctrl-C handler");

    loop {
        for receiver in receivers.iter() {
            match receiver.1.try_recv() {
                Ok(msg) => match msg {
                    Some(m) => received_mqtt_message(m, receiver.0.to_string(), &mut mqtt_manager),
                    None => warn!("Received NONE msg on {}", receiver.0),
                },
                Err(_) => {}
            }
        }
        thread::sleep(Duration::from_millis(100));
    }

    // Hitting ^C will exit the app and cause the broker to publish
    // the LWT message since we're not disconnecting cleanly.
}
