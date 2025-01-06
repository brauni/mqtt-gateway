mod mqtt_manager;
mod sensor_manager;

use log::{debug, error, info, warn, LevelFilter};
use log4rs::{
    append::{console::ConsoleAppender, file::FileAppender},
    config::{Appender, Logger, Root},
    Config,
};
use mqtt_manager::{MqttClientConfigs, MqttManager};
use paho_mqtt::{Message, Receiver};
use std::{
    fs,
    path::{Path, PathBuf},
    thread,
    time::Duration,
};

fn init_logger() -> log4rs::Handle {
    let stdout = ConsoleAppender::builder().build();

    let config = Config::builder()
        .appender(Appender::builder().build("stdout", Box::new(stdout)))
        .build(Root::builder().appender("stdout").build(LevelFilter::Info))
        .unwrap();

    return log4rs::init_config(config).unwrap();
}

fn update_log_file_path(handle: &log4rs::Handle, mount_point: String) {
    if mount_point.is_empty() {
        warn!("No mount point defined for logfile");
        return;
    }

    let log_file_path: String;
    match get_usb_drive_path(&mount_point) {
        Ok(log_path) => {
            log_file_path = log_path + "/mqtt-gateway.log";
            info!("log_file_path: {}", log_file_path);
        }
        Err(e) => {
            error!("Error getting usb drive path for log: {:?}", e);
            return;
        }
    }

    let stdout = ConsoleAppender::builder().build();

    let file = FileAppender::builder().build(log_file_path).unwrap();

    let config = Config::builder()
        .appender(Appender::builder().build("stdout", Box::new(stdout)))
        .appender(Appender::builder().build("file", Box::new(file)))
        .logger(
            Logger::builder()
                .appender("file")
                .additive(true)
                .build("mqtt_gateway", LevelFilter::Info),
        )
        .build(Root::builder().appender("stdout").build(LevelFilter::Info))
        .unwrap();

    handle.set_config(config);
}

fn load_config() -> (MqttClientConfigs, String, String) {
    let config = fs::read_to_string("config.json").expect("Unable to read config.json!");
    info!("{}", config);

    let client_configs = load_mqtt_client_config(&config).unwrap();

    let mount_point = load_mount_point_config(&config);

    let name = load_name(&config);

    return (client_configs, mount_point, name);
}

fn load_name(config: &str) -> String {
    let mut name: String = "".to_owned();
    match serde_json::from_str::<serde_json::Value>(config) {
        Ok(config) => match config["name"].as_str() {
            Some(value) => name = value.to_string(),
            None => warn!("No mount_point defined"),
        },
        Err(_) => error!("Could not open config.json!"),
    }
    info!("name: {}", name);
    return name;
}

fn load_mqtt_client_config(config: &str) -> Result<MqttClientConfigs, serde_json::Error> {
    let clients: MqttClientConfigs = serde_json::from_str(config)?;
    return Ok(clients);
}

/// Returns the mount point for usb drives defined in config.json as String. Returns empty String if no path is defined.
fn load_mount_point_config(config: &str) -> String {
    let mut mount_point: String = "".to_owned();
    match serde_json::from_str::<serde_json::Value>(config) {
        Ok(path) => match path["mount_point"].as_str() {
            Some(drive_path) => mount_point = drive_path.to_string(),
            None => warn!("No mount_point defined"),
        },
        Err(_) => error!("Could not open config.json!"),
    }
    info!("mount_point: {}", mount_point);
    return mount_point;
}

fn get_usb_drive_path(usb_drive: &str) -> Result<String, std::io::Error> {
    let mut usb_drive_path = PathBuf::new();

    usb_drive_path = Path::new(usb_drive).to_path_buf();

    let dir = fs::read_dir(usb_drive_path.clone())?.next().unwrap()?;
    usb_drive_path = usb_drive_path.join(dir.path());

    return Ok(usb_drive_path.display().to_string());
}

/// Inserts `value` to the table `table_name` in the passed database path. Creates the table if it does not yet exist.
fn insert_value_to_table(value: f64, table_name: String, db_file_path: &str) {
    info!("Writing to DB {}: {} - {}", db_file_path, value, table_name);

    let db_connection = sqlite::open(db_file_path).unwrap();
    let query = format!("CREATE TABLE IF NOT EXISTS {} (id INT AUTO_INCREMENT PRIMARY KEY, Value FLOAT, TimeStamp DATETIME DEFAULT (datetime('now','localtime')))", table_name);
    db_connection.execute(query).unwrap();

    let insert = format!("INSERT INTO {} (Value) VALUES ({});", table_name, value);
    db_connection.execute(insert).unwrap();
}

fn write_value_to_database(value: f64, mount_point: &str, table_name: String, client_id: &str) {
    if mount_point.is_empty() {
        warn!("No mount point defined for DB");
        return;
    }
    match get_usb_drive_path(mount_point) {
        Ok(db_path) => {
            let db_file_path = db_path + "/" + client_id + ".db";
            insert_value_to_table(value, table_name, &db_file_path);
        }
        Err(e) => error!("Error getting usb drive path for DB: {:?}", e),
    }
}

fn received_sensor_temperature(
    client_id: String,
    payload_str: String,
    topic: String,
    mqtt_manager: &mut MqttManager,
    mount_point: &str,
) {
    let collection: Vec<&str> = payload_str.split("#").collect();
    let value: f64 = collection[0].parse().unwrap();
    let sensor_id = collection[1].to_owned();
    let table_name: String = "tb_".to_owned() + &sensor_id;

    write_value_to_database(value, mount_point, table_name, &client_id);
    mqtt_manager.received_sensor_temperature(client_id, sensor_id, value, topic)
}

fn received_mqtt_message(
    msg: Message,
    client_id: String,
    mqtt_manager: &mut MqttManager,
    mount_point: &str,
    name: &str,
) {
    let topic = msg.topic().to_owned();
    let payload_str = msg.payload_str();

    if topic.starts_with("sensor/temperature") {
        info!("{}: {} - {}", client_id, topic, payload_str);
        received_sensor_temperature(
            client_id,
            payload_str.into_owned(),
            topic,
            mqtt_manager,
            mount_point,
        );
    } else if topic.starts_with("datalogger/temperature/command") {
        info!("{}: {} - {}", client_id, topic, payload_str);
        match payload_str.as_ref() {
            "get" => mqtt_manager.publish_sensors_of_client(client_id),
            "get_valid" => warn!("get_valid not yet implemented!"),
            unknown_cmd => error!("Received unknown command: {}", unknown_cmd),
        }
    } else if topic.starts_with("datalogger/command") {
        info!("{}: {} - {}", client_id, topic, payload_str);
        match payload_str.as_ref() {
            "ping" => mqtt_manager.publish_ping_ack(client_id, &name),
            unknown_cmd => error!("Received unknown command: {}", unknown_cmd),
        }
    } else {
        warn!("Received unknown topic!");
        match payload_str.len() < 50 {
            true => debug!("{}: {} - {}", client_id, topic, payload_str),
            false => debug!("{}: {} - len {}", client_id, topic, payload_str.len()),
        }
    }
}

fn receive_non_blocking(
    receiver: (&String, &Receiver<Option<Message>>),
    mount_point: &str,
    mqtt_manager: &mut MqttManager,
    name: &str,
) {
    match receiver.1.try_recv() {
        Ok(msg) => match msg {
            Some(m) => {
                received_mqtt_message(m, receiver.0.to_string(), mqtt_manager, &mount_point, &name);
            }
            None => {
                warn!("Received NONE msg on {}, trying to reconnect", receiver.0);
                mqtt_manager.reconnect(receiver.0.to_string());
            }
        },
        Err(_) => {}
    }
}

fn main() {
    let logger = init_logger();

    let (clients_config, mount_point, name) = load_config();

    update_log_file_path(&logger, mount_point.clone());

    info!("OS: {}", std::env::consts::OS);

    let mut mqtt_manager = mqtt_manager::MqttManager::new();

    mqtt_manager.add_clients_from_config(clients_config);

    let receivers = mqtt_manager.connect_all().unwrap();

    loop {
        for receiver in receivers.iter() {
            receive_non_blocking(receiver, &mount_point, &mut mqtt_manager, &name)
        }
        thread::sleep(Duration::from_millis(100));
    }

    // Hitting ^C will exit the app and cause the broker to publish
    // the LWT message since we're not disconnecting cleanly.
}
