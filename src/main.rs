mod mqtt_manager;
mod sensor_manager;

use mqtt_manager::MqttClientConfigs;
use std::{fs, process, thread, time::Duration};

fn load_mqtt_client_config() -> Result<MqttClientConfigs, serde_json::Error> {
    let config = fs::read_to_string("config.json").expect("Unable to read config.json!");
    println!("{}", config);

    let clients: MqttClientConfigs = serde_json::from_str(&config).unwrap();
    return Ok(clients);
}

fn main() {
    env_logger::init();
    println!("ThreadId: {}", thread_id::get());

    let clients_config =
        load_mqtt_client_config().expect("Error loading mqtt client config from config.json!");

    let mut mqtt_manager = mqtt_manager::MqttManager::new();
    mqtt_manager.add_clients_from_config(clients_config);
    mqtt_manager.connect_all().unwrap();

    ctrlc::set_handler(move || {
        thread::sleep(Duration::from_millis(100));
        process::exit(0);
    })
    .expect("Error setting Ctrl-C handler");

    loop {
        thread::sleep(Duration::from_millis(100));
    }

    // Hitting ^C will exit the app and cause the broker to publish
    // the LWT message since we're not disconnecting cleanly.
}
