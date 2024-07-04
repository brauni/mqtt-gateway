mod mqtt_manager;

use std::{process, thread, time::Duration, fs};
use mqtt_manager::MqttClientConfigs;

fn load_mqtt_client_config() -> Result<MqttClientConfigs,serde_json::Error>{
    let config = fs::read_to_string("config.json").expect("Unable to read config.json!");
    println!("{}", config);

    let clients: MqttClientConfigs = serde_json::from_str(&config).unwrap();
    return Ok(clients);
}

fn main() {
    // Initialize the logger from the environment
    env_logger::init();

    let clients_config = load_mqtt_client_config().expect("Error loading mqtt client config from config.json!");

    let client = mqtt_manager::MqttManager::new(clients_config);

    // ^C handler will stop the consumer, breaking us out of the loop, below
    let mut ctrlc_cli = client.clone();
    ctrlc::set_handler(move || {
        println!("Disconnecting...");
        ctrlc_cli.disconnect();
        thread::sleep(Duration::from_millis(100));
        process::exit(0);
    })
    .expect("Error setting Ctrl-C handler");

    // Just wait for incoming messages.
    loop {
        thread::sleep(Duration::from_millis(100));
    }

    // Hitting ^C will exit the app and cause the broker to publish
    // the LWT message since we're not disconnecting cleanly.
}
