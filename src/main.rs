mod mqtt_manager;
mod sensor_manager;

use log::{debug, info, warn};
use mqtt_manager::MqttClientConfigs;
use std::{fs, process, thread, time::Duration};

fn load_mqtt_client_config() -> Result<MqttClientConfigs, serde_json::Error> {
    let config = fs::read_to_string("config.json").expect("Unable to read config.json!");
    info!("{}", config);

    let clients: MqttClientConfigs = serde_json::from_str(&config).unwrap();
    return Ok(clients);
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

    ctrlc::set_handler(move || {
        thread::sleep(Duration::from_millis(100));
        process::exit(0);
    })
    .expect("Error setting Ctrl-C handler");

    loop {
        for receiver in receivers.iter() {
            match receiver.1.try_recv() {
                Ok(msg) => match msg {
                    Some(m) => {
                        println!(
                            "Received msg on {}: {} - {}",
                            receiver.0,
                            m.topic(),
                            m.payload_str()
                        )
                    }
                    None => {}
                },
                Err(_) => {}
            }
        }
        thread::sleep(Duration::from_millis(100));
    }

    // Hitting ^C will exit the app and cause the broker to publish
    // the LWT message since we're not disconnecting cleanly.
}
