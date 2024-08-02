use std::time::SystemTime;

use chrono::{DateTime, Local};
use log::info;

pub struct SensorManager {
    sensors: Vec<Sensor>,
}

impl SensorManager {
    pub fn new() -> Self {
        SensorManager { sensors: vec![] }
    }

    pub fn update_sensor(&mut self, sensor_id: String, value: f64, topic: String) {
        let sensor = self
            .sensors
            .iter_mut()
            .find(|sensor| sensor.id == sensor_id);

        match sensor {
            Some(s) => s.update(value, topic),
            None => self.sensors.push(Sensor::new(sensor_id, value, topic)),
        }
    }

    pub fn get_sensors(&self) -> &Vec<Sensor> {
        return &self.sensors;
    }
}

pub struct Sensor {
    id: String,
    value: f64,
    timestamp: DateTime<Local>,
    topic: String,
    valid: bool,
}

impl Sensor {
    pub fn new(id: String, value: f64, topic: String) -> Self {
        info!("New Sensor {} - {} | {}", id, topic, value);
        Sensor {
            id,
            value,
            timestamp: SystemTime::now().into(),
            topic,
            valid: true,
        }
    }

    pub fn update(&mut self, value: f64, topic: String) {
        info!("Update Sensor {} - {} | {}", self.id, topic, value);
        self.value = value;
        self.timestamp = SystemTime::now().into();
        self.valid = true;
        self.topic = topic;
    }

    pub fn get_encoded_string(&self) -> String {
        return self.value.to_string()
            + "#"
            + &self.id.to_string()
            + "#"
            + &self.timestamp.format("%H:%M:%S").to_string();
    }

    pub fn get_topic_string(&self) -> String {
        return self.topic.to_string();
    }
}
