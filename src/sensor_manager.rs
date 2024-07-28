use std::time::{Duration, Instant, SystemTime};

pub struct SensorManager {
    sensors: Vec<Sensor>,
}

impl SensorManager {
    pub fn new() -> Self {
        SensorManager { sensors: vec![] }
    }

    pub fn update_sensor(mut self, sensor_id: String, value: f64, topic: String) {
        let sensor = self
            .sensors
            .iter_mut()
            .find(|sensor| sensor.id == sensor_id);

        match sensor {
            Some(s) => s.update(value),
            None => self.sensors.push(Sensor::new(sensor_id, value, topic)),
        }
    }
}

pub struct Sensor {
    id: String,
    value: f64,
    timestamp: SystemTime,
    topic: String,
    valid: bool,
}

impl Sensor {
    pub fn new(id: String, value: f64, topic: String) -> Self {
        Sensor {
            id,
            value,
            timestamp: SystemTime::now(),
            topic,
            valid: true,
        }
    }

    pub fn update(&mut self, value: f64) {
        self.value = value;
        self.timestamp = SystemTime::now();
        self.valid = true;
    }
}
