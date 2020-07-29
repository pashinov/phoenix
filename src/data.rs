use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize)]
pub struct MqttMessage {
    pub topic: String,
    pub payload: String,
}
