use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct MqttMessage {
    pub topic: String,
    pub payload: String,
}
