use std::{cell::RefCell, rc::Rc};
use std::time::Duration;

use futures::channel::mpsc;
use futures::SinkExt;
use futures::stream::StreamExt;
use log::{debug, error, info, trace, warn};
use paho_mqtt as mqtt;
use protobuf::Message;

use crate::phoenix;

pub struct PahoConnector {
    paho_subscriber: PahoSubscriber,
    paho_publisher: PahoPublisher,
}

struct PahoPublisher {
    cfg: Rc<config::Config>,
    rx: mpsc::Receiver<String>,
    cli: Rc<RefCell<mqtt::AsyncClient>>,
}

struct PahoSubscriber {
    cfg: Rc<config::Config>,
    tx: mpsc::Sender<String>,
    cli: Rc<RefCell<mqtt::AsyncClient>>,
}

impl PahoConnector {
    pub fn new(cfg: Rc<config::Config>, tx: mpsc::Sender<String>, rx: mpsc::Receiver<String>) -> Self {
        let uri = cfg.get_str("Config.Connector.MQTT.URI").unwrap();
        let id = cfg.get_str("Config.Connector.MQTT.ClientId").unwrap();

        let create_opts = mqtt::CreateOptionsBuilder::new()
            .server_uri(uri)
            .client_id(id)
            .finalize();

        let cli = mqtt::AsyncClient::new(create_opts).unwrap_or_else(|err| {
            panic!("Error creating the client: {:?}", err);
        });

        let ref_cli = Rc::new(RefCell::new(cli));

        PahoConnector {
            paho_subscriber: PahoSubscriber::new(Rc::clone(&cfg), tx, Rc::clone(&ref_cli)),
            paho_publisher: PahoPublisher::new(Rc::clone(&cfg), rx, Rc::clone(&ref_cli)),
        }
    }

    pub async fn start(&mut self) {
        let paho_subscriber_start = self.paho_subscriber.start();
        let paho_publisher_start = self.paho_publisher.start();

        let (_, _) = futures::join!(paho_subscriber_start, paho_publisher_start);
    }
}

impl PahoPublisher {
    fn new(cfg: Rc<config::Config>, rx: mpsc::Receiver<String>, cli: Rc<RefCell<mqtt::AsyncClient>>) -> Self {
        PahoPublisher {
            cfg,
            rx,
            cli,
        }
    }

    async fn start(&mut self) -> () {
        while let Some(msg) = self.rx.next().await {
            let qos = self.cfg.get_int("Config.Connector.MQTT.QOS").unwrap() as i32;

            let message: phoenix::message = protobuf::parse_from_bytes(msg.as_bytes()).unwrap();
            let topic = message.topic;
            let payload = message.payload;

            info!("Publishing MQTT message where topic is '{}', payload is '{}'", topic, payload);

            let pub_msg = mqtt::Message::new(topic.clone(), payload.clone(), qos);
            if self.cli.borrow().is_connected() {
                match self.cli.borrow().publish(pub_msg).await {
                    Ok(_) => { debug!("Publish message '{}' to topic '{}' successful", payload.clone(), topic.clone()); }
                    Err(err) => { error!("Publish message '{}' to topic '{}' failed: {}", payload.clone(), topic.clone(), err); }
                }
            }
        }
    }
}

impl PahoSubscriber {
    fn new(cfg: Rc<config::Config>, tx: mpsc::Sender<String>, cli: Rc<RefCell<mqtt::AsyncClient>>) -> Self {
        PahoSubscriber {
            cfg,
            tx,
            cli,
        }
    }

    async fn start(&mut self) -> () {
        // Get message stream before connecting
        let mut strm = self.cli.borrow_mut().get_stream(25);

        let true_store = self.cfg.get_str("Config.Connector.MQTT.SSLOpt.TrustStore").unwrap();
        let key_store = self.cfg.get_str("Config.Connector.MQTT.SSLOpt.KeyStore").unwrap();
        let private_key = self.cfg.get_str("Config.Connector.MQTT.SSLOpt.PrivateKey").unwrap();

        let ssl_opts = mqtt::SslOptionsBuilder::new()
            .trust_store(true_store).unwrap()
            .key_store(key_store).unwrap()
            .private_key(private_key).unwrap()
            .finalize();

        let qos = self.cfg.get_int("Config.Connector.MQTT.QOS").unwrap() as i32;
        let clean_session = self.cfg.get_bool("Config.Connector.MQTT.ConnOpt.CleanSession").unwrap();
        let keep_alive_interval = self.cfg.get_int("Config.Connector.MQTT.ConnOpt.KeepAliveIntervalSec").unwrap() as u64;
        let reconnect_retry_interval = self.cfg.get_int("Config.Connector.MQTT.ConnOpt.ReconnectRetryIntervalSec").unwrap() as u64;

        let conn_opts = Rc::new(RefCell::new(mqtt::ConnectOptionsBuilder::new()
            .keep_alive_interval(Duration::from_secs(keep_alive_interval))
            .mqtt_version(mqtt::MQTT_VERSION_3_1_1)
            .clean_session(clean_session)
            .ssl_options(ssl_opts)
            .finalize()));

        info!("Connecting to the MQTT server...");
        while let Err(err) = self.cli.borrow().connect(conn_opts.borrow_mut().clone()).await {
            warn!("Error connecting: {}", err);
            async_std::task::sleep(Duration::from_secs(reconnect_retry_interval.clone())).await;
            info!("Connecting to the MQTT server...");
        }
        info!("...Connection established");

        let topics = self.cfg.get_array("Config.Connector.MQTT.Topic").unwrap();
        for topic in topics {
            match self.cli.borrow().subscribe(topic.clone().into_str().unwrap(), qos).await {
                Ok(_) => { info!("Subscribe to topic '{}' successful", topic.clone().into_str().unwrap()); }
                Err(err) => { error!("Subscribe to topic '{}' successful failed: {}", topic.clone().into_str().unwrap(), err); }
            }
        }

        // Just loop on incoming messages
        info!("Waiting for MQTT messages...");
        while let Some(msg_opt) = strm.next().await {
            if let Some(msg) = msg_opt {
                info!("Received MQTT message: topic is {}; payload is {}", msg.topic(), msg.payload_str());

                let mut message = phoenix::message::new();
                message.topic = msg.topic().to_string();
                message.payload = msg.payload_str().to_string();

                self.tx.send(String::from_utf8(message.write_to_bytes().unwrap()).expect("Found invalid UTF-8")).await.unwrap();
            } else {
                // A "None" means we were disconnected. Try to reconnect...
                warn!("Lost connection. Attempting reconnect.");
                while let Err(err) = self.cli.borrow().reconnect().await {
                    info!("Error reconnecting: {}", err);
                    async_std::task::sleep(Duration::from_secs(reconnect_retry_interval.clone())).await;
                }
                let topics = self.cfg.get_array("Config.Connector.MQTT.Topics.Sub").unwrap();
                for topic in topics {
                    match self.cli.borrow().subscribe(topic.clone().into_str().unwrap(), qos).await {
                        Ok(_) => { info!("Subscribe to topic '{}' successful", topic.clone().into_str().unwrap()); }
                        Err(err) => { error!("Subscribe to topic '{}' successful failed: {}", topic.clone().into_str().unwrap(), err); }
                    }
                }
            }
        }
    }
}

impl Drop for PahoSubscriber {
    fn drop(&mut self) {
        trace!("Dropping PahoSubscriber...");
        if self.cli.borrow().is_connected() {
            self.cli.borrow().disconnect(None);
        }
    }
}

impl Drop for PahoPublisher {
    fn drop(&mut self) {
        trace!("Dropping PahoPublisher...");
        if self.cli.borrow().is_connected() {
            self.cli.borrow().disconnect(None);
        }
    }
}

impl Drop for PahoConnector {
    fn drop(&mut self) {
        trace!("Dropping PahoConnector...");
    }
}
