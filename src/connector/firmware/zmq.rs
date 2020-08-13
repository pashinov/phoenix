use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::cell::RefCell;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use std::thread;

use futures::channel::mpsc;
use futures::SinkExt;
use futures::stream::StreamExt;
use log::{error, trace};
use protobuf::Message;

use crate::data::MqttMessage;
use crate::phoenix;

pub struct ZmqConnector {
    cfg: Rc<config::Config>,
    tx: Rc<RefCell<mpsc::Sender<String>>>,
    rx: Rc<RefCell<mpsc::Receiver<String>>>,
}

impl ZmqConnector {
    pub fn new(cfg: Rc<config::Config>, tx: mpsc::Sender<String>, rx: mpsc::Receiver<String>) -> Self {
        ZmqConnector {
            cfg,
            tx: Rc::new(RefCell::new(tx)),
            rx: Rc::new(RefCell::new(rx)),
        }
    }

    pub async fn start(&mut self) {
        let pub_cfg = self.cfg.clone();
        let sub_cfg = self.cfg.clone();
        let pub_tx = self.tx.clone();
        let sub_rx = self.rx.clone();

        let publisher = async {
            let zpublisher = Arc::new(Mutex::new(ZmqPublisher::new(pub_cfg)));
            while let Some(msg) = sub_rx.borrow_mut().next().await {
                let mqtt_msg: MqttMessage = match serde_json::from_str(&msg) {
                    Ok(val) => { val }
                    Err(err) => {
                        error!("Parsing JSON message was unsuccessful: {}", err);
                        continue;
                    }
                };

                let mut message = phoenix::message::new();
                message.topic = mqtt_msg.topic;
                message.payload = mqtt_msg.payload;

                zpublisher.lock().unwrap().publish(message.write_to_bytes().unwrap());
            }
        };

        let subscriber = async {
            let zsubscriber = Arc::new(Mutex::new(ZmqSubscriber::new(sub_cfg)));
            while let (_, proto) = (&mut *zsubscriber.lock().unwrap()).await {
                let message: phoenix::message = protobuf::parse_from_bytes(proto.as_bytes()).unwrap();
                let topic = message.topic;
                let payload = message.payload;

                let mqtt_msg = MqttMessage { topic, payload };
                pub_tx.borrow_mut().send(serde_json::to_string(&mqtt_msg).unwrap()).await.unwrap();
            }
        };

        let (_, _) = futures::join!(publisher, subscriber);
    }
}


struct ZmqPublisher {
    cfg: Rc<config::Config>,
    zsock: zmq::Socket,
}

impl ZmqPublisher {
    pub fn new(cfg: Rc<config::Config>) -> Self {
        let zctx = zmq::Context::new();
        let zsock = zctx.socket(zmq::PUB).unwrap();

        let addr = cfg.get_str("Config.Connector.Firmware.ZMQ.Pub.Addr").unwrap();
        zsock
            .bind(addr.as_str())
            .expect("could not bind publisher socket");

        ZmqPublisher {
            cfg,
            zsock,
        }
    }

    pub fn publish(&mut self, msg: Vec<u8>) {
        let topic = self.cfg.get_str("Config.Connector.Firmware.ZMQ.Pub.Topic").unwrap();
        self.zsock.send(topic.as_bytes(), zmq::SNDMORE).unwrap();
        self.zsock.send(msg, 0).unwrap();
    }
}


struct ZmqSubscriber {
    inner: ZmqSub,
}

impl ZmqSubscriber {
    fn new(cfg: Rc<config::Config>) -> Self {
        let inner = ZmqSub::new(cfg.clone());
        ZmqSubscriber {
            inner
        }
    }
}

impl Future for ZmqSubscriber {
    type Output = (String, String);

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.inner.has_data_to_read() {
            Poll::Ready(self.inner.read_data())
        } else {
            self.inner.set_readable_callback(cx.waker().clone());
            Poll::Pending
        }
    }
}

struct ZmqSub {
    inner_arc: Arc<Mutex<InnerZmqSub>>,
}

impl ZmqSub {
    fn new(cfg: Rc<config::Config>) -> Self {
        let inner = InnerZmqSub { data: VecDeque::new(), readable_waker: None };
        let inner_arc = Arc::new(Mutex::new(inner));

        let zctx = zmq::Context::new();
        let zsock = zctx.socket(zmq::SUB).unwrap();

        let addr = cfg.get_str("Config.Connector.Firmware.ZMQ.Sub.Addr").unwrap();
        zsock
            .connect(addr.as_str())
            .expect("failed connecting subscriber");

        let topic = cfg.get_str("Config.Connector.Firmware.ZMQ.Sub.Topic").unwrap();
        zsock
            .set_subscribe(topic.as_bytes())
            .expect("failed subscribing");

        let this_inner_arc = inner_arc.clone();
        thread::spawn(move || {
            loop {
                let topic = zsock
                    .recv_string(0)
                    .expect("failed receiving topic")
                    .expect("failed unwrapping topic");

                let payload = zsock
                    .recv_string(0)
                    .expect("failed receiving payload")
                    .expect("failed unwrapping payload");

                let mut inner = this_inner_arc.lock().unwrap();
                inner.data.push_back((topic, payload));
                if let Some(w) = inner.readable_waker.take() {
                    w.wake()
                }
            }
        });

        ZmqSub { inner_arc }
    }

    fn has_data_to_read(&self) -> bool {
        let inner = self.inner_arc.lock().unwrap();
        !inner.data.is_empty()
    }

    fn read_data(&self) -> (String, String) {
        let mut inner = self.inner_arc.lock().unwrap();
        let (topic, payload) = inner.data.pop_front().unwrap();
        (topic, payload)
    }

    #[allow(dead_code)]
    fn set_readable_callback(&self, waker: Waker) {
        let mut inner = self.inner_arc.lock().unwrap();
        inner.readable_waker = Some(waker);
    }
}

struct InnerZmqSub {
    data: VecDeque<(String, String)>,
    readable_waker: Option<Waker>,
}

impl Drop for ZmqConnector {
    fn drop(&mut self) {
        trace!("Dropping ZmqConnector...");
    }
}

impl Drop for ZmqPublisher {
    fn drop(&mut self) {
        trace!("Dropping ZmqPublisher...");
    }
}

impl Drop for ZmqSubscriber {
    fn drop(&mut self) {
        trace!("Dropping ZmqSubscriber...");
    }
}

impl Drop for ZmqSub {
    fn drop(&mut self) {
        trace!("Dropping ZmqSub...");
    }
}
