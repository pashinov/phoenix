use std::rc::Rc;
use std::future::Future;
use std::pin::Pin;
use std::thread;

use futures::SinkExt;
use futures::channel::mpsc;
use log::{trace};
use std::task::{Context, Poll, Waker};
use std::sync::{Arc, Mutex};

use crate::data::MqttMessage;
use crate::phoenix;

pub struct ZmqConnector {
    cfg: Rc<config::Config>,
    tx: mpsc::Sender<String>,
    _rx: mpsc::Receiver<String>,
}

impl ZmqConnector {
    pub fn new(cfg: Rc<config::Config>, tx: mpsc::Sender<String>, _rx: mpsc::Receiver<String>) -> Self {
        ZmqConnector {
            cfg,
            tx,
            _rx
        }
    }

    pub async fn start(&mut self) {
        let subscriber  = Arc::new(Mutex::new(ZmqSubscriber::new(self.cfg.clone())));
        loop {
            let (_, proto) = (&mut *subscriber.lock().unwrap()).await;
            let message: phoenix::message = protobuf::parse_from_bytes(proto.as_bytes()).unwrap();
            let topic = message.topic;
            let payload = message.payload;

            let mqtt_msg = MqttMessage { topic,  payload };
            self.tx.send(serde_json::to_string(&mqtt_msg).unwrap()).await.unwrap();
        }
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
        let inner = InnerZmqSub { data: None, readable_waker: None };
        let inner_arc = Arc::new(Mutex::new(inner));

        let zcontext = zmq::Context::new();
        let zsubscriber = zcontext.socket(zmq::SUB).unwrap();

        let addr = cfg.get_str("Config.Connector.Firmware.ZMQ.Sub.Addr").unwrap();
        zsubscriber
            .connect(addr.as_str())
            .expect("failed connecting subscriber");

        let topic = cfg.get_str("Config.Connector.Firmware.ZMQ.Sub.Topic").unwrap();
        zsubscriber
            .set_subscribe(topic.as_bytes())
            .expect("failed subscribing");

        let this_inner_arc = inner_arc.clone();
        thread::spawn(move || {
            loop {
                let topic = zsubscriber
                    .recv_string(0)
                    .expect("failed receiving topic")
                    .expect("failed unwrapping topic");

                let payload = zsubscriber
                    .recv_string(0)
                    .expect("failed receiving payload")
                    .expect("failed unwrapping payload");

                let mut inner = this_inner_arc.lock().unwrap();
                inner.data = Some((topic, payload));
                if let Some(w) = inner.readable_waker.take() {
                    w.wake()
                }
            }
        });

        ZmqSub { inner_arc }
    }

    fn has_data_to_read(&self) -> bool {
        let inner = self.inner_arc.lock().unwrap();
        inner.data.is_some()
    }

    fn read_data(&self) -> (String, String) {
        let mut inner = self.inner_arc.lock().unwrap();
        // dirty hack to unwrap Option<String, String>
        let (topic, payload) = inner.data.clone().unwrap(); inner.data = None;
        (topic, payload)
    }

    #[allow(dead_code)]
    fn set_readable_callback(&self, waker: Waker) {
        let mut inner = self.inner_arc.lock().unwrap();
        inner.readable_waker = Some(waker);
    }
}

struct InnerZmqSub {
    data: Option<(String, String)>,
    readable_waker: Option<Waker>,
}

impl Drop for ZmqConnector {
    fn drop(&mut self) {
        trace!("Dropping ZmqConnector...");
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
