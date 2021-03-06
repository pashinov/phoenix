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
use log::{trace};

pub struct ZmqConnector {
    cfg: Rc<config::Config>,
    tx: Rc<RefCell<mpsc::Sender<Vec<u8>>>>,
    rx: Rc<RefCell<mpsc::Receiver<Vec<u8>>>>,
}

impl ZmqConnector {
    pub fn new(cfg: Rc<config::Config>, tx: mpsc::Sender<Vec<u8>>, rx: mpsc::Receiver<Vec<u8>>) -> Self {
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
                zpublisher.lock().unwrap().publish(msg);
            }
        };

        let subscriber = async {
            let zsubscriber = Arc::new(Mutex::new(ZmqSubscriber::new(sub_cfg)));
            while let (_, proto) = (&mut *zsubscriber.lock().unwrap()).await {
                pub_tx.borrow_mut().send(proto).await.unwrap();
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
    type Output = (Vec<u8>, Vec<u8>);

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
                    .recv_bytes(0)
                    .expect("failed receiving topic");

                let payload = zsock
                    .recv_bytes(0)
                    .expect("failed receiving payload");

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

    fn read_data(&self) -> (Vec<u8>, Vec<u8>) {
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
    data: VecDeque<(Vec<u8>, Vec<u8>)>,
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
