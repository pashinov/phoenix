use std::{rc::Rc};

use futures::channel::mpsc;
use futures::sink::SinkExt;
use futures::stream::StreamExt;
use log::{debug, trace};

pub struct HandlerTx {
    _cfg: Rc<config::Config>,
    tx: mpsc::Sender<String>,
    rx: mpsc::Receiver<String>,
}

impl HandlerTx {
    pub fn new(cfg: Rc<config::Config>, tx: mpsc::Sender<String>, rx: mpsc::Receiver<String>) -> Self {
        HandlerTx {
            _cfg: cfg,
            tx,
            rx,
        }
    }

    pub async fn start(&mut self) -> () {
        loop {
            let msg = self.rx.next().await.unwrap();
            debug!("Received message from firmware connector: '{}'", msg);

            // TODO: implement message handler
            // self.tx.send(msg).await.unwrap();
        }
    }
}

impl Drop for HandlerTx {
    fn drop(&mut self) {
        trace!("Dropping HandlerTx...");
    }
}

pub struct HandlerRx {
    _cfg: Rc<config::Config>,
    tx: mpsc::Sender<String>,
    rx: mpsc::Receiver<String>,
}

impl HandlerRx {
    pub fn new(cfg: Rc<config::Config>, tx: mpsc::Sender<String>, rx: mpsc::Receiver<String>) -> Self {
        HandlerRx {
            _cfg: cfg,
            tx,
            rx,
        }
    }

    pub async fn start(&mut self) -> () {
        loop {
            let msg = self.rx.next().await.unwrap();
            debug!("Received message from mqtt connector: '{}'", msg);

            // TODO: implement message handler
            //self.tx.send(msg).await.unwrap();
        }
    }
}

impl Drop for HandlerRx {
    fn drop(&mut self) {
        trace!("Dropping HandlerRx...");
    }
}
