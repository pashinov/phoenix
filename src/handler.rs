use std::{rc::Rc};
use std::time::Duration;

use futures::channel::mpsc;
use futures::sink::SinkExt;
use futures::stream::StreamExt;
use log::{info};

use async_std::task;

pub struct HandlerTx {
    _cfg: Rc<config::Config>,
    rx: mpsc::Receiver<String>,
}

impl HandlerTx {
    pub fn new(cfg: Rc<config::Config>, rx: mpsc::Receiver<String>) -> Self {
        HandlerTx {
            _cfg: cfg,
            rx,
        }
    }

    pub async fn start(&mut self) -> () {
        loop {
            let msg = self.rx.next().await.unwrap();
            info!("Message TX: {}", msg)
        }
    }
}

impl Drop for HandlerTx {
    fn drop(&mut self) {
        info!("Dropping HandlerTx...");
    }
}

pub struct HandlerRx {
    _cfg: Rc<config::Config>,
    tx: mpsc::Sender<String>,
}

impl HandlerRx {
    pub fn new(cfg: Rc<config::Config>, tx: mpsc::Sender<String>) -> Self {
        HandlerRx {
            _cfg: cfg,
            tx,
        }
    }

    pub async fn start(&mut self) -> () {
        loop {
            task::sleep(Duration::from_secs(15)).await;
            self.tx.send("Hello, World!!!\n".to_string()).await.unwrap();
        }
    }
}

impl Drop for HandlerRx {
    fn drop(&mut self) {
        info!("Dropping HandlerRx...");
    }
}
