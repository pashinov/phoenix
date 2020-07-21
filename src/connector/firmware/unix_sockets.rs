use std::{rc::Rc};
use std::fs;
use std::path::Path;
use std::time::Duration;

use futures::channel::mpsc;
use futures::sink::SinkExt;
use futures::stream::StreamExt;
use log::{info, warn};

use async_std::io::BufReader;
use async_std::io::prelude::*;
use async_std::os::unix::net::{UnixListener, UnixStream};
use async_std::task;

pub struct UnixClient {
    cfg: Rc<config::Config>,
    rx: mpsc::Receiver<String>,
}

impl UnixClient {
    pub fn new(cfg: Rc<config::Config>, rx: mpsc::Receiver<String>) -> Self {
        UnixClient {
            cfg,
            rx,
        }
    }

    pub async fn connect(&mut self) -> () {
        loop {
            let addr = self.cfg.get_str("Config.Connector.Firmware.UnixSocket.Client.Addr").unwrap();
            match UnixStream::connect(addr.as_str()).await {
                Ok(mut stream) => {
                    info!("Connection to GPShield successful");
                    loop {
                        let msg = self.rx.next().await.unwrap();
                        stream.write_all(msg.as_bytes()).await.unwrap();
                        info!("Message RX: {}", msg);
                    }
                }
                Err(err) => {
                    warn!("Connection attempt to GPShield failed: {:?}", err);
                    task::sleep(Duration::from_secs(5)).await;
                    continue;
                }
            };
        }
    }
}

impl Drop for UnixClient {
    fn drop(&mut self) {
        info!("Dropping UnixClient...");
    }
}

pub struct UnixServer {
    cfg: Rc<config::Config>,
    tx: mpsc::Sender<String>,
}

impl UnixServer {
    pub fn new(cfg: Rc<config::Config>, tx: mpsc::Sender<String>) -> Self {
        UnixServer {
            cfg,
            tx,
        }
    }

    pub async fn start(&mut self) -> () {
        let addr = self.cfg.get_str("Config.Connector.Firmware.UnixSocket.Server.Addr").unwrap();
        if Path::new(addr.as_str()).exists() {
            fs::remove_file(addr.as_str()).unwrap();
        }

        let listener = UnixListener::bind(addr).await.unwrap();

        loop {
            match listener.accept().await {
                Ok((stream, _addr)) => {
                    info!("GPShield was connected successfully");
                    let mut reader = BufReader::new(&stream);
                    loop {
                        let mut byte_vec: Vec<u8> = Vec::new();
                        match reader.read_until(b'\0', &mut byte_vec).await {
                            Ok(bytes) => {
                                if bytes > 0 {
                                    let msg = std::str::from_utf8(&byte_vec[..bytes - 1]).unwrap();
                                    self.tx.send(msg.to_string()).await.unwrap();
                                } else if bytes == 0 {
                                    info!("GPShield was disconnected successfully");
                                    break;
                                } else {
                                    break;
                                }
                            }
                            Err(err) => {
                                warn!("Unix Server: read data failed: {:?}", err)
                            }
                        }
                    }
                }
                Err(err) => {
                    warn!("Unix Server: accept connection failed: {:?}", err)
                }
            }
        }
    }
}

impl Drop for UnixServer {
    fn drop(&mut self) {
        info!("Dropping UnixServer...");
    }
}
