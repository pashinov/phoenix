use std::{rc::Rc};
use std::str::FromStr;

use async_signals::Signals;
use clap::{App, Arg};
use daemonize::Daemonize;
use futures::channel::mpsc;
use futures::future::{Abortable, AbortHandle};
use futures::stream::StreamExt;
use log4rs::append::console::ConsoleAppender;
use log4rs::append::file::FileAppender;
use log4rs::config::{Appender, Config, Root};
use log4rs::encode::pattern::PatternEncoder;
use log::info;
use log::LevelFilter;

use connector::firmware::unix_sockets::{UnixClient, UnixServer};
use connector::mqtt::paho_client::PahoClient;
use handler::{HandlerRx, HandlerTx};

mod connector;
mod handler;
mod data;

async fn run_service(settings: config::Config) -> () {
    let cfg = Rc::new(settings);
    let (firmware_sender_tx, firmware_sender_rx) = mpsc::channel::<String>(100);
    let (firmware_receiver_tx, firmware_receiver_rx) = mpsc::channel::<String>(100);
    let (mqtt_sender_tx, mqtt_sender_rx) = mpsc::channel::<String>(100);
    let (mqtt_receiver_tx, mqtt_receiver_rx) = mpsc::channel::<String>(100);

    let mut paho_client = PahoClient::new(cfg.clone(), mqtt_receiver_tx, mqtt_sender_rx);
    let paho_client_connect = paho_client.connect();

    let mut unix_client = UnixClient::new(cfg.clone(), firmware_receiver_rx);
    let unix_client_connect = unix_client.connect();

    let mut unix_server = UnixServer::new(cfg.clone(), firmware_sender_tx);
    let unix_server_start = unix_server.start();

    let mut handler_tx = HandlerTx::new(cfg.clone(), mqtt_sender_tx, firmware_sender_rx);
    let handler_tx_start = handler_tx.start();

    let mut handler_rx = HandlerRx::new(cfg.clone(), firmware_receiver_tx, mqtt_receiver_rx);
    let handler_rx_start = handler_rx.start();

    let (paho_client_abort_handle, paho_client_abort_registration) = AbortHandle::new_pair();
    let abortable_paho_client = Abortable::new(paho_client_connect, paho_client_abort_registration);

    let (unix_client_abort_handle, unix_client_abort_registration) = AbortHandle::new_pair();
    let abortable_unix_client = Abortable::new(unix_client_connect, unix_client_abort_registration);

    let (unix_server_abort_handle, unix_server_abort_registration) = AbortHandle::new_pair();
    let abortable_unix_server = Abortable::new(unix_server_start, unix_server_abort_registration);

    let (handler_tx_abort_handle, handler_tx_abort_registration) = AbortHandle::new_pair();
    let abortable_handler_tx = Abortable::new(handler_tx_start, handler_tx_abort_registration);

    let (handler_rx_abort_handle, handler_rx_abort_registration) = AbortHandle::new_pair();
    let abortable_handler_rx = Abortable::new(handler_rx_start, handler_rx_abort_registration);

    let abort = async {
        let mut signals = Signals::new(vec![]).unwrap();
        signals.add_signal(libc::SIGTERM).unwrap();

        loop {
            let signal = signals.next().await.unwrap();
            match signal {
                libc::SIGTERM => {
                    paho_client_abort_handle.abort();
                    unix_client_abort_handle.abort();
                    unix_server_abort_handle.abort();
                    handler_tx_abort_handle.abort();
                    handler_rx_abort_handle.abort();
                    break;
                }
                _ => {}
            }
        }
    };

    let (_, _, _, _, _, _) = futures::join!(abortable_paho_client, abortable_unix_client, abortable_unix_server, abortable_handler_tx, abortable_handler_rx, abort);
}

fn main() {
    // Parsing arguments
    let args = App::new("Phoenix")
        .version("0.1.0")
        .author("Pashinov A. <pashinov93@gmail.com>")
        .arg(Arg::with_name("daemon")
            .short("d")
            .long("daemon")
            .help("Run as daemon")
            .takes_value(false))
        .arg(Arg::with_name("config")
            .short("c")
            .long("config")
            .value_name("FILE")
            .help("Sets a custom config file")
            .takes_value(true))
        .get_matches();

    // Init configuration
    let mut settings = config::Config::default();
    let config_file = args.value_of("config").unwrap_or("/home/parallels/git/phoenix/conf/default.json");
    settings.merge(config::File::with_name(config_file)).unwrap();

    // Init logging
    let log_filename = settings.get_str("Config.System.Logging.Path").unwrap();
    let log_level = LevelFilter::from_str(&settings.get_str("Config.System.Logging.Level").unwrap()).unwrap();

    let stdout = ConsoleAppender::builder().build();
    let logfile = FileAppender::builder()
        .encoder(Box::new(PatternEncoder::new("{d} [{l}] - {m}\n")))
        .build(log_filename).unwrap();

    let log_config = Config::builder()
        .appender(Appender::builder().build("stdout", Box::new(stdout)))
        .appender(Appender::builder().build("logfile", Box::new(logfile)))
        .build(Root::builder()
            .appender("logfile")
            .appender("stdout")
            .build(log_level)).unwrap();

    log4rs::init_config(log_config).unwrap();

    // Run service
    if args.is_present("daemon") {
        let daemon = Daemonize::new()
            .pid_file(settings.get_str("Config.System.Daemon.PidPath").unwrap())
            .working_directory("/")
            .umask(0o027)
            .privileged_action(|| info!("Running application as a daemon..."));

        match daemon.start() {
            Ok(_) => { futures::executor::block_on(run_service(settings)); }
            Err(err) => { panic!("Running the daemon: {}", err) }
        }
    } else {
        info!("Running application in console mode...");
        futures::executor::block_on(run_service(settings));
    }
}
