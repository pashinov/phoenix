use std::{rc::Rc};
use std::str::FromStr;

use async_signals::Signals;
use clap::{App, Arg};
use daemonize::Daemonize;
use futures::channel::mpsc;
use futures::future::{Abortable, AbortHandle};
use futures::stream::StreamExt;
use log::{info, warn};
use log4rs::append::console::ConsoleAppender;
use log4rs::append::file::FileAppender;
use log4rs::config::{Appender, Config, Root};
use log4rs::encode::pattern::PatternEncoder;
use log::LevelFilter;

use connector::firmware::zmq::ZmqConnector;
use connector::mqtt::paho::PahoConnector;

mod connector;
mod data;
mod phoenix;

async fn run_service(settings: config::Config) -> () {
    let cfg = Rc::new(settings);

    let (firmware_sender_tx, mqtt_sender_rx) = mpsc::channel::<String>(100);
    let (mqtt_receiver_tx, firmware_receiver_rx) = mpsc::channel::<String>(100);

    let mut mqtt_connector = PahoConnector::new(cfg.clone(), mqtt_receiver_tx, mqtt_sender_rx);
    let mqtt_connector_connect = mqtt_connector.start();

    let mut firmware_connector = ZmqConnector::new(cfg.clone(), firmware_sender_tx, firmware_receiver_rx);
    let firmware_connector_start = firmware_connector.start();

    let (mqtt_connector_abort_handle, mqtt_connector_abort_registration) = AbortHandle::new_pair();
    let abortable_mqtt_connector = Abortable::new(mqtt_connector_connect, mqtt_connector_abort_registration);

    let (firmware_connector_abort_handle, firmware_connector_abort_registration) = AbortHandle::new_pair();
    let abortable_firmware_connector = Abortable::new(firmware_connector_start, firmware_connector_abort_registration);

    let abort = async {
        let mut signals = Signals::new(vec![]).unwrap();
        signals.add_signal(libc::SIGTERM).unwrap();
        signals.add_signal(libc::SIGINT).unwrap();
        signals.add_signal(libc::SIGHUP).unwrap();

        loop {
            let signal = signals.next().await.unwrap();
            match signal {
                libc::SIGTERM | libc::SIGINT => {
                    firmware_connector_abort_handle.abort();
                    mqtt_connector_abort_handle.abort();
                    break;
                }
                _ => {
                    warn!("Signal handler not found");
                    continue;
                }
            }
        }
    };

    let (_, _, _) = futures::join!(abortable_firmware_connector, abortable_mqtt_connector, abort);
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
    let config_file = args.value_of("config").unwrap_or("/etc/phoenix/phoenix.json");
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
            Ok(_) => {
                info!("Running application in daemon mode...");
                futures::executor::block_on(run_service(settings));
            }
            Err(err) => { panic!("Running the daemon: {}", err) }
        }
    } else {
        info!("Running application in console mode...");
        futures::executor::block_on(run_service(settings));
    }
}
