use clap::{Arg, Command};
use kvs::{KvStore, KvsServer, SledKvsEngine};
use log::{error, info};
use std::env::current_dir;
use std::process::exit;
use std::{env, fs};

fn main() {
    env_logger::init();

    let matches = cli().get_matches();
    let addr = matches.get_one::<String>("addr").unwrap();
    let engine_name = matches.get_one::<String>("engine").unwrap();
    info!("kvs - {}", env!("CARGO_PKG_VERSION"));
    info!("ADDR {}", addr);
    info!("ENGINE-NAME {}", engine_name);

    let engine_file = current_dir().expect("cur die").join("engine");
    if !engine_file.exists() {
        fs::write(current_dir().unwrap().join("engine"), engine_name).expect("write engine err");
    }
    if fs::read_to_string(engine_file).expect("read engines file") != *engine_name {
        error!("unmatched engines");
        exit(1);
    }

    match engine_name.as_str() {
        "sled" => {
            let mut server = KvsServer::new(SledKvsEngine::new(current_dir().unwrap()).unwrap());
            server.run(addr).unwrap();
        }
        _ => {
            let mut server = KvsServer::new(KvStore::open(env::current_dir().unwrap()).unwrap());
            server.run(addr).unwrap();
        }
    }
}

fn cli() -> Command {
    Command::new("kvs-server")
        .about("A key-value store server")
        .version(env!("CARGO_PKG_VERSION"))
        .long_version(env!("CARGO_PKG_VERSION"))
        .arg_required_else_help(true)
        .arg(
            Arg::new("addr")
                .short('a')
                .long("addr")
                .value_name("ADDR")
                .default_value("127.0.0.1:4000")
                .ignore_case(true)
                .help("IP address"),
        )
        .arg(
            Arg::new("engine")
                .short('e')
                .long("engine")
                .value_name("ENGINE-NAME")
                .default_value("kvs")
                .help("engine name")
                .ignore_case(true),
        )
}
