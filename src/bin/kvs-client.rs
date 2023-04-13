use clap::{arg, Arg, Command};
use err::Result;
use kvs::{err, KvsClient};
use std::env;
use std::process::exit;

fn main() -> Result<()> {
    env_logger::init();

    let matches = cli().get_matches();

    match matches.subcommand() {
        Some(("set", sub_matches)) => {
            let addr = sub_matches.get_one::<String>("addr").expect("addr");
            let mut client = KvsClient::new(addr)?;
            let key = sub_matches.get_one::<String>("KEY").expect("require");
            let val = sub_matches.get_one::<String>("VALUE").expect("require");
            client.set(key.to_owned(), val.to_owned())?;
        }
        Some(("get", sub_matches)) => {
            let addr = sub_matches.get_one::<String>("addr").expect("addr");
            let mut client = KvsClient::new(addr)?;
            let key = sub_matches.get_one::<String>("KEY").expect("require");
            let rsp = client.get(key.to_owned())?;
            match rsp {
                Some(val) => println!("{}", val),
                None => println!("Key not found"),
            }
        }
        Some(("rm", sub_matches)) => {
            let addr = sub_matches.get_one::<String>("addr").expect("addr");
            let mut client = KvsClient::new(addr)?;
            let key = sub_matches.get_one::<String>("KEY").expect("require");
            client.remove(key.to_owned())?;
        }
        _ => {
            eprintln!("unimplemented");
            exit(1);
        }
    }
    Ok(())
}

fn cli() -> Command {
    Command::new("kvs-client")
        .about("A key-value store client")
        .version(env!("CARGO_PKG_VERSION"))
        .long_version(env!("CARGO_PKG_VERSION"))
        .subcommand_required(true)
        .arg_required_else_help(true)
        .allow_external_subcommands(true)
        .subcommand(
            Command::new("set")
                .about("set key and value to store")
                .args([arg!([KEY] "key"), arg!([VALUE] "value")])
                .arg_required_else_help(true)
                .arg(
                    Arg::new("addr")
                        .short('a')
                        .long("addr")
                        .value_name("ADDR")
                        .default_value("127.0.0.1:4000")
                        .help("IP address"),
                ),
        )
        .subcommand(
            Command::new("get")
                .about("get value from store")
                .arg(arg!([KEY] "key"))
                .arg_required_else_help(true)
                .arg(
                    Arg::new("addr")
                        .short('a')
                        .long("addr")
                        .value_name("ADDR")
                        .default_value("127.0.0.1:4000")
                        .help("IP address"),
                ),
        )
        .subcommand(
            Command::new("rm")
                .about("remove a pair of key-value")
                .arg(arg!([KEY] "key"))
                .arg_required_else_help(true)
                .arg(
                    Arg::new("addr")
                        .short('a')
                        .long("addr")
                        .value_name("ADDR")
                        .default_value("127.0.0.1:4000")
                        .help("IP address"),
                ),
        )
}
