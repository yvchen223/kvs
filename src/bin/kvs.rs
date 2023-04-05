use std::env;
use clap::{arg, Command};
use kvs::{err, KvStore};
use std::process::exit;
use err::Result;
use kvs::err::KvError;

fn main() -> Result<()> {
    let mut kv =  match KvStore::open(env::current_dir()?) {
        Ok(kv) => kv,
        Err(e) => return Err(e),
    };
    let matches = cli().get_matches();
    match matches.subcommand() {
        Some(("set", sub_matches)) => {
            let key = sub_matches.get_one::<String>("KEY").expect("require");
            let val = sub_matches.get_one::<String>("VALUE").expect("require");
            if let Err(e) = kv.set(key.to_owned(), val.to_owned()) {
              eprintln!("err: {:?}", e);
            };
            exit(0);
        }
        Some(("get", sub_matches)) => {
            let key = sub_matches.get_one::<String>("KEY").expect("require");
            match kv.get(key.to_owned()) {
                Ok(opt) => {
                    if let Some(val) = opt {
                        println!("{}", val);
                    } else {
                        println!("Key not found");
                    }
                },
                Err(e) => eprintln!("{:?}", e),
            };
            exit(0);
        }
        Some(("rm", sub_matches)) => {
            let key = sub_matches.get_one::<String>("KEY").expect("require");
            match kv.remove(key.to_owned()) {
                Ok(_) => exit(0),
                Err(KvError::RecordNotFound) => println!("Key not found"),
                Err(e) => eprintln!("{:?}", e),
            }

            exit(1);
        }
        _ => {
            eprintln!("unimplemented");
            exit(1);
        }
    }
}

fn cli() -> Command {
    Command::new(env!("CARGO_PKG_NAME"))
        .about("A key-value store")
        .version(env!("CARGO_PKG_VERSION"))
        .long_version(env!("CARGO_PKG_VERSION"))
        .subcommand_required(true)
        .arg_required_else_help(true)
        .allow_external_subcommands(true)
        .subcommand(
            Command::new("set")
                .about("set key and value to store")
                .args([arg!([KEY] "key"), arg!([VALUE] "value")])
                .arg_required_else_help(true),
        )
        .subcommand(
            Command::new("get")
                .about("get value from store")
                .arg(arg!([KEY] "key"))
                .arg_required_else_help(true),
        )
        .subcommand(
            Command::new("rm")
                .about("remove a pair of key-value")
                .arg(arg!([KEY] "key"))
                .arg_required_else_help(true),
        )
}

