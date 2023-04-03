use clap::{arg, Command};
use kvs::KvStore;
use std::process::exit;

fn main() {
    let mut kv = KvStore::new();
    let matches = cli().get_matches();
    match matches.subcommand() {
        Some(("set", sub_matches)) => {
            let key = sub_matches.get_one::<String>("KEY").expect("require");
            let val = sub_matches.get_one::<String>("VALUE").expect("require");
            println!("Set {} {}", key, val);
            kv.set(key.to_owned(), val.to_owned());
            eprintln!("unimplemented");
            exit(1);
        }
        Some(("get", sub_matches)) => {
            let key = sub_matches.get_one::<String>("KEY").expect("require");
            println!("Get {}", key);
            if let Some(val) = kv.get(key.to_owned()) {
                println!("Val {}", val);
            } else {
                println!("None");
            }

            eprintln!("unimplemented");
            exit(1);
        }
        Some(("rm", sub_matches)) => {
            let key = sub_matches.get_one::<String>("KEY").expect("require");
            println!("RM {}", key);
            kv.remove(key.to_owned());

            eprintln!("unimplemented");
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
