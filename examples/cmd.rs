use std::io::{self, Write};
use std::process;

use slmlib::lsm::{self, KVStore};

pub fn run() {
    println!("Welcome to bitcask database!");

    let path = "database";
    let mut db = lsm::OpenOptions::new()
        .max_log_length(50)
        .merge_window(3)
        .open(path)
        .unwrap();

    loop {
        let mut cmd = String::new();
        io::stdout().write("> ".as_bytes()).unwrap();
        io::stdout().flush().unwrap();

        io::stdin()
            .read_line(&mut cmd)
            .expect("Failed to read command");

        let cmd = cmd.strip_suffix("\n").unwrap();
        let cmds: Vec<&str> = cmd.split(" ").collect();

        match cmds[0] {
            "exit" => {
                println!("Bye!");
                drop(db);
                process::exit(0);
            }
            "help" => help(),
            "set" | "get" | "ls" | "rm" | "merge" => {
                process_db_command(&mut db, &cmds);
            }
            "" => empty(),
            _ => {
                println!("{}", cmds.join("-"));
            }
        };
    }
}

fn process_db_command(db: &mut lsm::Lsm, cmds: &Vec<&str>) {
    match cmds[0] {
        "set" => {
            db.put(cmds[1].as_bytes().to_vec(), cmds[2].as_bytes().to_vec())
                .unwrap();
        }
        "get" => {
            let value = db.get(cmds[1].as_bytes()).unwrap();
            if let Some(value) = value {
                println!("{}", String::from_utf8(value).unwrap());
            }
        }
        "ls" => {
            let keys = db.list_keys().unwrap();
            for key in keys {
                println!("{}", String::from_utf8(key.clone()).unwrap());
            }
        }
        "rm" => {
            db.delete(cmds[1].as_bytes()).unwrap();
        }
        "merge" => {
            todo!()
            // db.merge().unwrap();
        }
        &_ => todo!(),
    };
}

fn empty() {}

fn help() {
    println!("help -- show help");
    println!("get  -- get key value, by: <key>");
    println!("set  -- set key value, by: <key> <value>");
    println!("ls   -- list keys");
    println!("rm   -- remove key value, by: <key>");
    println!("exit -- exit command");
}

fn main() {
    env_logger::init();

    run();
}
