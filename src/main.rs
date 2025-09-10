use std::io::{self, Write};
use std::process;

enum ExecMode {
    Command,
    Statement,
}

fn get_exec_mode(input: &str) -> ExecMode {
    if input.starts_with(".") { return ExecMode::Command; }
    ExecMode::Statement
}

fn main() {
    let mut buf = String::new();
    loop {
        print!("rqlite> ");
        io::stdout().flush().expect("ERROR: flush");
        let n = io::stdin().read_line(&mut buf).unwrap_or_else(|error| {
            eprintln!("ERROR: read_line fail: {error}");
            process::exit(1);
        });
        if n == 0 { break; } // ctrl+d
        let input = buf.trim();
        if input == "" { continue; }
        match get_exec_mode(input) {
            ExecMode::Command => {
                match input {
                    ".exit" => break,
                    _ => println!("unknown command: '{}'", input),
                }
            },
            ExecMode::Statement => {
                let mut tokens = input.split([' ', '\t']);
                let first_token = tokens.next().expect("ERROR: empty statement keyword");
                match first_token {
                    "insert" => println!("insert something!"),
                    "select" => println!("select something!"),
                    _ => println!("unkown statement keyword: '{input}'")
                }
            },
        }
        buf.clear();
    }
}
