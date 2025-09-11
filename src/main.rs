use std::error::Error;
use std::io::{self, Write};
use std::mem;
use std::process;

const PAGE_SIZE: usize = 4096;
const MAX_TABLE_PAGES: usize = 64;

struct Table {
    row_count: u64,
    pages: [Option<Vec<Row>>; MAX_TABLE_PAGES],
}

struct Row {
    id: u32,
    name: [u8; 32],
    description: [u8; 256],
}

impl Table {
    fn new() -> Self {
        Table {
            row_count: 0,
            pages: [const { None }; MAX_TABLE_PAGES],
        }
    }

    fn insert(&mut self, args: &[&str]) -> Result<(), Box<dyn Error>> {
        if args.len() != 3 {
            return Err("syntax error: insert <id> <name> <description>".into());
        }
        let id = args[0]
            .parse::<u32>()
            .map_err(|_| "syntax error: insert <id> <name> <description>")?;
        let name = args[1];
        let description = args[2];
        let rows_per_page = PAGE_SIZE / mem::size_of::<Row>();
        let total_rows = rows_per_page * MAX_TABLE_PAGES;
        if total_rows == self.row_count as usize {
            return Err("table reach max size".into());
        }
        let mut name_buf = [0u8; 32];
        let mut description_buf = [0u8; 256];
        let ceiling = name.len().min(32);
        name_buf[0..ceiling].copy_from_slice(&name.as_bytes()[0..ceiling]);
        let ceiling = description.len().min(256);
        description_buf[0..ceiling].copy_from_slice(&description.as_bytes()[0..ceiling]);
        let new_row = Row {
            id,
            name: name_buf,
            description: description_buf,
        };
        let page_index = self.row_count as usize / rows_per_page;
        self.pages[page_index]
            .get_or_insert_with(Vec::new)
            .push(new_row);
        self.row_count += 1;
        Ok(())
    }

    fn select(&self) {
        self.pages
            .iter()
            .flatten() // Option<Vec<Row>> -> Vec<Row>
            .flatten() // Vec<Row> -> Row
            .for_each(|row| println!("{}. {} {}", row.id, str::from_utf8(&row.name).unwrap(), str::from_utf8(&row.description).unwrap()));
    }
}

fn main() {
    let mut table = Table::new();
    let mut buf = String::new();
    loop {
        print!("rqlite> ");
        io::stdout().flush().expect("ERROR: flush");
        let n = io::stdin().read_line(&mut buf).unwrap_or_else(|error| {
            eprintln!("ERROR: read_line fail: {error}");
            process::exit(1);
        });
        if n == 0 {
            break;
        } // ctrl+d
        let input = buf.trim();
        if input.is_empty() {
            continue;
        }
        if input.starts_with(".") {
            // exec metacommand
            match input {
                ".exit" => break,
                _ => println!("error: unknown command: '{input}'"),
            }
        } else {
            // exec statement
            let tokens = input.split([' ', '\t']).collect::<Vec<_>>();
            match tokens[0] {
                "insert" => match table.insert(&tokens[1..]) {
                    Ok(()) => println!("success!"),
                    Err(e) => println!("{e}"),
                },
                "select" => {
                    table.select();
                    println!("success!");
                }
                _ => println!("error: unkown statement keyword: '{input}'"),
            }
        }
        buf.clear();
    }
}
