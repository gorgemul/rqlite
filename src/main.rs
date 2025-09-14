use std::error::Error;
use std::io::prelude::*;
use std::io::{self, BufReader, SeekFrom};
use std::mem;
use std::process;
use std::env;
use std::fs::{File, OpenOptions};
use std::os::unix::fs::FileExt;

const PAGE_SIZE: usize = 4096;
const NAME_MAX_LEN: usize = 32;
const DESCRIPTION_MAX_LEN: usize = 256;
const PAGE_MAX_LEN: usize = 64;
const ROW_SIZE: usize = mem::size_of::<Row>();
const ROWS_PER_PAGE: usize = PAGE_SIZE / ROW_SIZE;

struct Table {
    row_count: usize,
    pager: Pager,
}

struct Pager {
    file: File,
    pages: [Option<Vec<Row>>; PAGE_MAX_LEN],
}

struct Row {
    id: i32,
    name: [u8; NAME_MAX_LEN],
    description: [u8; DESCRIPTION_MAX_LEN],
}

impl Table {
    fn new(pager: Pager) -> Self {
        let file_len = pager.file.metadata().unwrap().len() as usize;
        let n_pages = file_len / PAGE_SIZE;
        let extra_len = file_len % PAGE_SIZE; // could be extra rows
        Table {
            row_count: (n_pages * ROWS_PER_PAGE) + (extra_len / ROW_SIZE),
            pager,
        }
    }

    fn insert(&mut self, args: &[&str]) -> Result<(), Box<dyn Error>> {
        // TODO: parse ""
        if args.len() != 3 {
            return Err("syntax error: insert <id> <name> <description>".into());
        }
        let id = args[0]
            .parse::<i32>()
            .map_err(|_| "syntax error: insert <id> <name> <description>")?;
        if id <= 0 {
            return Err("id must be greater than 0".into());
        }
        let name = args[1];
        if name.len() > NAME_MAX_LEN {
            return Err("name too long".into());
        }
        let description = args[2];
        if description.len() > DESCRIPTION_MAX_LEN {
            return Err("description too long".into());
        }
        let mut name_buf = [0u8; NAME_MAX_LEN];
        let mut description_buf = [0u8; DESCRIPTION_MAX_LEN];
        let ceiling = name.len().min(NAME_MAX_LEN);
        name_buf[0..ceiling].copy_from_slice(&name.as_bytes()[0..ceiling]);
        let ceiling = description.len().min(DESCRIPTION_MAX_LEN);
        description_buf[0..ceiling].copy_from_slice(&description.as_bytes()[0..ceiling]);
        let new_row = Row {
            id,
            name: name_buf,
            description: description_buf,
        };
        let page_index = self.row_count / ROWS_PER_PAGE;
        self.pager.get_page(page_index)?.push(new_row);
        self.row_count += 1;
        Ok(())
    }

    // actually don't need &mut here, but for the sake of compiler complain,
    // or just wrote two get_page method, one for &mut and the other for &
    fn select(&mut self) {
        let mut n_pages = self.row_count / ROWS_PER_PAGE;
        if self.row_count % ROWS_PER_PAGE != 0 {
            n_pages += 1;
        }
        for page_index in 0..n_pages {
            let page = self.pager.get_page(page_index).unwrap();
            if page.is_empty() {
                break;
            }
            for row in page {
                println!(
                    "{}. {} {}",
                    row.id,
                    str::from_utf8(&row.name).unwrap(),
                    str::from_utf8(&row.description).unwrap()
                )
            }
        }
    }
}

impl Drop for Table {
    fn drop(&mut self) {
        for page_index in 0..PAGE_MAX_LEN {
            if let Err(error) = self.pager.flush_page(page_index) {
                eprintln!("ERROR: db close {error}");
                process::exit(1);
            }
        }
    }
}

impl Pager {
    fn new(path: &str) -> Result<Self, io::Error> {
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(path)?;
        Ok(Pager{
            file,
            pages: [const { None }; PAGE_MAX_LEN],
        })
    }

    fn get_page(&mut self, page_index: usize) -> Result<&mut Vec<Row>, Box<dyn Error>> {
        if page_index >= PAGE_MAX_LEN {
            return Err("table reach max size".into());
        }
        let file_len = self.file.metadata()?.len() as usize;
        let n_pages = file_len / PAGE_SIZE;
        if self.pages[page_index].is_some() {
            return Ok(self.pages[page_index].as_mut().unwrap());
        }
        let mut new_page = Vec::new();
        if page_index < n_pages {
            let offset = (page_index * PAGE_SIZE) as u64;
            self.file.seek(SeekFrom::Start(offset))?;
            let mut reader = BufReader::new(&mut self.file);
            let mut n_read: usize = 0;
            while n_read < ROWS_PER_PAGE {
                let mut id_buf = [0u8; 4];
                let mut name_buf = [0u8; NAME_MAX_LEN];
                let mut description_buf = [0u8; DESCRIPTION_MAX_LEN];
                reader.read_exact(&mut id_buf)?;
                let id = i32::from_le_bytes(id_buf);
                reader.read_exact(&mut name_buf)?;
                reader.read_exact(&mut description_buf)?;
                new_page.push(Row { id, name: name_buf, description: description_buf });
                n_read += 1;
            }
        }
        if page_index == n_pages && file_len % PAGE_SIZE != 0 { // right before last page append some rows don't have whole page size
            let offset = (page_index * PAGE_SIZE) as u64;
            self.file.seek(SeekFrom::Start(offset))?;
            let mut reader = BufReader::new(&mut self.file);
            let n_rows = (file_len % PAGE_SIZE) / ROW_SIZE;
            for _ in 0..n_rows {
                let mut id_buf = [0u8; 4];
                let mut name_buf = [0u8; NAME_MAX_LEN];
                let mut description_buf = [0u8; DESCRIPTION_MAX_LEN];
                reader.read_exact(&mut id_buf)?;
                let id = i32::from_le_bytes(id_buf);
                reader.read_exact(&mut name_buf)?;
                reader.read_exact(&mut description_buf)?;
                new_page.push(Row { id, name: name_buf, description: description_buf });
            }
        }
        self.pages[page_index] = Some(new_page);
        Ok(self.pages[page_index].as_mut().unwrap())
    }

    fn flush_page(&mut self, page_index: usize) -> Result<(), io::Error> {
        if self.pages[page_index].is_none() {
            return Ok(());
        }
        let page_offset = page_index * PAGE_SIZE;
        let page = self.pages[page_index].as_ref().unwrap();
        let mut n_rows: usize = 0;
        for row in page {
            let mut offset = (page_offset + n_rows * ROW_SIZE) as u64;
            self.file.write_at(&row.id.to_le_bytes(), offset)?;
            offset += 4;
            self.file.write_at(&row.name, offset)?;
            offset += NAME_MAX_LEN as u64;
            self.file.write_at(&row.description, offset)?;
            n_rows += 1;
        }
        let has_extra_rows = page.len() != ROWS_PER_PAGE; // only happen in last page
        if !has_extra_rows {
            let offset = (page_offset + ROWS_PER_PAGE * ROW_SIZE) as u64;
            let padding = vec![0u8; PAGE_SIZE - ROWS_PER_PAGE * ROW_SIZE];
            self.file.write_at(&padding, offset)?;
        }
        self.file.flush()?;
        Ok(())
    }
}

fn main() {
    let args: Vec<_> = env::args().collect();
    if args.len() != 2 {
        eprintln!("USAGE: rqlite <database>");
        process::exit(1);
    }
    let pager = Pager::new(&args[1]).unwrap_or_else(|error| {
        eprintln!("ERROR: init pager: {error}");
        process::exit(1);
    });
    let mut table = Table::new(pager);
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
