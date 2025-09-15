use std::env;
use std::error::Error;
use std::fs::{File, OpenOptions};
use std::io;
use std::io::prelude::*;
use std::mem;
use std::os::unix::fs::FileExt;
use std::process;

const PAGE_SIZE: usize = 4096;
const NAME_MAX_LEN: usize = 32;
const DESCRIPTION_MAX_LEN: usize = 256;
const PAGE_MAX_LEN: usize = 64;
const ROW_SIZE: usize = mem::size_of::<Row>();
const ROWS_PER_PAGE: usize = PAGE_SIZE / ROW_SIZE;

struct Table {
    n_rows: usize,
    pager: Pager,
}

struct Pager {
    file: File,
    pages: [Option<Vec<Option<Row>>>; PAGE_MAX_LEN],
}

#[derive(Clone)]
struct Row {
    id: i32,
    name: [u8; NAME_MAX_LEN],
    description: [u8; DESCRIPTION_MAX_LEN],
}

struct Cursor<'a> {
    table: &'a mut Table,
    row_num: usize,        // not index, index is row_num - 1
    pointing_to_end: bool, // when row_num == table.n_rows
}

impl Table {
    fn new(pager: Pager) -> Self {
        let file_len = pager.file.metadata().unwrap().len() as usize;
        let n_pages = file_len / PAGE_SIZE;
        let extra_len = file_len % PAGE_SIZE; // could be extra rows
        Table {
            n_rows: (n_pages * ROWS_PER_PAGE) + (extra_len / ROW_SIZE),
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
        let max = name.len().min(NAME_MAX_LEN);
        name_buf[0..max].copy_from_slice(&name.as_bytes()[0..max]);
        let max = description.len().min(DESCRIPTION_MAX_LEN);
        description_buf[0..max].copy_from_slice(&description.as_bytes()[0..max]);
        Cursor::from_end(self).write(Row {
            id,
            name: name_buf,
            description: description_buf,
        })?;
        self.n_rows += 1;
        Ok(())
    }

    fn select(&mut self) {
        let mut cursor = Cursor::from_start(self);
        while !cursor.pointing_to_end {
            if let Some(row) = cursor.read().unwrap() {
                row.print();
            }
            cursor.advance();
        }
    }
}

impl Drop for Table {
    fn drop(&mut self) {
        for page_index in 0..PAGE_MAX_LEN {
            let is_last_page = (page_index + 1) * ROWS_PER_PAGE >= self.n_rows; // TODO: better solution?
            if let Err(error) = self.pager.flush_page(page_index, is_last_page) {
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
        Ok(Pager {
            file,
            pages: [const { None }; PAGE_MAX_LEN],
        })
    }

    fn get_page(&mut self, page_index: usize) -> Result<&mut Vec<Option<Row>>, Box<dyn Error>> {
        if page_index >= PAGE_MAX_LEN {
            return Err("table reach max size".into());
        }
        let file_len = self.file.metadata()?.len() as usize;
        let n_pages = file_len / PAGE_SIZE;
        if self.pages[page_index].is_some() {
            return Ok(self.pages[page_index].as_mut().unwrap());
        }
        let mut new_page = vec![None; ROWS_PER_PAGE];
        let page_offset = page_index * PAGE_SIZE;
        if page_index < n_pages {
            for (i, row) in new_page.iter_mut().enumerate().take(ROWS_PER_PAGE) {
                let offset = (page_offset + i * ROW_SIZE) as u64;
                *row = Some(Row::read_at(&self.file, offset)?);
            }
        }
        if page_index == n_pages && file_len % PAGE_SIZE != 0 {
            // right before last page append some rows don't have whole page size
            let n_extra_rows = (file_len % PAGE_SIZE) / ROW_SIZE;
            for (i, row) in new_page.iter_mut().enumerate().take(n_extra_rows) {
                let offset = (page_offset + i * ROW_SIZE) as u64;
                *row = Some(Row::read_at(&self.file, offset)?);
            }
        }
        self.pages[page_index] = Some(new_page);
        Ok(self.pages[page_index].as_mut().unwrap())
    }

    fn flush_page(&mut self, page_index: usize, is_last_page: bool) -> Result<(), io::Error> {
        if self.pages[page_index].is_none() {
            return Ok(());
        }
        let page_offset = page_index * PAGE_SIZE;
        let page = self.pages[page_index].as_ref().unwrap();
        for (i, row_option) in page.iter().enumerate() {
            if let Some(row) = row_option {
                let offset = (page_offset + i * ROW_SIZE) as u64;
                row.write_at(&mut self.file, offset)?;
            }
        }
        if !is_last_page {
            let offset = (page_offset + ROWS_PER_PAGE * ROW_SIZE) as u64;
            let padding = vec![0u8; PAGE_SIZE - ROWS_PER_PAGE * ROW_SIZE];
            self.file.write_at(&padding, offset)?;
        }
        self.file.flush()?;
        Ok(())
    }
}

impl Row {
    fn read_at(file: &File, mut offset: u64) -> Result<Self, io::Error> {
        let mut id_buf = [0u8; 4];
        let mut name = [0u8; NAME_MAX_LEN];
        let mut description = [0u8; DESCRIPTION_MAX_LEN];
        file.read_at(&mut id_buf, offset)?;
        offset += 4;
        file.read_at(&mut name, offset)?;
        offset += NAME_MAX_LEN as u64;
        file.read_at(&mut description, offset)?;
        let id = i32::from_le_bytes(id_buf);
        Ok(Row {
            id,
            name,
            description,
        })
    }

    fn write_at(&self, file: &mut File, mut offset: u64) -> Result<(), io::Error> {
        file.write_at(&self.id.to_le_bytes(), offset)?;
        offset += 4;
        file.write_at(&self.name, offset)?;
        offset += NAME_MAX_LEN as u64;
        file.write_at(&self.description, offset)?;
        file.flush()?;
        Ok(())
    }

    fn print(&self) {
        println!(
            "{}. {} {}",
            self.id,
            str::from_utf8(&self.name).unwrap(),
            str::from_utf8(&self.description).unwrap()
        )
    }
}

impl<'a> Cursor<'a> {
    fn from_start(table: &'a mut Table) -> Self {
        let pointing_to_end = table.n_rows == 0; // since table is &mut, need to get n_rows before table assingment
        Cursor {
            table,
            row_num: 0,
            pointing_to_end,
        }
    }

    fn from_end(table: &'a mut Table) -> Self {
        let row_num = table.n_rows;
        Cursor {
            table,
            row_num,
            pointing_to_end: true,
        }
    }

    fn advance(&mut self) {
        self.row_num += 1;
        if self.row_num == self.table.n_rows {
            self.pointing_to_end = true;
        }
    }

    // actually don't need &mut here, but for the sake of compiler complain,
    // or just wrote two get_page method, one for &mut and the other for &
    fn read(&mut self) -> Result<Option<&Row>, Box<dyn Error>> {
        let page_index = self.row_num / ROWS_PER_PAGE;
        let row_index = self.row_num % ROWS_PER_PAGE;
        let page = self.table.pager.get_page(page_index)?;
        match page[row_index] {
            None => Ok(None),
            Some(_) => Ok(page[row_index].as_ref()),
        }
    }

    fn write(&mut self, row: Row) -> Result<(), Box<dyn Error>> {
        let page_index = self.row_num / ROWS_PER_PAGE;
        let row_index = self.row_num % ROWS_PER_PAGE;
        let page = self.table.pager.get_page(page_index)?;
        page[row_index] = Some(row);
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
