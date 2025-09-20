use std::env;
use std::error::Error;
use std::fs::{File, OpenOptions};
use std::io;
use std::io::prelude::*;
use std::mem;
use std::os::unix::fs::FileExt;
use std::process;

const NO_PARENT: i32 = -1;

const PAGE_SIZE: usize = 4096;
const ID_SIZE: usize = mem::size_of::<i64>();
const NAME_MAX_SIZE: usize = 32;
const DESCRIPTION_MAX_SIZE: usize = 256;
const PAGE_MAX_NUMS: usize = 64;

const NODE_KIND_SIZE: usize = size_of::<NodeKind>();
const NODE_IS_ROOT_SIZE: usize = size_of::<bool>();
const NODE_PARENT_SIZE: usize = size_of::<i32>();
const INTERNAL_NODE_HEADER_SIZE: usize = NODE_KIND_SIZE + NODE_IS_ROOT_SIZE + NODE_PARENT_SIZE;
const LEAF_NODE_N_CELLS_SIZE: usize = size_of::<u16>();
const LEAF_NODE_HEADER_SIZE: usize = INTERNAL_NODE_HEADER_SIZE + LEAF_NODE_N_CELLS_SIZE;
const LEAF_NODE_SPACE_FOR_CELLS: usize = PAGE_SIZE - LEAF_NODE_HEADER_SIZE;
const LEAF_NODE_CELL_SIZE: usize = size_of::<Cell>();
const LEAF_NODE_CELL_KEY_SIZE: usize = size_of::<i64>();
const LEAF_NODE_CELL_MAX_NUM: usize = LEAF_NODE_SPACE_FOR_CELLS / LEAF_NODE_CELL_SIZE;

const ERR_INSERT_SYNTAX: &str = "ERROR: insert <id> <name> <description>.";
const ERR_NOT_POSITIVE_ID: &str = "ERROR: id must be greater than 0.";
const ERR_NAME_TOO_LONG: &str = "ERROR: name too long.";
const ERR_DESCRIPTION_TOO_LONG: &str = "ERROR: description too long.";
const ERR_TABLE_FULL: &str = "ERROR: table reach max size.";

// make sure always one byte in size
#[repr(u8)]
enum NodeKind {
    Internal = 1,
    Leaf = 2,
}

struct Table {
    root_node_index: usize,
    pager: Pager,
}

struct Pager {
    file: File,
    n_pages: usize,
    pages: [Option<Node>; PAGE_MAX_NUMS],
}

struct Row {
    id: i64,
    name: [u8; NAME_MAX_SIZE],
    description: [u8; DESCRIPTION_MAX_SIZE],
}

struct Cursor<'a> {
    table: &'a mut Table,
    page_index: usize,
    cell_index: usize,
    end_of_table: bool,
}

struct Cell {
    key: i64,
    value: Row,
}

struct Node {
    kind: NodeKind,
    is_root: bool,
    parent: i32,
    // following fields only exist in leaf node
    n_cells: Option<u16>,
    cells: Option<[Option<Cell>; LEAF_NODE_CELL_MAX_NUM]>,
}

impl Table {
    fn new(mut pager: Pager) -> Self {
        let root_node_index = 0usize;
        if pager.n_pages == 0 {
            let root_node = pager.get_page(root_node_index).unwrap();
            root_node.kind = NodeKind::Leaf;
            root_node.is_root = true;
            root_node.parent = NO_PARENT;
            root_node.n_cells = Some(0);
            root_node.cells = Some([const { None }; LEAF_NODE_CELL_MAX_NUM]);
        }
        Table {
            root_node_index,
            pager,
        }
    }

    fn insert(&mut self, args: &[&str]) -> Result<(), Box<dyn Error>> {
        // TODO: parse ""
        if args.len() != 3 {
            return Err(ERR_INSERT_SYNTAX.into());
        }
        let n_cells = self.pager.get_page(self.root_node_index)?.get_n_cells() as usize;
        if n_cells >= LEAF_NODE_CELL_MAX_NUM {
            return Err(ERR_TABLE_FULL.into());
        }
        let id = args[0].parse::<i64>().map_err(|_| ERR_INSERT_SYNTAX)?;
        if id <= 0 {
            return Err(ERR_NOT_POSITIVE_ID.into());
        }
        let name = args[1];
        if name.len() > NAME_MAX_SIZE {
            return Err(ERR_NAME_TOO_LONG.into());
        }
        let description = args[2];
        if description.len() > DESCRIPTION_MAX_SIZE {
            return Err(ERR_DESCRIPTION_TOO_LONG.into());
        }
        let mut name_buf = [0u8; NAME_MAX_SIZE];
        let mut description_buf = [0u8; DESCRIPTION_MAX_SIZE];
        let max = name.len().min(NAME_MAX_SIZE);
        name_buf[0..max].copy_from_slice(&name.as_bytes()[0..max]);
        let max = description.len().min(DESCRIPTION_MAX_SIZE);
        description_buf[0..max].copy_from_slice(&description.as_bytes()[0..max]);
        let mut cursor = Cursor::from(self, id);
        if cursor.cell_index < n_cells && id == cursor.read()?.unwrap().key {
            return Err(format!("ERROR: key '{id}' already exist.").into());
        }
        cursor.write(Cell {
            key: id,
            value: Row {
                id,
                name: name_buf,
                description: description_buf,
            },
        })?;
        Ok(())
    }

    fn select(&mut self) {
        let mut cursor = Cursor::from_start(self);
        while !cursor.end_of_table {
            if let Some(cell) = cursor.read().unwrap() {
                println!(
                    "[{}, {}, {}]",
                    cell.value.id,
                    str::from_utf8(&cell.value.name).unwrap(),
                    str::from_utf8(&cell.value.description).unwrap()
                )
            }
            cursor.advance();
        }
    }
}

impl Drop for Table {
    fn drop(&mut self) {
        for page_index in 0..self.pager.n_pages {
            if let Err(error) = self.pager.flush_page(page_index) {
                eprintln!("ERROR: db close {error}.");
                process::exit(1);
            }
        }
    }
}

impl Pager {
    fn new(path: &str) -> Result<Self, Box<dyn Error>> {
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(path)?;
        let file_size = file.metadata()?.len() as usize;
        if !file_size.is_multiple_of(PAGE_SIZE) {
            return Err("ERROR: invalid database file, should be page-aligned.".into());
        }
        Ok(Pager {
            file,
            n_pages: file_size / PAGE_SIZE,
            pages: [const { None }; PAGE_MAX_NUMS],
        })
    }

    fn get_page(&mut self, page_index: usize) -> Result<&mut Node, Box<dyn Error>> {
        if page_index >= PAGE_MAX_NUMS {
            return Err(ERR_TABLE_FULL.into());
        }
        if self.pages[page_index].is_some() {
            return Ok(self.pages[page_index].as_mut().unwrap());
        }
        if page_index < self.n_pages {
            self.pages[page_index] = Some(Node::read_at(&self.file, page_index * PAGE_SIZE)?);
        } else {
            self.n_pages = page_index + 1;
            self.pages[page_index] = Some(Node {
                kind: NodeKind::Internal,
                is_root: false,
                parent: NO_PARENT,
                n_cells: None,
                cells: None,
            });
        }
        Ok(self.pages[page_index].as_mut().unwrap())
    }

    fn flush_page(&mut self, page_index: usize) -> Result<(), Box<dyn Error>> {
        match self.pages[page_index].as_mut() {
            None => Ok(()),
            Some(page) => Ok(page.write_at(&self.file, page_index * PAGE_SIZE)?),
        }
    }
}

impl<'a> Cursor<'a> {
    fn from_start(table: &'a mut Table) -> Self {
        let page_index = table.root_node_index; // since table is &mut, need to get n_rows before table assingment
        let root_node = table.pager.get_page(page_index).unwrap();
        let end_of_table = root_node.get_n_cells() == 0;
        Cursor {
            table,
            page_index,
            cell_index: 0,
            end_of_table,
        }
    }

    fn from(table: &'a mut Table, key: i64) -> Self {
        let page_index = table.root_node_index;
        let root_node = table.pager.get_page(page_index).unwrap();
        match root_node.kind {
            NodeKind::Leaf => {
                let n_cells = root_node.get_n_cells() as usize;
                let mut left = 0usize;
                let mut right = n_cells;
                while left != right {
                    let mid = (left + right) / 2;
                    let cell_key = root_node.read_cell(mid).unwrap().key;
                    if key == cell_key {
                        return Cursor {
                            table,
                            page_index,
                            cell_index: mid,
                            end_of_table: false,
                        };
                    } else if key < cell_key {
                        right = mid;
                    } else {
                        left = mid + 1;
                    }
                }
                Cursor {
                    table,
                    page_index,
                    cell_index: left,
                    end_of_table: left == n_cells,
                }
            }
            NodeKind::Internal => {
                panic!("TODO: search in internal node");
            }
        }
    }

    fn advance(&mut self) {
        self.cell_index += 1;
        let current_node = self.table.pager.get_page(self.page_index).unwrap();
        if self.cell_index >= current_node.get_n_cells().into() {
            self.end_of_table = true;
        }
    }

    // actually don't need &mut here, but for the sake of compiler's complain
    fn read(&mut self) -> Result<Option<&Cell>, Box<dyn Error>> {
        Ok(self
            .table
            .pager
            .get_page(self.page_index)?
            .read_cell(self.cell_index))
    }

    fn write(&mut self, cell: Cell) -> Result<(), Box<dyn Error>> {
        self.table
            .pager
            .get_page(self.page_index)?
            .insert_cell(self.cell_index, cell);
        Ok(())
    }
}

impl NodeKind {
    fn from_u8(v: u8) -> Result<Self, Box<dyn Error>> {
        match v {
            1 => Ok(Self::Internal),
            2 => Ok(Self::Leaf),
            _ => Err("ERROR: unkown value {v}, can't transform valid node kind.".into()),
        }
    }
    fn to_u8(&self) -> u8 {
        match self {
            Self::Internal => 1,
            Self::Leaf => 2,
        }
    }
}

impl Node {
    fn get_n_cells(&self) -> u16 {
        self.n_cells
            .expect("ERROR: get_n_cells must be called by leaf node.")
    }
    fn get_mut_cells(&mut self) -> &mut [Option<Cell>] {
        self.cells
            .as_mut()
            .map(|arr| arr.as_mut_slice())
            .expect("ERROR: get_mut_cells must be called by leaf node.")
    }
    fn read_at(file: &File, mut offset: usize) -> Result<Self, Box<dyn Error>> {
        let mut kind_buf = [0u8; NODE_KIND_SIZE];
        let mut is_root_buf = [0u8; NODE_IS_ROOT_SIZE];
        let mut parent_buf = [0u8; NODE_PARENT_SIZE];
        read_and_advance(file, &mut kind_buf, &mut offset, NODE_KIND_SIZE)?;
        read_and_advance(file, &mut is_root_buf, &mut offset, NODE_IS_ROOT_SIZE)?;
        read_and_advance(file, &mut parent_buf, &mut offset, NODE_PARENT_SIZE)?;
        let mut new_node = Node {
            kind: NodeKind::from_u8(u8::from_le_bytes(kind_buf))?,
            is_root: u8::from_le_bytes(is_root_buf) != 0,
            parent: i32::from_le_bytes(parent_buf),
            n_cells: None,
            cells: None,
        };
        if let NodeKind::Internal = new_node.kind {
            return Ok(new_node);
        }
        let mut n_cells_buf = [0u8; LEAF_NODE_N_CELLS_SIZE];
        read_and_advance(file, &mut n_cells_buf, &mut offset, LEAF_NODE_N_CELLS_SIZE)?;
        new_node.n_cells = Some(u16::from_le_bytes(n_cells_buf));
        new_node.cells = Some([const { None }; LEAF_NODE_CELL_MAX_NUM]);
        let n_cells = new_node.get_n_cells();
        for cell in new_node.get_mut_cells().iter_mut().take(n_cells.into()) {
            let mut cell_key_buf = [0u8; LEAF_NODE_CELL_KEY_SIZE];
            read_and_advance(
                file,
                &mut cell_key_buf,
                &mut offset,
                LEAF_NODE_CELL_KEY_SIZE,
            )?;
            let mut id_buf = [0u8; ID_SIZE];
            let mut name_buf = [0u8; NAME_MAX_SIZE];
            let mut description_buf = [0u8; DESCRIPTION_MAX_SIZE];
            read_and_advance(file, &mut id_buf, &mut offset, ID_SIZE)?;
            read_and_advance(file, &mut name_buf, &mut offset, NAME_MAX_SIZE)?;
            read_and_advance(
                file,
                &mut description_buf,
                &mut offset,
                DESCRIPTION_MAX_SIZE,
            )?;
            *cell = Some(Cell {
                key: i64::from_le_bytes(cell_key_buf),
                value: Row {
                    id: i64::from_le_bytes(id_buf),
                    name: name_buf,
                    description: description_buf,
                },
            });
        }
        Ok(new_node)
    }
    fn write_at(&mut self, mut file: &File, mut offset: usize) -> Result<(), Box<dyn Error>> {
        let start = offset;
        write_and_advance(
            file,
            &self.kind.to_u8().to_le_bytes(),
            &mut offset,
            NODE_KIND_SIZE,
        )?;
        write_and_advance(
            file,
            &(self.is_root as u8).to_le_bytes(),
            &mut offset,
            NODE_IS_ROOT_SIZE,
        )?;
        write_and_advance(
            file,
            &self.parent.to_le_bytes(),
            &mut offset,
            NODE_PARENT_SIZE,
        )?;
        if let NodeKind::Internal = self.kind {
            let padding_len = PAGE_SIZE - (offset - start);
            let padding = vec![0u8; padding_len];
            write_and_advance(file, &padding, &mut offset, padding_len)?;
            file.flush()?;
            return Ok(());
        }
        let n_cells = self.get_n_cells();
        write_and_advance(
            file,
            &n_cells.to_le_bytes(),
            &mut offset,
            LEAF_NODE_N_CELLS_SIZE,
        )?;
        for cell in self.get_mut_cells().iter().take(n_cells.into()).flatten() {
            write_and_advance(
                file,
                &cell.key.to_le_bytes(),
                &mut offset,
                LEAF_NODE_CELL_KEY_SIZE,
            )?;
            write_and_advance(file, &cell.value.id.to_le_bytes(), &mut offset, ID_SIZE)?;
            write_and_advance(file, &cell.value.name, &mut offset, NAME_MAX_SIZE)?;
            write_and_advance(
                file,
                &cell.value.description,
                &mut offset,
                DESCRIPTION_MAX_SIZE,
            )?;
        }
        let padding_len = PAGE_SIZE - (offset - start);
        let padding = vec![0u8; padding_len];
        write_and_advance(file, &padding, &mut offset, padding_len)?;
        file.flush()?;
        Ok(())
    }
    fn read_cell(&self, cell_index: usize) -> Option<&Cell> {
        self.cells.as_ref()?.get(cell_index)?.as_ref()
    }
    fn insert_cell(&mut self, cell_index: usize, cell: Cell) {
        if cell_index >= LEAF_NODE_CELL_MAX_NUM {
            panic!("TODO: need to split page");
        }
        let n_cells = self.get_n_cells();
        let mut i = n_cells as usize;
        let cells = self.get_mut_cells();
        while cell_index < i {
            cells[i] = cells[i - 1].take();
            i -= 1;
        }
        cells[cell_index] = Some(cell);
        self.n_cells = Some(n_cells + 1);
    }
}

fn write_and_advance(
    file: &File,
    buf: &[u8],
    offset: &mut usize,
    advance_distance: usize,
) -> Result<(), Box<dyn Error>> {
    file.write_at(buf, *offset as u64)?;
    *offset += advance_distance;
    Ok(())
}

fn read_and_advance(
    file: &File,
    buf: &mut [u8],
    offset: &mut usize,
    advance_distance: usize,
) -> Result<(), Box<dyn Error>> {
    file.read_at(buf, *offset as u64)?;
    *offset += advance_distance;
    Ok(())
}

fn main() {
    let args: Vec<_> = env::args().collect();
    if args.len() != 2 {
        eprintln!("USAGE: rqlite <database>");
        process::exit(1);
    }
    let pager = Pager::new(&args[1]).unwrap_or_else(|error| {
        eprintln!("ERROR: init pager: {error}.");
        process::exit(1);
    });
    let mut table = Table::new(pager);
    let mut buf = String::new();
    loop {
        print!("rqlite> ");
        io::stdout().flush().expect("ERROR: flush.");
        let n = io::stdin().read_line(&mut buf).unwrap_or_else(|error| {
            eprintln!("ERROR: read_line fail: {error}.");
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
                ".constants" => {
                    println!("row size: {}", size_of::<Row>());
                    println!("internal node header size: {INTERNAL_NODE_HEADER_SIZE}");
                    println!("leaf node header size: {LEAF_NODE_HEADER_SIZE}");
                    println!("leaf node cell size: {LEAF_NODE_CELL_SIZE}");
                    println!("leaf node space for cells: {LEAF_NODE_SPACE_FOR_CELLS}");
                    println!("leaf node max cells: {LEAF_NODE_CELL_MAX_NUM}");
                }
                _ => println!("ERROR: unknown command: '{input}'"),
            }
        } else {
            // exec statement
            let tokens = input.split([' ', '\t']).collect::<Vec<_>>();
            match tokens[0] {
                "insert" => match table.insert(&tokens[1..]) {
                    Ok(()) => println!("executed."),
                    Err(e) => println!("{e}"),
                },
                "select" => {
                    table.select();
                    println!("executed.");
                }
                _ => println!("ERROR: unkown statement keyword: '{input}'"),
            }
        }
        buf.clear();
    }
}
