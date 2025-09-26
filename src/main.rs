use std::env;
use std::error::Error;
use std::fs::{File, OpenOptions};
use std::io;
use std::io::prelude::*;
use std::mem;
use std::os::unix::fs::FileExt;
use std::process;

const NOT_EXIST: i32 = -1;

const PAGE_SIZE: usize = 4096;
const ID_SIZE: usize = mem::size_of::<i64>();
const NAME_MAX_SIZE: usize = 32;
const DESCRIPTION_MAX_SIZE: usize = 256;
const PAGE_MAX_NUM: usize = 64;

const NODE_KIND_SIZE: usize = size_of::<NodeKind>();
const NODE_IS_ROOT_SIZE: usize = size_of::<bool>();
const NODE_PARENT_SIZE: usize = size_of::<i32>();
const NODE_N_CELLS_SIZE: usize = size_of::<u32>();
const NODE_HEADER_SIZE: usize =
    NODE_KIND_SIZE + NODE_IS_ROOT_SIZE + NODE_PARENT_SIZE + NODE_N_CELLS_SIZE;

const LEAF_NODE_HEADER_SIZE: usize = NODE_HEADER_SIZE;
const LEAF_NODE_SPACE_FOR_CELLS: usize = PAGE_SIZE - LEAF_NODE_HEADER_SIZE;
const LEAF_NODE_CELL_KEY_SIZE: usize = size_of::<i64>();
const LEAF_NODE_CELL_SIZE: usize =
    LEAF_NODE_CELL_KEY_SIZE + ID_SIZE + NAME_MAX_SIZE + DESCRIPTION_MAX_SIZE;
const LEAF_NODE_CELL_MAX_NUM: usize = LEAF_NODE_SPACE_FOR_CELLS / LEAF_NODE_CELL_SIZE;

const INTERNAL_NODE_RIGHT_CHILD_SIZE: usize = size_of::<i32>();
const INTERNAL_NODE_HEADER_SIZE: usize = NODE_HEADER_SIZE + INTERNAL_NODE_RIGHT_CHILD_SIZE;
const INTERNAL_NODE_SPACE_FOR_CELLS: usize = PAGE_SIZE - INTERNAL_NODE_HEADER_SIZE;
const INTERNAL_NODE_CELL_KEY_SIZE: usize = size_of::<i64>();
const INTERNAL_NODE_CELL_CHILD_SIZE: usize = size_of::<i32>();
const INTERNAL_NODE_CELL_SIZE: usize = INTERNAL_NODE_CELL_KEY_SIZE + INTERNAL_NODE_CELL_CHILD_SIZE;
const INTERNAL_NODE_CELL_MAX_NUM: usize = INTERNAL_NODE_SPACE_FOR_CELLS / INTERNAL_NODE_CELL_SIZE;

const SPLIT_RIGHT_LEAF_NODE_NUM: usize = (LEAF_NODE_CELL_MAX_NUM + 1) / 2;
const SPLIT_LEFT_LEAF_NODE_NUM: usize = (LEAF_NODE_CELL_MAX_NUM + 1) - SPLIT_RIGHT_LEAF_NODE_NUM;

const ERR_INSERT_SYNTAX: &str = "ERROR: insert <id> <name> <description>.";
const ERR_NOT_POSITIVE_ID: &str = "ERROR: id must be greater than 0.";
const ERR_NAME_TOO_LONG: &str = "ERROR: name too long.";
const ERR_DESCRIPTION_TOO_LONG: &str = "ERROR: description too long.";
const ERR_TABLE_FULL: &str = "ERROR: table reach max size.";
const ERR_INVALID_FILE: &str = "ERROR: invalid database file, should be page-aligned.";

// make sure always one byte in size
#[repr(u8)]
#[derive(Clone)]
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
    pages: [Option<Node>; PAGE_MAX_NUM],
}

#[derive(Clone)]
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

#[derive(Clone)]
struct LeafCell {
    key: i64,
    value: Row,
}

struct InternalCell {
    child: i32,
    key: i64,
}

struct Node {
    kind: NodeKind,
    is_root: bool,
    parent: i32,
    n_cells: u32,
    // following fields only exist in leaf node
    leaf_cells: Option<[Option<LeafCell>; LEAF_NODE_CELL_MAX_NUM]>,
    // following fields only exist in internal node
    right_child: Option<i32>,
    internal_cells: Option<[Option<InternalCell>; INTERNAL_NODE_CELL_MAX_NUM]>,
}

impl Table {
    fn new(mut pager: Pager) -> Self {
        let root_node_index = 0usize;
        if pager.n_pages == 0 {
            let root_node = pager.get_page(root_node_index).unwrap();
            root_node.become_leaf_node();
            root_node.is_root = true;
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
        name_buf[..max].copy_from_slice(&name.as_bytes()[..max]);
        let max = description.len().min(DESCRIPTION_MAX_SIZE);
        description_buf[..max].copy_from_slice(&description.as_bytes()[..max]);
        let n_cells = self.pager.get_page(self.root_node_index)?.get_n_cells();
        let mut cursor = Cursor::from(self, id);
        if cursor.cell_index < n_cells && id == cursor.read_leaf_cell()?.unwrap().key {
            return Err(format!("ERROR: key '{id}' already exist.").into());
        }
        cursor.write_leaf_cell(LeafCell {
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
            if let Some(cell) = cursor.read_leaf_cell().unwrap() {
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
            if let Err(error) = self.pager.flush_page_to_file(page_index) {
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
            return Err(ERR_INVALID_FILE.into());
        }
        Ok(Pager {
            file,
            n_pages: file_size / PAGE_SIZE,
            pages: [const { None }; PAGE_MAX_NUM],
        })
    }

    fn print_tree(&mut self, page_index: usize, indentation: usize) {
        self.fetch_page_from_file(page_index).unwrap();
        let exist = self.pages[page_index].is_some();
        if !exist {
            println!("tree node {page_index} not exist");
            return;
        }
        let (node_kind, n_cells) = {
            let node = self.pages[page_index].as_ref().unwrap();
            (node.kind.clone(), node.get_n_cells())
        };
        match node_kind {
            NodeKind::Leaf => {
                print_with_indentation(
                    indentation,
                    format!("- leaf (size {n_cells})").as_ref(),
                );
                for i in 0..n_cells {
                    let key = {
                        let node = self.pages[page_index].as_ref().unwrap();
                        node.read_leaf_cell(i).unwrap().key
                    };
                    print_with_indentation(
                        indentation + 1,
                        format!("- {}", key).as_ref(),
                    );
                }
            }
            NodeKind::Internal => {
                print_with_indentation(
                    indentation,
                    format!("- internal (size {n_cells})").as_ref(),
                );
                for i in 0..n_cells {
                    let (child_page, key) = {
                        let node = self.pages[page_index].as_ref().unwrap();
                        let internal_cell = node.read_internal_cell(i).unwrap();
                        (internal_cell.child as usize, internal_cell.key)
                    };
                    self.print_tree(child_page, indentation + 1);
                    print_with_indentation(
                        indentation + 1,
                        format!("- key {}", key).as_ref(),
                    );
                }
                let right_child = {
                    let node = self.pages[page_index].as_ref().unwrap();
                    node.right_child.unwrap() as usize
                };
                self.print_tree(right_child, indentation + 1);
            }
        }
    }

    fn get_new_page_index(&self) -> usize {
        self.n_pages
    }

    // must be called when two page exist
    fn get_two_pages(
        &mut self,
        first_page_index: usize,
        second_page_index: usize,
    ) -> (&mut Node, &mut Node) {
        let ptr = self.pages.as_mut_ptr();
        unsafe {
            let first_page = (*ptr.add(first_page_index)).as_mut().unwrap();
            let second_page = (*ptr.add(second_page_index)).as_mut().unwrap();
            (first_page, second_page)
        }
    }

    fn get_page(&mut self, page_index: usize) -> Result<&mut Node, Box<dyn Error>> {
        if page_index >= PAGE_MAX_NUM {
            return Err(ERR_TABLE_FULL.into());
        }
        if self.pages[page_index].is_some() {
            return Ok(self.pages[page_index].as_mut().unwrap());
        }
        if page_index < self.n_pages {
            self.fetch_page_from_file(page_index)?;
        } else {
            self.n_pages = page_index + 1;
            self.pages[page_index] = Some(Node {
                kind: NodeKind::Leaf,
                is_root: false,
                parent: NOT_EXIST,
                n_cells: 0,
                leaf_cells: None,
                right_child: None,
                internal_cells: None,
            });
        }
        Ok(self.pages[page_index].as_mut().unwrap())
    }

    fn fetch_page_from_file(&mut self, page_index: usize) -> Result<(), Box<dyn Error>> {
        if let None = self.pages[page_index] {
            self.pages[page_index] = Some(Node::read_at(&self.file, page_index * PAGE_SIZE)?);
        }
        Ok(())
    }

    fn flush_page_to_file(&mut self, page_index: usize) -> Result<(), Box<dyn Error>> {
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
        let root_index = table.root_node_index;
        let root_node = table.pager.get_page(root_index).unwrap();
        match root_node.kind {
            NodeKind::Leaf => Self::from_leaf_node(table, root_index, key),
            NodeKind::Internal => Self::from_internal_node(table, root_index, key),
        }
    }

    fn from_leaf_node(table: &'a mut Table, page_index: usize, key: i64) -> Self {
        let node = table.pager.get_page(page_index).unwrap();
        let n_cells = node.get_n_cells();
        let mut left = 0usize;
        let mut right = n_cells;
        while left != right {
            let mid = (left + right) / 2;
            let cell_key = node.read_leaf_cell(mid).unwrap().key;
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

    fn from_internal_node(table: &'a mut Table, page_index: usize, key: i64) -> Self {
        let node = table.pager.get_page(page_index).unwrap();
        let n_cells = node.get_n_cells();
        let mut left = 0usize;
        let mut right = n_cells; // 2
        while left != right {
            let mid = (left + right) / 2;
            let cell_key = node.read_internal_cell(mid).unwrap().key;
            if key <= cell_key {
                right = mid;
            } else {
                left = mid + 1;
            }
        }
        let child_index = node.get_child_index(left);
        let child_node = table.pager.get_page(child_index).unwrap();
        match child_node.kind {
            NodeKind::Leaf => Self::from_leaf_node(table, child_index, key),
            NodeKind::Internal => Self::from_internal_node(table, child_index, key),
        }
    }

    fn advance(&mut self) {
        self.cell_index += 1;
        let current_node = self.table.pager.get_page(self.page_index).unwrap();
        if self.cell_index >= current_node.get_n_cells() {
            self.end_of_table = true;
        }
    }

    // actually don't need &mut here, but for the sake of compiler's complain
    fn read_leaf_cell(&mut self) -> Result<Option<&LeafCell>, Box<dyn Error>> {
        Ok(self
            .table
            .pager
            .get_page(self.page_index)?
            .read_leaf_cell(self.cell_index))
    }

    fn write_leaf_cell(&mut self, cell: LeafCell) -> Result<(), Box<dyn Error>> {
        let node = self.table.pager.get_page(self.page_index)?;
        if node.get_n_cells() < LEAF_NODE_CELL_MAX_NUM {
            node.insert_leaf_cell(self.cell_index, cell);
            return Ok(());
        }
        let new_page_index = self.table.pager.get_new_page_index();
        let new_node = self.table.pager.get_page(new_page_index)?;
        new_node.become_leaf_node();
        let (old_node, new_node) = self
            .table
            .pager
            .get_two_pages(self.page_index, new_page_index);
        for i in (0..LEAF_NODE_CELL_MAX_NUM + 1).rev() {
            let cell_index = i % SPLIT_LEFT_LEAF_NODE_NUM;
            if i == self.cell_index {
                if i >= SPLIT_LEFT_LEAF_NODE_NUM {
                    new_node.put_leaf_cell(cell_index, cell.clone());
                } else {
                    old_node.put_leaf_cell(cell_index, cell.clone());
                }
            } else {
                let leaf_cells = old_node.get_mut_leaf_cells();
                let index = if i > self.cell_index { i - 1 } else { i };
                let cell = leaf_cells[index].take().unwrap();
                if i >= SPLIT_LEFT_LEAF_NODE_NUM {
                    new_node.put_leaf_cell(cell_index, cell);
                } else {
                    old_node.put_leaf_cell(cell_index, cell);
                }
            }
        }
        old_node.n_cells = SPLIT_LEFT_LEAF_NODE_NUM as u32;
        new_node.n_cells = SPLIT_RIGHT_LEAF_NODE_NUM as u32;
        if old_node.is_root {
            new_node.parent = self.page_index as i32;
            let left_child_page_index = self.table.pager.get_new_page_index();
            let left_child = self.table.pager.get_page(left_child_page_index)?;
            left_child.become_leaf_node();
            let (root_node, left_child) = self
                .table
                .pager
                .get_two_pages(self.page_index, left_child_page_index);
            let n_cells = root_node.get_n_cells();
            left_child.n_cells = n_cells as u32;
            left_child.parent = self.page_index as i32;
            let root_leaf_cells = root_node.get_mut_leaf_cells();
            let left_child_leaf_cells = left_child.get_mut_leaf_cells();
            for i in 0..n_cells {
                left_child_leaf_cells[i] = Some(root_leaf_cells[i].take().unwrap());
            }
            root_node.become_internal_node();
            root_node.n_cells = 1;
            root_node.right_child = Some(new_page_index as i32);
            let internal_cells = root_node.get_mut_internal_cells();
            internal_cells[0] = Some(InternalCell {
                key: left_child.get_max_key(),
                child: left_child_page_index as i32,
            });
        } else {
            panic!("update parent after split");
        }
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
    fn become_leaf_node(&mut self) {
        self.kind = NodeKind::Leaf;
        self.n_cells = 0;
        self.leaf_cells = Some([const { None }; LEAF_NODE_CELL_MAX_NUM]);
        self.right_child = None;
        self.internal_cells = None;
    }
    fn become_internal_node(&mut self) {
        self.kind = NodeKind::Internal;
        self.n_cells = 0;
        self.leaf_cells = None;
        self.right_child = Some(NOT_EXIST);
        self.internal_cells = Some([const { None }; INTERNAL_NODE_CELL_MAX_NUM]);
    }
    fn get_n_cells(&self) -> usize {
        self.n_cells as usize
    }
    fn get_mut_leaf_cells(&mut self) -> &mut [Option<LeafCell>] {
        self.leaf_cells
            .as_mut()
            .map(|arr| arr.as_mut_slice())
            .expect("ERROR: get_mut_leaf_cells must be called by leaf node.")
    }
    fn get_mut_internal_cells(&mut self) -> &mut [Option<InternalCell>] {
        self.internal_cells
            .as_mut()
            .map(|arr| arr.as_mut_slice())
            .expect("ERROR: get_mut_internal_cells must be called by internal node.")
    }
    fn read_at(file: &File, mut offset: usize) -> Result<Self, Box<dyn Error>> {
        let mut kind_buf = [0u8; NODE_KIND_SIZE];
        let mut is_root_buf = [0u8; NODE_IS_ROOT_SIZE];
        let mut parent_buf = [0u8; NODE_PARENT_SIZE];
        let mut n_cells_buf = [0u8; NODE_N_CELLS_SIZE];
        read_and_advance(file, &mut kind_buf, &mut offset, NODE_KIND_SIZE)?;
        read_and_advance(file, &mut is_root_buf, &mut offset, NODE_IS_ROOT_SIZE)?;
        read_and_advance(file, &mut parent_buf, &mut offset, NODE_PARENT_SIZE)?;
        read_and_advance(file, &mut n_cells_buf, &mut offset, NODE_N_CELLS_SIZE)?;
        let mut new_node = Node {
            kind: NodeKind::from_u8(u8::from_le_bytes(kind_buf))?,
            is_root: u8::from_le_bytes(is_root_buf) != 0,
            parent: i32::from_le_bytes(parent_buf),
            n_cells: u32::from_le_bytes(n_cells_buf),
            leaf_cells: None,
            right_child: None,
            internal_cells: None,
        };
        if let NodeKind::Internal = new_node.kind {
            let mut right_child_buf = [0u8; INTERNAL_NODE_RIGHT_CHILD_SIZE];
            read_and_advance(
                file,
                &mut right_child_buf,
                &mut offset,
                INTERNAL_NODE_RIGHT_CHILD_SIZE,
            )?;
            new_node.internal_cells = Some([const { None }; INTERNAL_NODE_CELL_MAX_NUM]);
            new_node.right_child = Some(i32::from_le_bytes(right_child_buf));
            let n_cells = new_node.get_n_cells();
            for cell in new_node.get_mut_internal_cells().iter_mut().take(n_cells) {
                let mut internal_cell_key_buf = [0u8; INTERNAL_NODE_CELL_KEY_SIZE];
                let mut internal_cell_child_buf = [0u8; INTERNAL_NODE_CELL_CHILD_SIZE];
                read_and_advance(
                    file,
                    &mut internal_cell_child_buf,
                    &mut offset,
                    INTERNAL_NODE_CELL_CHILD_SIZE,
                )?;
                read_and_advance(
                    file,
                    &mut internal_cell_key_buf,
                    &mut offset,
                    INTERNAL_NODE_CELL_KEY_SIZE,
                )?;
                *cell = Some(InternalCell {
                    key: i64::from_le_bytes(internal_cell_key_buf),
                    child: i32::from_le_bytes(internal_cell_child_buf),
                });
            }
            return Ok(new_node);
        }
        new_node.leaf_cells = Some([const { None }; LEAF_NODE_CELL_MAX_NUM]);
        let n_cells = new_node.get_n_cells();
        for cell in new_node.get_mut_leaf_cells().iter_mut().take(n_cells) {
            let mut leaf_cell_key_buf = [0u8; LEAF_NODE_CELL_KEY_SIZE];
            read_and_advance(
                file,
                &mut leaf_cell_key_buf,
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
            *cell = Some(LeafCell {
                key: i64::from_le_bytes(leaf_cell_key_buf),
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
        let n_cells = self.get_n_cells() as u32;
        write_and_advance(file, &n_cells.to_le_bytes(), &mut offset, NODE_N_CELLS_SIZE)?;
        if let NodeKind::Internal = self.kind {
            let right_child = self.right_child.unwrap();
            write_and_advance(
                file,
                &right_child.to_le_bytes(),
                &mut offset,
                INTERNAL_NODE_RIGHT_CHILD_SIZE,
            )?;
            for cell in self
                .get_mut_internal_cells()
                .iter()
                .take(n_cells as usize)
                .flatten()
            {
                write_and_advance(
                    file,
                    &cell.child.to_le_bytes(),
                    &mut offset,
                    INTERNAL_NODE_CELL_CHILD_SIZE,
                )?;
                write_and_advance(
                    file,
                    &cell.key.to_le_bytes(),
                    &mut offset,
                    INTERNAL_NODE_CELL_KEY_SIZE,
                )?;
            }
            let padding_len = PAGE_SIZE - (offset - start);
            let padding = vec![0u8; padding_len];
            write_and_advance(file, &padding, &mut offset, padding_len)?;
            file.flush()?;
            return Ok(());
        }
        for cell in self
            .get_mut_leaf_cells()
            .iter()
            .take(n_cells as usize)
            .flatten()
        {
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
    fn read_leaf_cell(&self, cell_index: usize) -> Option<&LeafCell> {
        self.leaf_cells.as_ref()?.get(cell_index)?.as_ref()
    }
    fn read_internal_cell(&self, cell_index: usize) -> Option<&InternalCell> {
        self.internal_cells.as_ref()?.get(cell_index)?.as_ref()
    }
    fn put_leaf_cell(&mut self, cell_index: usize, cell: LeafCell) {
        let leaf_cells = self.leaf_cells.as_mut().unwrap();
        leaf_cells[cell_index] = Some(cell);
    }
    fn insert_leaf_cell(&mut self, cell_index: usize, cell: LeafCell) {
        if cell_index >= LEAF_NODE_CELL_MAX_NUM {
            panic!("TODO: need to split page");
        }
        let n_cells = self.get_n_cells();
        let mut i = n_cells;
        let leaf_cells = self.get_mut_leaf_cells();
        while cell_index < i {
            leaf_cells[i] = leaf_cells[i - 1].take();
            i -= 1;
        }
        leaf_cells[cell_index] = Some(cell);
        self.n_cells = (n_cells as u32) + 1;
    }
    fn get_max_key(&self) -> i64 {
        let index = self.get_n_cells() - 1;
        match self.kind {
            NodeKind::Leaf => {
                self.read_leaf_cell(index).unwrap().key
            }
            NodeKind::Internal => {
                self.read_internal_cell(index).unwrap().key
            }
        }
    }
    fn get_child_index(&self, child_index: usize) -> usize {
        match self.kind {
            NodeKind::Leaf => panic!("ERROR: get_child_index must be called by internal node."),
            NodeKind::Internal => {
                let n_cells = self.get_n_cells();
                if child_index > n_cells {
                    panic!("child_index out of bound");
                } else if child_index == n_cells {
                    self.right_child.unwrap() as usize
                } else {
                    let leaf_cell = self.read_internal_cell(child_index);
                    leaf_cell.unwrap().child as usize
                }
            }
        }
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

fn print_with_indentation(indentation: usize, text: &str) {
    println!("{indent}{text}", indent = " ".repeat(indentation * 2));
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
                    println!("CONSTANT:");
                    println!("row size: {}", size_of::<Row>());
                    println!("node header size: {NODE_HEADER_SIZE}");
                    println!("leaf node header size: {LEAF_NODE_HEADER_SIZE}");
                    println!("leaf node cell size: {LEAF_NODE_CELL_SIZE}");
                    println!("leaf node space for cells: {LEAF_NODE_SPACE_FOR_CELLS}");
                    println!("leaf node max cells: {LEAF_NODE_CELL_MAX_NUM}");
                }
                ".tree" => {
                    println!("TREE:");
                    table.pager.print_tree(0, 0);
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
