from ctypes import sizeof
from enum import Enum
import os


PAGE_SIZE = 4096
ROW_SIZE = 32
ROWS_PER_PAGE = 128
MAX_ROWS = 1000
TABLE_MAX_PAGES = 1000

class PrepareResult(Enum):
    PREPARE_SUCCESS = 1
    PREPARE_UNRECOGNIZED_STATEMENT = 2
    PREPARE_SYNTAX_ERROR = 3


class ExecResult(Enum):
    EXECUTE_SUCCESS = 1
    EXECUTE_TABLE_FULL = 2
    EXECUTE_FAIL = 3

class NodeType(Enum):
    NODE_INTERNAL = 1
    NODE_LEAF = 2


# node header
NODE_TYPE_SIZE = 1
NODE_TYPE_OFFSET = 0
IS_ROOT_SIZE = 1
IS_ROOT_OFFSET = NODE_TYPE_SIZE
PARENT_POINTER_SIZE = 4
PARENT_POINTER_OFFSET = IS_ROOT_OFFSET + IS_ROOT_SIZE
COMMON_NODE_HEADER_SIZE =  NODE_TYPE_SIZE + IS_ROOT_SIZE + PARENT_POINTER_SIZE

# leaf node header
LEAF_NODE_NUM_CELLS_SIZE = 4
LEAF_NODE_NUM_CELLS_OFFSET = COMMON_NODE_HEADER_SIZE
LEAF_NODE_HEADER_SIZE = COMMON_NODE_HEADER_SIZE + LEAF_NODE_NUM_CELLS_SIZE

# leaf node body
LEAF_NODE_KEY_SIZE = 4
LEAF_NODE_KEY_OFFSET = 0
LEAF_NODE_VALUE_SIZE = ROW_SIZE
LEAF_NODE_VALUE_OFFSET = LEAF_NODE_KEY_OFFSET + LEAF_NODE_KEY_SIZE
LEAF_NODE_CELL_SIZE = LEAF_NODE_KEY_SIZE + LEAF_NODE_VALUE_SIZE
LEAF_NODE_SPACE_FOR_CELLS = PAGE_SIZE - LEAF_NODE_HEADER_SIZE
LEAF_NODE_MAX_CELLS = LEAF_NODE_SPACE_FOR_CELLS / LEAF_NODE_CELL_SIZE

class Pager():
    def __init__(self, filename):
        # self.file_lenth = os.path.getsize(filename)
        
        try:
            # fd = open(filename, 'wb+')
            fd = os.open(filename, os.O_RDWR | os.O_CREAT)
            # self.file_lenth = fd.seek(0, os.SEEK_END)
            # fd = os.open(filename, os.O_RDWR | os.O_CREAT)
        except Exception as e:
            print(f'Open pager err: {e}')
        self.fd = fd
        self.file_lenth = os.lseek(fd, 0, os.SEEK_END)
        
        print(f'file_lenth:{self.file_lenth}')
        self.pages = []
        for i in range(0, TABLE_MAX_PAGES):
            self.pages.append(None)
        self.num_pages = int(self.file_lenth / PAGE_SIZE)
        if self.file_lenth % PAGE_SIZE != 0:
            raise Exception(f'file_lenth:{self.file_lenth} not divide PAGE_SIZE')
    
    
    def get_page(self, page_num):
        page = bytearray(PAGE_SIZE)
        num_pages = int(self.file_lenth / PAGE_SIZE)
        if self.file_lenth % PAGE_SIZE != 0:
            num_pages += 1
        if self.pages[page_num] == None:
            if num_pages > page_num:
                os.lseek(self.fd, page_num * PAGE_SIZE, os.SEEK_SET)
                self.pages[page_num] = bytearray(os.read(self.fd, PAGE_SIZE))
            else:
                self.pages[page_num] = page
            if page_num >= self.num_pages:
                self.num_pages = int(page_num + 1)
        
        return self.pages[page_num]

    def flush_page(self, page_num):
        # self.fd.seek(page_num * PAGE_SIZE, os.SEEK_SET)
        # self.fd.write(self.pages[page_num])
        os.lseek(self.fd, page_num * PAGE_SIZE, os.SEEK_SET)
        os.write(self.fd, self.pages[page_num])

def initialize_leaf_node(page):
    page[LEAF_NODE_NUM_CELLS_OFFSET:LEAF_NODE_NUM_CELLS_OFFSET+LEAF_NODE_NUM_CELLS_SIZE] = (0).to_bytes(LEAF_NODE_NUM_CELLS_SIZE, 'little')

def leaf_node_cell(cell_num):
    return LEAF_NODE_HEADER_SIZE + cell_num * LEAF_NODE_CELL_SIZE

def leaf_node_value(cell_num):
    return LEAF_NODE_HEADER_SIZE + cell_num * LEAF_NODE_CELL_SIZE + LEAF_NODE_VALUE_OFFSET

class Table():
    def __init__(self, pager:Pager) -> None:
        self.root_page_num = 0
        self.pager = pager
        pass

    def table_start(self):
        cursor = Cursor(self)
        cursor.page_num = self.root_page_num
        root_node = self.pager.get_page(self.root_page_num)
        num_cells = int.from_bytes(root_node[LEAF_NODE_NUM_CELLS_OFFSET:LEAF_NODE_CELL_SIZE], byteorder='little')
        cursor.end_of_table = (num_cells == 0)

        return cursor

    def table_finish(self):
        cursor = Cursor(self)
        cursor.page_num = self.root_page_num
        root_node = self.pager.get_page(self.root_page_num)
        num_cells = int.from_bytes(root_node[LEAF_NODE_NUM_CELLS_OFFSET:LEAF_NODE_CELL_SIZE], byteorder='little')
        cursor.cell_num = num_cells
        cursor.end_of_table = True
        return cursor

class Row():
    def __init__(self) -> None:
        self.id = ''
        self.username = ''
    
    def __str__(self) -> str:
        return f'<Row {self.id} {self.username}>'

class Statement():
    def __init__(self) -> None:
        self.type = ''
        self.row_to_insert = Row()

class Cursor():
    def __init__(self, table:Table) -> None:
        self.table = table
        self.end_of_table = False
        self.page_num = 0
        self.cell_num = 0

    def get_value(self):
        page_num = self.page_num
        print(f'pnum:{page_num}')
        
        return table.pager.get_page(page_num)[LEAF_NODE_VALUE_OFFSET:LEAF_NODE_VALUE_OFFSET+LEAF_NODE_VALUE_SIZE]

    def advance(self):
        page = self.table.pager.get_page(self.page_num)
        self.cell_num += 1
        num_cells = int.from_bytes(page[LEAF_NODE_NUM_CELLS_OFFSET:LEAF_NODE_NUM_CELLS_OFFSET+LEAF_NODE_NUM_CELLS_SIZE], byteorder='little')
        print(f'num_cells:{num_cells} cell_num:{self.cell_num}')
        if self.cell_num >= num_cells:
            self.end_of_table = True

    def leaf_node_insert(self, key, value:Row):
        node = self.table.pager.get_page(self.page_num)
        num_cells =  int.from_bytes(node[LEAF_NODE_NUM_CELLS_OFFSET:LEAF_NODE_NUM_CELLS_OFFSET+LEAF_NODE_NUM_CELLS_SIZE], byteorder='little')
        if num_cells >= LEAF_NODE_MAX_CELLS:
            raise Exception(f'Too many cells found {num_cells}')
        if self.cell_num < num_cells:
            print('debug0')
            index = num_cells
            while index > self.cell_num:
                node[leaf_node_cell(index):leaf_node_cell(index)+LEAF_NODE_CELL_SIZE] = node[leaf_node_cell(index-1):LEAF_NODE_CELL_SIZE]
                index -= 1
        b_key = bytearray(key.encode('utf-8'))
        node[leaf_node_cell(num_cells):leaf_node_cell(num_cells)+len(b_key)] = b_key
        serialize_row(value, node, leaf_node_value(num_cells))
        num_cells += 1
        node[LEAF_NODE_NUM_CELLS_OFFSET:LEAF_NODE_NUM_CELLS_OFFSET+LEAF_NODE_NUM_CELLS_SIZE] = num_cells.to_bytes(LEAF_NODE_NUM_CELLS_SIZE, 'little')




def db_open(db_file) -> Table:
    # if os.path.getsize(db_file) == 0:
    #     return table
    
    pager = Pager(db_file)
    # table.pager = pager
    
        # table.num_rows = int.from_bytes(fd.read(4), byteorder='little')
        # table.pages = [bytearray(fd.read(PAGE_SIZE)) for _ in range(int(table.num_rows/ROWS_PER_PAGE) + 1)]
    table = Table(pager)
    if pager.num_pages == 0:
        root_node = pager.get_page(0)
        initialize_leaf_node(root_node)
    return table
    

def db_close(table:Table):
    pager = table.pager
    print('num pages', pager.num_pages)
    for i in range(pager.num_pages):
        # pager.pages[i] = bytearray()
        if pager.pages[i] == None:
            continue
        pager.flush_page(i)
    
def do_meta_command(inputBuffer):
    print(f'Unrecognized command {inputBuffer}')

def prepare_statement(inputBuffer, statement:Statement):
    if inputBuffer[0:6] =='select':
        statement.type = 'SELECT'
    elif inputBuffer[0:6] =='insert':
        if len(inputBuffer.split()) < 3:
            return PrepareResult.PREPARE_SYNTAX_ERROR

        statement.type = 'INSERT'
        statement.row_to_insert.id = inputBuffer.split()[1]
        statement.row_to_insert.username = inputBuffer.split()[2]
    else:
        return PrepareResult.PREPARE_UNRECOGNIZED_STATEMENT

    return PrepareResult.PREPARE_SUCCESS

def row_slot(table:Table, row_num):
    row = Row()
    # page_num = (row_num * ROW_SIZE) / PAGE_SIZE
    page_num = int(row_num / ROWS_PER_PAGE)
    if page_num >= len(table.pages):
        table.pages.append(bytearray(PAGE_SIZE))
    page = table.pages[page_num]
    row_offset = (row_num % ROWS_PER_PAGE) * ROW_SIZE
    return page, row_offset

def deserialize_row(page, row_offset):
    id = page[row_offset:row_offset+16].decode()
    username = page[row_offset+16:row_offset+32].decode()
    return id, username

def serialize_row(value, page, row_offset):

    b_id = bytearray(value.id.encode('utf8'))
    b_username = bytearray(value.username.encode('utf8'))
    # page.extend(bytearray(32))
    page[row_offset:row_offset+len(b_id)] = b_id
    page[row_offset+16:row_offset+16+len(b_username)] = b_username



def execute_insert(statement:Statement, table:Table):
    cursor = table.table_finish()
    node = table.pager.get_page(table.root_page_num)
    cursor.leaf_node_insert(statement.row_to_insert.id, statement.row_to_insert)
    return ExecResult.EXECUTE_SUCCESS 


def execute_statment(statement:Statement, table:Table):
    if statement.type == 'SELECT':
        cursor = table.table_start()
        while not cursor.end_of_table:
    
            id, username = deserialize_row(table.pager.get_page(cursor.page_num), leaf_node_value(cursor.cell_num))
            cursor.advance()
            print(f'id:{id} username:{username}')
        print("This is where we would do an select.")
        return ExecResult.EXECUTE_SUCCESS   
    elif statement.type == 'INSERT':
        print("This is where we would do an insert.")  
        result = execute_insert(statement, table)
        return result
  

if __name__ == '__main__': 
    table = db_open('btree.db')
    while (inputBuffer := input('db >')) != '.exit':
        if inputBuffer[0] == '.':
            do_meta_command(inputBuffer)
        else:
            statment = Statement()
            prepare_reuslt = prepare_statement(inputBuffer, statment)
            if prepare_reuslt == PrepareResult.PREPARE_SUCCESS:
                result = execute_statment(statment, table)
                if result == ExecResult.EXECUTE_SUCCESS:
                    print('Successfully executed')
                else:
                    print(f'Execute failed: {result.name}')
            elif prepare_reuslt == PrepareResult.PREPARE_SYNTAX_ERROR:
                print(f'Syntax error: {inputBuffer}')
            else:
                print(f'Unrecognized statement {inputBuffer}')
    db_close(table)
        


