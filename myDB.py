from ctypes import sizeof

from cursor import Cursor
from table import Table, Row
from pager import Pager
from common import *

class Statement():
    def __init__(self) -> None:
        self.type = ''
        self.row_to_insert = Row()


def db_open(db_file) -> Table:
    
    pager = Pager(db_file)

    table = Table(pager)
    if pager.num_pages == 0:
        root_node = pager.get_page(0)
        initialize_leaf_node(root_node)
        root_node[IS_ROOT_OFFSET:IS_ROOT_OFFSET+IS_ROOT_SIZE] = (1).to_bytes(IS_ROOT_SIZE, 'little')
    return table
    
def db_close(table:Table):
    pager = table.pager
    print('num pages', pager.num_pages)
    for i in range(pager.num_pages):
        # pager.pages[i] = bytearray()
        if pager.pages[i] == None:
            continue
        pager.flush_page(i)
    
def do_meta_command(inputBuffer, table:Table):
    if inputBuffer[0:6] == '.btree':
        node = table.pager.get_page(table.root_page_num)
        print_tree(table.pager, node)
    elif inputBuffer[0:6] == '.print':
        print_constants()
    else:
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

def execute_insert(statement:Statement, table:Table):
    node = table.pager.get_page(table.root_page_num)
    num_cells = int.from_bytes(node[LEAF_NODE_NUM_CELLS_OFFSET:LEAF_NODE_NUM_CELLS_OFFSET+LEAF_NODE_NUM_CELLS_SIZE], byteorder='little')
    
    cursor = table.find_key(statement.row_to_insert.id)
    if cursor.cell_num < num_cells:
        b_key_at_index = node[leaf_node_cell(cursor.cell_num):leaf_node_cell(cursor.cell_num)+LEAF_NODE_KEY_SIZE]
        key_at_index = b_key_at_index.decode('utf-8')
        if key_at_index == statement.row_to_insert.id:
            return ExecResult.EXECUTE_DUPLICATE_KEY
        
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
            do_meta_command(inputBuffer, table)
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
        


