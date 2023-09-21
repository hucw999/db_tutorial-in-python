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
    EXECUTE_DUPLICATE_KEY = 4

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


def initialize_leaf_node(node):
    set_node_type(NodeType.NODE_LEAF.value, node)
    node[LEAF_NODE_NUM_CELLS_OFFSET:LEAF_NODE_NUM_CELLS_OFFSET+LEAF_NODE_NUM_CELLS_SIZE] = (0).to_bytes(LEAF_NODE_NUM_CELLS_SIZE, 'little')

def leaf_node_cell(cell_num):
    return LEAF_NODE_HEADER_SIZE + cell_num * LEAF_NODE_CELL_SIZE

def leaf_node_value(cell_num):
    return LEAF_NODE_HEADER_SIZE + cell_num * LEAF_NODE_CELL_SIZE + LEAF_NODE_VALUE_OFFSET

def get_node_type(node):
    return int.from_bytes(node[NODE_TYPE_OFFSET:NODE_TYPE_OFFSET + NODE_TYPE_SIZE], byteorder='little')

def set_node_type(node_type:int, node):
    node[NODE_TYPE_OFFSET:NODE_TYPE_OFFSET + NODE_TYPE_SIZE] = (node_type).to_bytes(NODE_TYPE_SIZE, 'little')



def print_leaf_node(node):
    b_num_cells = node[LEAF_NODE_NUM_CELLS_OFFSET:LEAF_NODE_NUM_CELLS_OFFSET + LEAF_NODE_NUM_CELLS_SIZE]
    num_cells = int.from_bytes(b_num_cells, byteorder='little')
    for i in range(num_cells):
        b_key = node[leaf_node_cell(i):leaf_node_cell(i)+LEAF_NODE_KEY_SIZE]
        b_value = node[leaf_node_value(i):leaf_node_value(i)+LEAF_NODE_VALUE_SIZE]
        key = b_key.decode('utf-8')
        val = b_value.decode('utf-8')
        print(f'i:{i} key:{key} value:{val}')

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