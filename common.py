from enum import Enum
import os


PAGE_SIZE = 46
ROW_SIZE = 32
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
    NODE_INTERNAL = 0
    NODE_LEAF = 1


# node header 6
NODE_TYPE_SIZE = 1
NODE_TYPE_OFFSET = 0
IS_ROOT_SIZE = 1
IS_ROOT_OFFSET = NODE_TYPE_SIZE
PARENT_POINTER_SIZE = 4
PARENT_POINTER_OFFSET = IS_ROOT_OFFSET + IS_ROOT_SIZE
COMMON_NODE_HEADER_SIZE =  NODE_TYPE_SIZE + IS_ROOT_SIZE + PARENT_POINTER_SIZE

# leaf node header 10
LEAF_NODE_NUM_CELLS_SIZE = 4
LEAF_NODE_NUM_CELLS_OFFSET = COMMON_NODE_HEADER_SIZE
LEAF_NODE_HEADER_SIZE = COMMON_NODE_HEADER_SIZE + LEAF_NODE_NUM_CELLS_SIZE

# leaf node body 46
LEAF_NODE_KEY_SIZE = 4
LEAF_NODE_KEY_OFFSET = 0
LEAF_NODE_VALUE_SIZE = ROW_SIZE
LEAF_NODE_VALUE_OFFSET = LEAF_NODE_KEY_OFFSET + LEAF_NODE_KEY_SIZE
LEAF_NODE_CELL_SIZE = LEAF_NODE_KEY_SIZE + LEAF_NODE_VALUE_SIZE
LEAF_NODE_SPACE_FOR_CELLS = PAGE_SIZE - LEAF_NODE_HEADER_SIZE
LEAF_NODE_MAX_CELLS = int(LEAF_NODE_SPACE_FOR_CELLS / LEAF_NODE_CELL_SIZE)

# split leaf node
LEAF_NODE_RIGHT_SPLIT_COUNT = int((LEAF_NODE_MAX_CELLS + 1) / 2)
LEAF_NODE_LEFT_SPLIT_COUNT = LEAF_NODE_MAX_CELLS + 1 - LEAF_NODE_RIGHT_SPLIT_COUNT

# Internal Node Header Layout
INTERNAL_NODE_NUM_KEYS_SIZE = 4
INTERNAL_NODE_NUM_KEYS_OFFSET = COMMON_NODE_HEADER_SIZE
INTERNAL_NODE_RIGHT_CHILD_SIZE = 4
INTERNAL_NODE_RIGHT_CHILD_OFFSET = INTERNAL_NODE_NUM_KEYS_OFFSET + INTERNAL_NODE_NUM_KEYS_SIZE

INTERNAL_NODE_HEADER_SIZE = COMMON_NODE_HEADER_SIZE + INTERNAL_NODE_NUM_KEYS_SIZE + INTERNAL_NODE_RIGHT_CHILD_SIZE

# Internal Node Body Layout
INTERNAL_NODE_KEY_SIZE = 4
INTERNAL_NODE_CHILD_SIZE = 4
INTERNAL_NODE_CELL_SIZE = INTERNAL_NODE_CHILD_SIZE + INTERNAL_NODE_KEY_SIZE

def print_constants():

    print(f'LEAF_NODE_MAX_CELLS:{LEAF_NODE_MAX_CELLS}')

    print(f'LEAF_NODE_RIGHT_SPLIT_COUNT:{LEAF_NODE_RIGHT_SPLIT_COUNT}')
    print(f'LEAF_NODE_LEFT_SPLIT_COUNT:{LEAF_NODE_LEFT_SPLIT_COUNT}')
    


def initialize_leaf_node(node):
    set_node_type(NodeType.NODE_LEAF.value, node)
    node[LEAF_NODE_NUM_CELLS_OFFSET:LEAF_NODE_NUM_CELLS_OFFSET+LEAF_NODE_NUM_CELLS_SIZE] = (0).to_bytes(LEAF_NODE_NUM_CELLS_SIZE, 'little')
    node[IS_ROOT_OFFSET:IS_ROOT_OFFSET+IS_ROOT_SIZE] = (0).to_bytes(IS_ROOT_SIZE, 'little')

def initialize_internal_node(node):
    set_node_type(NodeType.NODE_INTERNAL.value, node)
    node[LEAF_NODE_NUM_CELLS_OFFSET:LEAF_NODE_NUM_CELLS_OFFSET+LEAF_NODE_NUM_CELLS_SIZE] = (0).to_bytes(LEAF_NODE_NUM_CELLS_SIZE, 'little')

def leaf_node_cell(cell_num):
    return LEAF_NODE_HEADER_SIZE + cell_num * LEAF_NODE_CELL_SIZE

def leaf_node_value(cell_num):
    return LEAF_NODE_HEADER_SIZE + cell_num * LEAF_NODE_CELL_SIZE + LEAF_NODE_VALUE_OFFSET

def get_node_type(node):
    return int.from_bytes(node[NODE_TYPE_OFFSET:NODE_TYPE_OFFSET + NODE_TYPE_SIZE], byteorder='little')

def set_node_type(node_type:int, node):
    node[NODE_TYPE_OFFSET:NODE_TYPE_OFFSET + NODE_TYPE_SIZE] = (node_type).to_bytes(NODE_TYPE_SIZE, 'little')

def internal_node_cell(node, cell_num):
    return node[INTERNAL_NODE_HEADER_SIZE + cell_num * INTERNAL_NODE_CELL_SIZE: 
        INTERNAL_NODE_HEADER_SIZE + cell_num * INTERNAL_NODE_CELL_SIZE + INTERNAL_NODE_CELL_SIZE]

def internal_node_child(node, child_num):
    num_keys = int.from_bytes(node[INTERNAL_NODE_NUM_KEYS_OFFSET:INTERNAL_NODE_NUM_KEYS_OFFSET+INTERNAL_NODE_NUM_KEYS_SIZE], byteorder='little')
    if num_keys < child_num:
        print(f'Tried to access child_num {child_num} > num_keys {num_keys}')
        exit(1)
    elif num_keys == child_num:
        return int.from_bytes(node[INTERNAL_NODE_RIGHT_CHILD_OFFSET:INTERNAL_NODE_RIGHT_CHILD_OFFSET+INTERNAL_NODE_RIGHT_CHILD_SIZE]
            , byteorder='little')
    else:
        cell = internal_node_cell(node, child_num)

        return int.from_bytes(cell[INTERNAL_NODE_KEY_SIZE:INTERNAL_NODE_KEY_SIZE+INTERNAL_NODE_CHILD_SIZE], byteorder='little')


def print_leaf_node(node):
    b_num_cells = node[LEAF_NODE_NUM_CELLS_OFFSET:LEAF_NODE_NUM_CELLS_OFFSET + LEAF_NODE_NUM_CELLS_SIZE]
    num_cells = int.from_bytes(b_num_cells, byteorder='little')
    for i in range(num_cells):
        b_key = node[leaf_node_cell(i):leaf_node_cell(i)+LEAF_NODE_KEY_SIZE]
        b_value = node[leaf_node_value(i):leaf_node_value(i)+LEAF_NODE_VALUE_SIZE]
        key = b_key.decode('utf-8')
        val = b_value.decode('utf-8')
        print(f'i:{i} key:{key} value:{val}')

def print_tree(pager, node):
    node_type = get_node_type(node)
    print(f'node_type:{node_type}')
    if node_type == NodeType.NODE_LEAF.value:
        print_leaf_node(node)
    else:
        # num_keys = int.from_bytes(node[INTERNAL_NODE_NUM_KEYS_OFFSET:
        #     INTERNAL_NODE_NUM_KEYS_OFFSET + INTERNAL_NODE_NUM_KEYS_SIZE], byteorder='little')
        # for i in range(num_keys):
        #     child_page = pager.get_page(internal_node_child(node, i))
        #     print_tree(pager, child_page)
        right_child_ptr = int.from_bytes(node[INTERNAL_NODE_RIGHT_CHILD_OFFSET:
            INTERNAL_NODE_RIGHT_CHILD_OFFSET+INTERNAL_NODE_RIGHT_CHILD_SIZE], byteorder='little')
        print(f'right_child_ptr:{right_child_ptr}')
        right_child = pager.get_page(right_child_ptr)
        print_tree(pager, right_child)
        
        print('internal node ', node[INTERNAL_NODE_HEADER_SIZE:INTERNAL_NODE_HEADER_SIZE+INTERNAL_NODE_KEY_SIZE].decode('utf-8'))


def deserialize_row(page, row_offset):
    id = page[row_offset:row_offset+4].decode()
    username = page[row_offset+4:row_offset+32].decode()
    return id, username

def serialize_row(value, page, row_offset):

    b_id = bytearray(value.id.encode('utf8'))
    b_username = bytearray(value.username.encode('utf8'))
    # page.extend(bytearray(32))
    page[row_offset:row_offset+len(b_id)] = b_id
    page[row_offset+4:row_offset+4+len(b_username)] = b_username

def get_unused_page_num(pager):
    print(f'newpage:{pager.num_pages}')
    return pager.num_pages

def create_new_root(table, right_child_page_num):
    print(f'table.root_page_num:{table.root_page_num}')
    root = table.pager.get_page(table.root_page_num)
    right_child = table.pager.get_page(right_child_page_num)
    left_child_page_num = get_unused_page_num(table.pager)
    left_child = table.pager.get_page(left_child_page_num)

    left_child = root[:]
    left_child[IS_ROOT_OFFSET:IS_ROOT_OFFSET+IS_ROOT_SIZE] = (0).to_bytes(IS_ROOT_SIZE, 'little')

    initialize_internal_node(root)
    root[IS_ROOT_OFFSET:IS_ROOT_OFFSET+IS_ROOT_SIZE] = (1).to_bytes(IS_ROOT_SIZE, 'little')
    root[INTERNAL_NODE_RIGHT_CHILD_OFFSET:INTERNAL_NODE_RIGHT_CHILD_OFFSET+INTERNAL_NODE_RIGHT_CHILD_SIZE] \
        = right_child_page_num.to_bytes(INTERNAL_NODE_RIGHT_CHILD_SIZE, 'little')
    root[INTERNAL_NODE_NUM_KEYS_OFFSET:INTERNAL_NODE_NUM_KEYS_OFFSET+INTERNAL_NODE_NUM_KEYS_SIZE] \
        = (1).to_bytes(INTERNAL_NODE_NUM_KEYS_SIZE, 'little')

