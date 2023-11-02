from enum import Enum
import os


PAGE_SIZE = 92
ROW_SIZE = 32
TABLE_MAX_PAGES = 1000
INTERNAL_NODE_MAX_CELLS = 2
INVALID_PAGE_NUM = 999

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
LEAF_NODE_NEXT_LEAF_SIZE = 4
LEAF_NODE_NEXT_LEAF_OFFSET = LEAF_NODE_NUM_CELLS_OFFSET + LEAF_NODE_NUM_CELLS_SIZE
LEAF_NODE_HEADER_SIZE = COMMON_NODE_HEADER_SIZE + LEAF_NODE_NUM_CELLS_SIZE + LEAF_NODE_NEXT_LEAF_SIZE

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
    node[LEAF_NODE_NEXT_LEAF_OFFSET:LEAF_NODE_NEXT_LEAF_OFFSET+LEAF_NODE_NEXT_LEAF_SIZE] = (0).to_bytes(LEAF_NODE_NEXT_LEAF_SIZE, 'little')

def initialize_internal_node(node):
    set_node_type(NodeType.NODE_INTERNAL.value, node)
    node[IS_ROOT_OFFSET:IS_ROOT_OFFSET+IS_ROOT_SIZE] = (0).to_bytes(IS_ROOT_SIZE, 'little')
    node[INTERNAL_NODE_NUM_KEYS_OFFSET:INTERNAL_NODE_NUM_KEYS_OFFSET+INTERNAL_NODE_NUM_KEYS_SIZE] \
        = (0).to_bytes(INTERNAL_NODE_NUM_KEYS_SIZE, 'little')
    set_internal_node_right_child(node, INVALID_PAGE_NUM)

def leaf_node_cell(cell_num):
    return LEAF_NODE_HEADER_SIZE + cell_num * LEAF_NODE_CELL_SIZE

def leaf_node_value(cell_num):
    return LEAF_NODE_HEADER_SIZE + cell_num * LEAF_NODE_CELL_SIZE + LEAF_NODE_VALUE_OFFSET

def get_leaf_node_next_leaf(node):
    return int.from_bytes(node[LEAF_NODE_NEXT_LEAF_OFFSET:LEAF_NODE_NEXT_LEAF_OFFSET+LEAF_NODE_NEXT_LEAF_SIZE], 'little')

def get_leaf_num_cells(node) -> int:
    num_cells = int.from_bytes(node[LEAF_NODE_NUM_CELLS_OFFSET:LEAF_NODE_NUM_CELLS_OFFSET+LEAF_NODE_NUM_CELLS_SIZE], byteorder='little')
    return num_cells

def get_node_type(node):
    return int.from_bytes(node[NODE_TYPE_OFFSET:NODE_TYPE_OFFSET + NODE_TYPE_SIZE], byteorder='little')

def set_node_type(node_type:int, node):
    node[NODE_TYPE_OFFSET:NODE_TYPE_OFFSET + NODE_TYPE_SIZE] = (node_type).to_bytes(NODE_TYPE_SIZE, 'little')

def get_parent_page_num(node):
    return int.from_bytes(node[PARENT_POINTER_OFFSET:PARENT_POINTER_OFFSET+PARENT_POINTER_SIZE], byteorder='little')

def internal_node_cell(node, cell_num):
    return node[INTERNAL_NODE_HEADER_SIZE + cell_num * INTERNAL_NODE_CELL_SIZE: 
        INTERNAL_NODE_HEADER_SIZE + cell_num * INTERNAL_NODE_CELL_SIZE + INTERNAL_NODE_CELL_SIZE]



def get_internal_node_num_keys(node) -> int:
    num_keys = int.from_bytes(node[INTERNAL_NODE_NUM_KEYS_OFFSET:INTERNAL_NODE_NUM_KEYS_OFFSET+INTERNAL_NODE_NUM_KEYS_SIZE], byteorder='little')
    return num_keys

def set_intial_internal_node_num_keys(node, num_keys):
    node[INTERNAL_NODE_NUM_KEYS_OFFSET:INTERNAL_NODE_NUM_KEYS_OFFSET+INTERNAL_NODE_NUM_KEYS_SIZE] \
        = (num_keys).to_bytes(INTERNAL_NODE_NUM_KEYS_SIZE, 'little')


def internal_node_child(node, child_num):
    num_keys = get_internal_node_num_keys(node)
    print(f'num of keys: {num_keys} child_num: {child_num}')
    if num_keys < child_num:
        print(f'Tried to access child_num {child_num} > num_keys {num_keys}')
        exit(1)
    elif num_keys == child_num:
        return int.from_bytes(internal_node_right_child(node), byteorder='little')
    else:
        cell = internal_node_cell(node, child_num)

        return int.from_bytes(cell[:INTERNAL_NODE_CHILD_SIZE], byteorder='little')

def set_internal_node_child(node, child_num, child_page_num):
    node[INTERNAL_NODE_HEADER_SIZE + child_num * INTERNAL_NODE_CELL_SIZE: \
        INTERNAL_NODE_HEADER_SIZE + child_num * INTERNAL_NODE_CELL_SIZE + INTERNAL_NODE_CHILD_SIZE] = child_page_num.to_bytes(INTERNAL_NODE_CHILD_SIZE, 'little')

def set_internal_node_key(node, child_num, key):
    b_key = key.encode('utf-8')
    node[INTERNAL_NODE_HEADER_SIZE + child_num * INTERNAL_NODE_CELL_SIZE + INTERNAL_NODE_CHILD_SIZE: \
        INTERNAL_NODE_HEADER_SIZE + child_num * INTERNAL_NODE_CELL_SIZE + INTERNAL_NODE_CHILD_SIZE + len(b_key) ] \
        = b_key

def internal_node_right_child(node):
    return node[INTERNAL_NODE_RIGHT_CHILD_OFFSET:INTERNAL_NODE_RIGHT_CHILD_OFFSET+INTERNAL_NODE_RIGHT_CHILD_SIZE]

def set_internal_node_right_child(node, child_page_num):
    node[INTERNAL_NODE_RIGHT_CHILD_OFFSET:INTERNAL_NODE_RIGHT_CHILD_OFFSET + INTERNAL_NODE_RIGHT_CHILD_SIZE] \
        = child_page_num.to_bytes(INTERNAL_NODE_RIGHT_CHILD_SIZE, 'little')


def internal_node_key(node, cell_num):
    return internal_node_cell(node, cell_num)[INTERNAL_NODE_CHILD_SIZE:INTERNAL_NODE_CHILD_SIZE+INTERNAL_NODE_KEY_SIZE].decode('utf-8')





def get_node_max_key(pager, node):
    if get_node_type(node) == NodeType.NODE_INTERNAL.value:
        right_child_ptr = int.from_bytes(internal_node_right_child(node), byteorder='little')
        right_child = pager.get_page(right_child_ptr)
        return get_node_max_key(pager, right_child)
    else:
        num_cells = get_leaf_num_cells(node)
        return node[leaf_node_cell(num_cells-1):leaf_node_cell(num_cells-1)+LEAF_NODE_KEY_SIZE].decode('utf-8')
           

def print_leaf_node(node, level):
    num_cells = get_leaf_num_cells(node)
    for i in range(num_cells):
        b_key = node[leaf_node_cell(i):leaf_node_cell(i)+LEAF_NODE_KEY_SIZE]
        b_value = node[leaf_node_value(i):leaf_node_value(i)+LEAF_NODE_VALUE_SIZE]
        key = b_key.decode('utf-8')
        val = b_value.decode('utf-8')
        hold = ''
        for j in range(level):
            hold = hold + '--'
        print(f'{hold}leaf node i:{i} key:{key} value:{val}')

def print_tree(pager, node, level=0):
    node_type = get_node_type(node)
    # print(f'node_type:{node_type}')
    if node_type == NodeType.NODE_LEAF.value:
        print_leaf_node(node, level)
    else:
        num_keys = get_internal_node_num_keys(node)
        if num_keys > 0 :
            for i in range(num_keys):
                child_ptr = internal_node_child(node, i)
                child_page = pager.get_page(child_ptr)
                # print(f'child_page:{internal_node_child(node, i)} child_num:{i}')
                # print(get_node_type(child_page))
                hold = ''
                for j in range(level):
                    hold = hold + '--'
                print_tree(pager, child_page, level+1)
                print(f'{hold}internal {i} node key:{internal_node_key(node, i)}')
            right_child_ptr = int.from_bytes(internal_node_right_child(node), byteorder='little')
            # print(f'right_child_ptr:{right_child_ptr}')
            right_child = pager.get_page(right_child_ptr)
            print_tree(pager, right_child, level+1)

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

    root = table.pager.get_page(table.root_page_num)
    right_child = table.pager.get_page(right_child_page_num)
    left_child_page_num = get_unused_page_num(table.pager)
    left_child = table.pager.get_page(left_child_page_num)

    if get_node_type(root) ==  NodeType.NODE_INTERNAL.value:
        initialize_internal_node(right_child)
        initialize_internal_node(left_child)

    left_child[:] = root[:]
    left_child[IS_ROOT_OFFSET:IS_ROOT_OFFSET+IS_ROOT_SIZE] = (0).to_bytes(IS_ROOT_SIZE, 'little')
    # print('left_child_page_num left_child get_node_type', left_child_page_num, get_node_type(left_child))

    if get_node_type(left_child) == NodeType.NODE_INTERNAL.value:
        for i in range(get_internal_node_num_keys(left_child)):
            child = table.pager.get_page(internal_node_child(left_child, i))
            child[PARENT_POINTER_OFFSET:PARENT_POINTER_OFFSET+PARENT_POINTER_SIZE] = left_child_page_num.to_bytes(PARENT_POINTER_SIZE, 'little')
        child_ptr = int.from_bytes(internal_node_right_child(left_child),byteorder='little')
        print(f'child_ptr:{child_ptr}')
        child = table.pager.get_page(child_ptr)
        child[PARENT_POINTER_OFFSET:PARENT_POINTER_OFFSET+PARENT_POINTER_SIZE] = left_child_page_num.to_bytes(PARENT_POINTER_SIZE, 'little')
            

    initialize_internal_node(root)
    root[IS_ROOT_OFFSET:IS_ROOT_OFFSET+IS_ROOT_SIZE] = (1).to_bytes(IS_ROOT_SIZE, 'little')

    
    root[INTERNAL_NODE_NUM_KEYS_OFFSET:INTERNAL_NODE_NUM_KEYS_OFFSET+INTERNAL_NODE_NUM_KEYS_SIZE] \
        = (1).to_bytes(INTERNAL_NODE_NUM_KEYS_SIZE, 'little')
    root[INTERNAL_NODE_HEADER_SIZE: INTERNAL_NODE_HEADER_SIZE + INTERNAL_NODE_CHILD_SIZE] \
        = (left_child_page_num).to_bytes(INTERNAL_NODE_CHILD_SIZE, byteorder='little')

    
    b_max_key = get_node_max_key(table.pager, left_child).encode('utf-8')

    root[INTERNAL_NODE_HEADER_SIZE+INTERNAL_NODE_CHILD_SIZE: \
        INTERNAL_NODE_HEADER_SIZE+INTERNAL_NODE_CHILD_SIZE + len(b_max_key)] \
        = b_max_key

    set_internal_node_right_child(root, right_child_page_num)

    left_child[PARENT_POINTER_OFFSET:PARENT_POINTER_OFFSET+PARENT_POINTER_SIZE] = (table.root_page_num).to_bytes(PARENT_POINTER_SIZE, byteorder='little')
    right_child[PARENT_POINTER_OFFSET:PARENT_POINTER_OFFSET+PARENT_POINTER_SIZE] = (table.root_page_num).to_bytes(PARENT_POINTER_SIZE, byteorder='little')
    
    


