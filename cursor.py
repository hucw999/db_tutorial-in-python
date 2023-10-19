from secrets import token_bytes
from common import *

def leaf_node_find(table, page_num, key):
    node = table.pager.get_page(page_num)
    cell_nums = get_leaf_num_cells(node)
    l = 0
    r = cell_nums
    cursor = Cursor(table)
    cursor.page_num = page_num
    while l != r:
        mid = int((l+r) / 2)
        print(l,r,mid)
        mid_key = node[leaf_node_cell(mid):leaf_node_cell(mid)+LEAF_NODE_KEY_SIZE].decode('utf-8')
        
        if mid_key == key:
            cursor.cell_num = mid
            return cursor
        elif mid_key > key:
            r = mid 
        else:
            l = mid + 1
    cursor.cell_num = l
    print('cursor.cell_num', cursor.cell_num)
    return cursor

def update_internal_node_key(node, old_key, new_key):
    old_child_index = internal_node_find_child(node, old_key)
    b_new_key = new_key.encode('utf-8')
    node[INTERNAL_NODE_HEADER_SIZE + old_child_index * INTERNAL_NODE_CELL_SIZE + INTERNAL_NODE_CHILD_SIZE:
        INTERNAL_NODE_HEADER_SIZE + old_child_index * INTERNAL_NODE_CELL_SIZE + INTERNAL_NODE_CHILD_SIZE + len(b_new_key)] \
        = b_new_key

def internal_node_find_child(node, key):
    num_keys = get_internal_node_num_keys(node)
    l = 0
    r = num_keys
    while l != r:
        mid = int((l+r) / 2)
        mid_key = internal_node_key(node, mid)
        if mid_key >= key:
            r = mid
        else:
            l = mid + 1
    return l

def internal_node_find(table, page_num, key):
    node = table.pager.get_page(page_num)
    child_index = internal_node_find_child(node, key)
    child_num = internal_node_child(node, child_index)
    child_node = table.pager.get_page(child_num)
    if get_node_type(child_node) == NodeType.NODE_INTERNAL.value:
        return internal_node_find(table, child_num, key)
    else:
        return leaf_node_find(table, child_num, key)
        
def internal_node_split_and_insert(table, parent_page_num, child_page_num):
    print('internel node split and insert')
    old_page_num = parent_page_num
    old_node = table.pager.get_page(old_page_num)
    old_max = get_node_max_key(table.pager, old_node)
    
    child = table.pager.get_page(child_page_num)
    child_max = get_node_max_key(table.pager, child)
    
    new_page_num = get_unused_page_num(table.pager)

    splitting_root = int.from_bytes(old_node[IS_ROOT_OFFSET:IS_ROOT_OFFSET+IS_ROOT_SIZE], byteorder='little')

    if splitting_root == 1:
        create_new_root(table, new_page_num)
        parent = table.pager.get_page(table.root_page_num)
        old_page_num = internal_node_child(parent, 0)
        old_node = table.pager.get_page(old_page_num)
    else:
        
        parent = table.pager.get_page(get_parent_page_num(old_node))
        new_node = table.pager.get_page(new_page_num)
        initialize_internal_node(new_node)
    
    old_num_keys = get_internal_node_num_keys(old_node)
    cur_page_num = int.from_bytes(internal_node_right_child(old_node), byteorder='little')
    cur_node = table.pager.get_page(cur_page_num)

    # First put right child into new node and set right child of old node to invalid page number
    internal_node_insert(table, new_page_num, cur_page_num)
    cur_node[PARENT_POINTER_OFFSET:PARENT_POINTER_OFFSET+PARENT_POINTER_SIZE] = new_page_num.to_bytes(PARENT_POINTER_SIZE, byteorder='little')
    set_internal_node_right_child(old_node, INVALID_PAGE_NUM)

    # For each key until you get to the middle key, move the key and the child to the new node
    for i in range(INTERNAL_NODE_MAX_CELLS-1, int((INTERNAL_NODE_MAX_CELLS) / 2), -1):
        cur_page_num = internal_node_child(old_node, i)
        cur = table.pager.get_page(cur_page_num)
        internal_node_insert(table, new_page_num, cur_page_num)
        cur[PARENT_POINTER_OFFSET:PARENT_POINTER_OFFSET+PARENT_POINTER_SIZE] = new_page_num.to_bytes(PARENT_POINTER_SIZE, byteorder='little')
        old_num_keys -= 1
        set_intial_internal_node_num_keys(old_node, old_num_keys)

    set_internal_node_right_child(old_node, internal_node_child(old_node, old_num_keys - 1))
    old_num_keys -= 1
    set_intial_internal_node_num_keys(old_node, old_num_keys)
    # Determine which of the two nodes after the split should contain the child to be inserted, and insert the child
    max_after_split = get_node_max_key(table.pager, old_node)
    destination_page_num = old_page_num if child_max < max_after_split else new_page_num
    internal_node_insert(table, destination_page_num, child_page_num)
    child[PARENT_POINTER_OFFSET:PARENT_POINTER_OFFSET+PARENT_POINTER_SIZE] \
        = destination_page_num.to_bytes(PARENT_POINTER_SIZE, byteorder='little')
    update_internal_node_key(parent, old_max, get_node_max_key(table.pager, old_node))

   
    if splitting_root == 0:
        internal_node_insert(table, get_parent_page_num(old_node), new_page_num)
        new_node[PARENT_POINTER_OFFSET:PARENT_POINTER_OFFSET+PARENT_POINTER_SIZE] \
            = get_parent_page_num(old_node).to_bytes(PARENT_POINTER_SIZE, byteorder='little')

def internal_node_insert(table, parent_page_num, child_page_num):
    parent = table.pager.get_page(parent_page_num)
    child = table.pager.get_page(child_page_num)
    child_max_key = get_node_max_key(table.pager, child)
    index =  internal_node_find_child(parent, child_max_key)

    original_num_keys = get_internal_node_num_keys(parent)
    
    if original_num_keys >= INTERNAL_NODE_MAX_CELLS:
        internal_node_split_and_insert(table, parent_page_num, child_page_num)
        return
    right_child_page_num = int.from_bytes(internal_node_right_child(parent), byteorder='little')
    # An internal node with a right child of INVALID_PAGE_NUM is empty
    if right_child_page_num == INVALID_PAGE_NUM:
        set_internal_node_right_child(parent, child_page_num)
        return
    right_child = table.pager.get_page(right_child_page_num)


    parent[INTERNAL_NODE_NUM_KEYS_OFFSET:INTERNAL_NODE_NUM_KEYS_OFFSET+INTERNAL_NODE_NUM_KEYS_SIZE] \
        = (original_num_keys + 1).to_bytes(INTERNAL_NODE_NUM_KEYS_SIZE, 'little')
    
    if child_max_key > get_node_max_key(table.pager, right_child):
        set_internal_node_child(parent, original_num_keys, right_child_page_num)
        set_internal_node_key(parent, original_num_keys, get_node_max_key(table.pager, right_child))
        
        parent[INTERNAL_NODE_RIGHT_CHILD_OFFSET:INTERNAL_NODE_RIGHT_CHILD_OFFSET+INTERNAL_NODE_RIGHT_CHILD_SIZE] = \
            child_page_num.to_bytes(INTERNAL_NODE_RIGHT_CHILD_SIZE, 'little')
    else:
        for i in range(original_num_keys, index, -1):
            parent[INTERNAL_NODE_HEADER_SIZE + i * INTERNAL_NODE_CELL_SIZE: 
        INTERNAL_NODE_HEADER_SIZE + i * INTERNAL_NODE_CELL_SIZE + INTERNAL_NODE_CELL_SIZE] = \
            internal_node_cell(parent, i-1)
        set_internal_node_child(parent, index, child_page_num)
        set_internal_node_key(parent, index, child_max_key)
    

class Cursor():
    def __init__(self, table) -> None:
        self.table = table
        self.end_of_table = False
        self.page_num = 0
        self.cell_num = 0

    def get_value(self):
        page_num = self.page_num
        print(f'pnum:{page_num}')
        
        return self.table.pager.get_page(page_num)[LEAF_NODE_VALUE_OFFSET:LEAF_NODE_VALUE_OFFSET+LEAF_NODE_VALUE_SIZE]

    def advance(self):
        page = self.table.pager.get_page(self.page_num)
        self.cell_num += 1
        num_cells = get_leaf_num_cells(page)
        if self.cell_num >= num_cells:
            next_page_num = get_leaf_node_next_leaf(page)
            if next_page_num == 0:
                self.end_of_table = True
            else:
                self.page_num= next_page_num
                self.cell_num = 0

    def leaf_node_insert(self, key, value):
        node = self.table.pager.get_page(self.page_num)
        num_cells = get_leaf_num_cells(node)
        if num_cells >= LEAF_NODE_MAX_CELLS:
            self.leaf_node_split_insert(key, value)
            return
            # raise Exception(f'Too many cells found {num_cells}')
        if self.cell_num < num_cells:
            print('debug0')
            index = num_cells
            while index > self.cell_num:
                node[leaf_node_cell(index):leaf_node_cell(index)+LEAF_NODE_CELL_SIZE] = node[leaf_node_cell(index-1):leaf_node_cell(index-1)+LEAF_NODE_CELL_SIZE]
                index -= 1
        b_key = bytearray(key.encode('utf-8'))
        node[leaf_node_cell(self.cell_num):leaf_node_cell(self.cell_num)+len(b_key)] = b_key
        serialize_row(value, node, leaf_node_value(self.cell_num))
        num_cells += 1
        node[LEAF_NODE_NUM_CELLS_OFFSET:LEAF_NODE_NUM_CELLS_OFFSET+LEAF_NODE_NUM_CELLS_SIZE] = num_cells.to_bytes(LEAF_NODE_NUM_CELLS_SIZE, 'little')

    def leaf_node_split_insert(self, key, value):
        old_node = self.table.pager.get_page(self.page_num)
        old_max = get_node_max_key(self.table.pager, old_node)
        new_page_num = get_unused_page_num(self.table.pager)
        new_node = self.table.pager.get_page(new_page_num)
        initialize_leaf_node(new_node)
        new_node[PARENT_POINTER_OFFSET:PARENT_POINTER_OFFSET+PARENT_POINTER_SIZE] = old_node[PARENT_POINTER_OFFSET:PARENT_POINTER_OFFSET+PARENT_POINTER_SIZE]
        old_next_page_num = get_leaf_node_next_leaf(old_node)
        new_node[LEAF_NODE_NEXT_LEAF_OFFSET:LEAF_NODE_NEXT_LEAF_OFFSET+LEAF_NODE_NEXT_LEAF_SIZE] = (old_next_page_num).to_bytes(LEAF_NODE_NEXT_LEAF_SIZE, 'little')
        old_node[LEAF_NODE_NEXT_LEAF_OFFSET:LEAF_NODE_NEXT_LEAF_OFFSET+LEAF_NODE_NEXT_LEAF_SIZE] = (new_page_num).to_bytes(LEAF_NODE_NEXT_LEAF_SIZE, 'little')

        for i in range(LEAF_NODE_MAX_CELLS, -1, -1):
            print(f'i: {i}')
            index_within_node = i % LEAF_NODE_LEFT_SPLIT_COUNT
            if i >= LEAF_NODE_LEFT_SPLIT_COUNT:
                dest_node = new_node
            else:
                dest_node = old_node
            if i == self.cell_num:
                b_key = bytearray(key.encode('utf-8'))
                dest_node[leaf_node_cell(index_within_node):leaf_node_cell(index_within_node)+len(b_key)] = b_key
                serialize_row(value, dest_node, leaf_node_value(index_within_node))
                
            elif i > self.cell_num:
                dest_node[leaf_node_cell(index_within_node):leaf_node_cell(index_within_node)+LEAF_NODE_CELL_SIZE] \
                    = old_node[leaf_node_cell(i-1):leaf_node_cell(i-1)+LEAF_NODE_CELL_SIZE]
            else:
                print(i, leaf_node_cell(i))
                dest_node[leaf_node_cell(index_within_node):leaf_node_cell(index_within_node)+LEAF_NODE_CELL_SIZE] \
                    = old_node[leaf_node_cell(i):leaf_node_cell(i)+LEAF_NODE_CELL_SIZE]
            
        old_node[LEAF_NODE_NUM_CELLS_OFFSET:LEAF_NODE_NUM_CELLS_OFFSET+LEAF_NODE_NUM_CELLS_SIZE] = LEAF_NODE_LEFT_SPLIT_COUNT.to_bytes(LEAF_NODE_NUM_CELLS_SIZE, 'little')
        new_node[LEAF_NODE_NUM_CELLS_OFFSET:LEAF_NODE_NUM_CELLS_OFFSET+LEAF_NODE_NUM_CELLS_SIZE] = LEAF_NODE_RIGHT_SPLIT_COUNT.to_bytes(LEAF_NODE_NUM_CELLS_SIZE, 'little')
        if old_node[IS_ROOT_OFFSET:IS_ROOT_OFFSET+IS_ROOT_SIZE] == (1).to_bytes(IS_ROOT_SIZE, 'little'):
            print('debug2')
            return create_new_root(self.table, new_page_num)
        else:
            parent_page_num = get_parent_page_num(old_node)
            new_max = get_node_max_key(self.table.pager, old_node)
            parent_node = self.table.pager.get_page(parent_page_num)
            update_internal_node_key(parent_node, old_max, new_max)
            internal_node_insert(self.table, parent_page_num, new_page_num)
