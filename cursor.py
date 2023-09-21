from common import *

def leaf_node_find(table, page_num, key):
    node = table.pager.get_page(page_num)
    cell_nums = int.from_bytes(node[LEAF_NODE_NUM_CELLS_OFFSET:LEAF_NODE_NUM_CELLS_OFFSET+LEAF_NODE_NUM_CELLS_SIZE], byteorder='little')
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
        num_cells = int.from_bytes(page[LEAF_NODE_NUM_CELLS_OFFSET:LEAF_NODE_NUM_CELLS_OFFSET+LEAF_NODE_NUM_CELLS_SIZE], byteorder='little')
        if self.cell_num >= num_cells:
            self.end_of_table = True

    def leaf_node_insert(self, key, value):
        node = self.table.pager.get_page(self.page_num)
        num_cells = int.from_bytes(node[LEAF_NODE_NUM_CELLS_OFFSET:LEAF_NODE_NUM_CELLS_OFFSET+LEAF_NODE_NUM_CELLS_SIZE], byteorder='little')
        if num_cells >= LEAF_NODE_MAX_CELLS:
            raise Exception(f'Too many cells found {num_cells}')
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
