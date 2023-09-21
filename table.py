from cursor import Cursor, leaf_node_find
from pager import Pager
from common import *

class Table():
    def __init__(self, pager:Pager) -> None:
        self.root_page_num = 0
        self.pager = pager

    def table_start(self):
        cursor = Cursor(self)
        cursor.page_num = self.root_page_num
        root_node = self.pager.get_page(self.root_page_num)
        num_cells = int.from_bytes(root_node[LEAF_NODE_NUM_CELLS_OFFSET:LEAF_NODE_CELL_SIZE], byteorder='little')
        cursor.end_of_table = (num_cells == 0)

        return cursor

    def find_key(self, key):
        node = self.pager.get_page(self.root_page_num)
        if get_node_type(node) == NodeType.NODE_LEAF.value:
            return leaf_node_find(self, self.root_page_num, key)
        else:
            print('Need to implement searching an internal node')


class Row():
    def __init__(self) -> None:
        self.id = ''
        self.username = ''
    
    def __str__(self) -> str:
        return f'<Row {self.id} {self.username}>'