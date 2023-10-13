from cursor import Cursor, leaf_node_find, internal_node_find
from pager import Pager
from common import *

class Table():
    def __init__(self, pager:Pager) -> None:
        self.root_page_num = 0
        self.pager = pager

    def table_start(self):
        cursor = self.find_key('0')
        node = self.pager.get_page(cursor.page_num)
        num_cells = get_leaf_num_cells(node)
        cursor.end_of_table = (num_cells == 0)
        return cursor

    def find_key(self, key):
        node = self.pager.get_page(self.root_page_num)
        if get_node_type(node) == NodeType.NODE_LEAF.value:
            return leaf_node_find(self, self.root_page_num, key)
        else:
            return internal_node_find(self, self.root_page_num, key)


class Row():
    def __init__(self) -> None:
        self.id = ''
        self.username = ''
    
    def __str__(self) -> str:
        return f'<Row {self.id} {self.username}>'