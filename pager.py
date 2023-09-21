from common import *

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