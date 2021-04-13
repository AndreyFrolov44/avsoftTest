import time
from parserClass import Parser

url = 'https://stackoverflow.com/'
depth = 2
db_name = 'data.db'
max_workers = 10
allow_redirects = False



if __name__ == '__main__':
    start = time.time()
    site = Parser(url, max_page=depth, db_name=db_name, max_workers=max_workers, allow_redirects=allow_redirects)
    site.start()
    print(f'Время {time.time() - start}')
    print(f'Найдено ссылок: {len(site.visited_pages)}')
    site.csv('table')
    site.sql()
