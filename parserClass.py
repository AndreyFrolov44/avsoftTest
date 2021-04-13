import requests
import re
import csv
import sqlite3
from bs4 import BeautifulSoup
import concurrent.futures


class Page:
    def __init__(self, url, depth=0):
        self.url = url
        self.depth = depth
        self.sublink = []
        self.subpage = []

    def __repr__(self):
        return self.url + ' ' + str(self.depth)

    def __str__(self):
        return self.url


class Parser:
    def __init__(self, url, max_page, db_name, max_workers=1, allow_redirects=False):
        self.url = url
        self.max_page = max_page
        self.links_depth = []
        self.links = []
        self.pages = []
        self.visited_links = []
        self.visited_pages = []
        self.max_workers = max_workers
        self.db_name = self.get_domain(self.url)
        self.db = sqlite3.connect(f'{db_name}')
        self.cursor = self.db.cursor()
        self.allow_redirects = allow_redirects

    def _get_html(self, url):
        try:
            r = requests.get(url, allow_redirects=self.allow_redirects, timeout=5)
            return r.text
        except requests.exceptions.ReadTimeout:
            return

    def _get_all_links(self, url, depth):
        html = self._get_html(url)
        if not html:
            return
        soup = BeautifulSoup(html, 'lxml')
        for link in soup.findAll('a', href=re.compile(f"(^{self.url[:-1]})|(^/)")):
            l = link['href']
            if l.find(self.url) == 0:
                l = l[len(self.url) - 1:]
            if l not in self.visited_links and l not in self.links:
                self.links.append(l)
                yield (l, depth)

    def get_domain(self, link):
        site = re.match('http[s]*://.+?/', link)
        site = site.group(0)
        return site

    def subpages(self, page):
        # print(page)
        if page.depth >= self.max_page or page.url in self.visited_links:
            self.pages.pop(self.pages.index(page))
            self.visited_pages.append(page)
            return
        page.sublink = self._get_all_links(f'{self.url}{page.url[1:]}', page.depth + 1)
        for p in page.sublink:
            depth = p[1]
            if depth != self.max_page:
                self.pages.append(Page(p[0], depth))
            # self.visited_links.append(p[0])
        self.pages.pop(self.pages.index(page))
        self.visited_pages.append(page)
        self.vesited_links.append(page.url)

    def start(self):
        self.links_depth = self._get_all_links(self.url, 0)
        for link in self.links_depth:
            self.pages.append(Page(link[0], link[1]))
        while self.pages:
            print(len(self.pages))
            with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                executor.map(self.subpages, self.pages)

    def csv(self, name):
        with open(f'{name}.csv', 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f, delimiter=',')
            writer.writerow(['url', 'depth'])
            for page in self.visited_pages:
                writer.writerow([page.url, page.depth])

    def sql(self):
        self.cursor.execute(f"""CREATE TABLE IF NOT EXISTS '{self.db_name}' (
            link TEXT,
            depth INT
        )""")
        self.db.commit()
        for link in self.visited_pages:
            self.cursor.execute(f"SELECT link FROM '{self.db_name}' WHERE link = ?", (self.url + link.url[1:],))
            if self.cursor.fetchall():
                continue
            self.cursor.execute(f"INSERT INTO '{self.db_name}' VALUES (?, ?)", (self.url + link.url[1:], link.depth))
            self.db.commit()
