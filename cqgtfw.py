#coding=utf-8
# 都他么为了买房


import requests
import bs4
import csv
import threading
import queue
import time

class CqGtFw(object):
    
    def __init__(self):
        self.init_url = "http://www.cqgtfw.gov.cn/scxx/tdgyjg/index.htm"
        self.serial_url = "http://www.cqgtfw.gov.cn/scxx/tdgyjg/index_%s.htm"
        self.crawled = queue.Queue()
        self.crawl_queue = queue.Queue()
        self.set_head_flag = True
        self.end = False
        self.start_crawl = False
        self._construct_urls()

    def _construct_urls(self):
        
        self.crawl_queue.put(self.init_url)
        for i in range(1, 100):
            self.crawl_queue.put(self.serial_url%i)

    def _crawl_url(self, url):
        
        req = requests.get(url)
        req.encoding = "utf-8"
        if req.status_code == 200:
            self._analyze_text(req.text)
        else:
            self.crawl_queue.put(url)

    def _analyze_text(self, content):

        soup = bs4.BeautifulSoup(content, "lxml")
        soup_div = soup.find("div", class_="zwbf")
        soup_all_tr = soup_div.find_all("tr")
        for tr in soup_all_tr:
            if self.set_head_flag and tr.th:
                all_th = tr.get_text("||||", strip=True)
                head = all_th.split("||||")
                self.set_head_flag = False
                self.set_csv_head(head)
            elif tr.th:
                continue
            else:
                all_text = tr.get_text("||||", strip=True)
                contents = all_text.split("||||")
                self.crawled.put(contents)
                self.start_crawl = True

    def set_csv_head(self, head):
        
        with open("cagtfw.csv", "a+", encoding='utf-8',newline='') as f:
            writer = csv.writer(f)
            writer.writerow(head)

    def add_csv_content(self, contents):
        
        with open("cagtfw.csv", "a+", encoding='utf-8',newline='') as f:
            writer = csv.writer(f)
            for item in contents:
                if len(item) < 2:
                    continue
                writer.writerow(item)

    def _run_spider(self):
        
        while True:
            
            if self.crawl_queue.empty():
                self.end = True
                break

            if threading.active_count() > 5:
                time.sleep(5)

            u = self.crawl_queue.get()
            t = threading.Thread(target=self._crawl_url, args=(u,))
            t.start()

            # time.sleep(1)

    def _run_writer(self):
        
        while not self.end or (not self.crawled.empty()): 
            
            contents = []
            while True:
                if self.crawled.empty():
                    break
                else:
                    contents.append(self.crawled.get())

            self.add_csv_content(contents)

            time.sleep(10)

    def run(self):
        
        t1 = threading.Thread(target=self._run_spider)
        t1.start()
        time.sleep(5)
        t2 = threading.Thread(target=self._run_writer)
        t2.start()


if __name__ == "__main__":

    cqft = CqGtFw()
    cqft.run()
