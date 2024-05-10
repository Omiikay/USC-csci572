import scrapy
from scrapy.spiders import CrawlSpider, Rule
from scrapy.linkextractors import LinkExtractor
from scrapy.exceptions import CloseSpider
from collections import deque, defaultdict, Counter
import re
import pandas as pd

class FOXCrawler(CrawlSpider):
    name = 'foxSpider'
    allowed_domains = ['www.foxnews.com'] 
    start_urls = ['https://www.foxnews.com/']
    handle_httpstatus_list = [
        200, 201, 202, 203, 204, 205, 206, 300, 301, 302, 303, 304, 305, 307, 308,
        400, 401, 402, 403, 404, 405, 406, 407, 408, 409, 410, 411, 412, 413, 414, 
        415, 416, 417, 418, 421, 422, 423, 424, 425, 426, 428, 429, 431, 451,
        500, 501, 502, 503, 504, 505, 506, 507, 508, 510, 511
    ]
    custom_settings = {
        "DEPTH_LIMIT": 16,
        "CONCURRENT_REQUESTS": 32,
        "REDIRECT_ENABLED": False
    }
    rules = (
        Rule(LinkExtractor(), callback='parseItem', follow=True),
    )

    # Output Files
    fetchCSV = './fetch_foxnews.csv'
    visitCSV = './visit_foxnews.csv'
    urlsCSV = './urls_foxnews.csv'

    fetchURL = {"Link": [], "Status": []}
    visitURL = {"Link":[],"Size (KB)":[],'# of Outlinks':[], 'Content Type':[]}
    discoverURL = {"Link": [], "IN/OUT": []}

    news_site = 'foxnews'
    tolFetched, maxFetchPage = 0, 20001
    to_visit, visited = deque([]), defaultdict(int)
    fetch_succeed, fetch_unsucceed = 0, 0

    linksFilter = re.compile(r'^https?://')
    aTagFilter = re.compile(r'^(https?://(www\.)?foxnews\.com|http://(www\.)?foxnews\.com)')
    rootFilter = re.compile(r'^/')

    statusTable = {
        200: "OK", 201: "Created", 202: "Accepted", 203: "Non-Authoritative Information", 204: "No Content", 
        205: "Reset Content", 206: "Partial Content",
        300: "Multiple Choices", 301: "Moved Permanently", 302: "Found", 303: "See Other", 304: "Not Modified", 
        305: "Use Proxy", 307: "Temporary Redirect", 308: "Permanent Redirect",
        400: "Bad Request", 401: "Unauthorized", 402: "Payment Required", 403: "Forbidden", 404: "Not Found",
        405: "Method Not Allowed", 406: "Not Acceptable", 407: "Proxy Authentication Required", 408: "Request Timeout",
        409: "Conflict", 410: "Gone", 411: "Length Required", 412: "Precondition Failed", 413: "Payload Too Large",
        414: "URI Too Long", 415: "Unsupported Media Type", 416: "Range Not Satisfiable", 417: "Expectation Failed",
        418: "I'm a teapot", 421: "Misdirected Request", 422: "Unprocessable Entity", 423: "Locked", 424: "Failed Dependency",
        425: "Too Early", 426: "Upgrade Required", 428: "Precondition Required", 429: "Too Many Requests", 431: "Request Header Fields Too Large",
        451: "Unavailable For Legal Reasons", 
        500: "Internal Server Error", 501: "Not Implemented", 502: "Bad Gateway", 503: "Service Unavailable", 504: "Gateway Timeout", 
        505: "HTTP Version Not Supported", 506: "Variant Also Negotiates", 507: "Insufficient Storage", 508: "Loop Detected", 
        510: "Not Extended", 511: "Network Authentication Required"
    }
            
    def contentType(self, header):
        types = {'text', 'html', 'image', 'pdf'} # Should include all docs 
        set_header = set(re.split(r';|/', header))
        return header if (types & set_header) else "NA"

    def parseItem(self, response):
        cur_url, cur_status = response.request.url, response.status
        cur_type = response.headers.get(b'Content-Type', b' ').decode('utf-8').split(';')[0]
        # css checking
        css_slected_a, css_slected_link = response.css('a'), response.css('link')
        css_slected =  css_slected_a + css_slected_link
        if self.visited[cur_url] < 1:
            self.visited[cur_url] += 1
            self.tolFetched += 1
            if self.tolFetched >= self.maxFetchPage:
                raise CloseSpider("Reached 20,000 pages limit")
            self.fetchURL["Link"].append(cur_url)
            self.fetchURL["Status"].append(cur_status)
            if cur_status == 200:
                self.fetch_succeed += 1
            else:
                self.fetch_unsucceed += 1
            
            if 'image' not in cur_type:
                for url in css_slected:
                    if 'href' in url.get():
                        link = url.attrib['href']
                        if(self.aTagFilter.match(link)):
                            self.to_visit.append(link)
                            self.discoverURL["Link"].append(link)
                            self.discoverURL["IN/OUT"].append('OK')
                        elif(self.rootFilter.match(link)):
                                link_new = 'https://www.foxnews.com/' + link
                                self.to_visit.append(link_new)
                                self.discoverURL["Link"].append(link_new)
                                self.discoverURL["IN/OUT"].append('OK')
                        elif (self.linksFilter.match(link)):
                            self.discoverURL["Link"].append(link)
                            self.discoverURL["IN/OUT"].append('N_OK')
         
            if cur_status == 200:
                content_type = self.contentType(cur_type)
                outlinks = 0
                # all links except images are taken as outgoing sites
                if 'image' not in content_type:
                    # Total Outlinks
                    outlinks = len(css_slected)
                if content_type != "NA":
                    self.visitURL["Link"].append(cur_url)
                    self.visitURL["Size (KB)"].append(len(response.body) / 1024)
                    self.visitURL['# of Outlinks'].append(outlinks)
                    self.visitURL['Content Type'].append(content_type)
        while self.to_visit:
            cur_link = self.to_visit.popleft()
            yield response.follow(cur_link, callback= self.parseItem)

    def closed(self, reason):
        task1 = pd.DataFrame(self.fetchURL) 
        task2 = pd.DataFrame(self.visitURL) 
        task3 = pd.DataFrame(self.discoverURL) 

        task1.to_csv(self.fetchCSV, index=False)
        task2.to_csv(self.visitCSV, index=False)
        task3.to_csv(self.urlsCSV, index=False)

        uniqueLinks = task3.drop_duplicates("Link")
        inSiteNum = len(uniqueLinks[uniqueLinks["IN/OUT"] == "OK"])
        outSiteNum = len(uniqueLinks[uniqueLinks["IN/OUT"] == "N_OK"])

        status_codes = ""
        for code, num in Counter(self.fetchURL["Status"]).items():
            status_codes += f'{code} {self.statusTable[int(code)]}: {num}\n'

        with open('CrawlReport_foxnews.txt', 'w') as f:
            f.write("Name: Zouyan Song\n")
            f.write(f'USC ID: 1171340446\n')
            f.write(f'News site crawled: foxnews.com\n')
            f.write(f'Number of threads: 32 (CONCURRENT_REQUESTS)\n') 
            f.write(f'\nFetch Statistics\n')
            f.write(f'================\n')
            f.write(f'# fetches attempted: {len(self.fetchURL["Link"])}\n')
            f.write(f'# fetches succeeded: {self.fetch_succeed}\n')
            f.write(f'# fetches failed or aborted: {self.fetch_unsucceed}\n')
            
            f.write(f'\nOutgoing URLs:\n')
            f.write(f'==============\n')
            f.write(f'Total URLs extracted: \n')
            f.write(f'# unique URLs extracted: {len(uniqueLinks)}\n')
            f.write(f'# unique URLs within News Site: {inSiteNum}\n')
            f.write(f'# unique URLs outside News Site: {outSiteNum}\n')
            f.write(f'\nStatus Codes:\n')
            f.write(f'=============\n')
            f.write(f'{status_codes}\n')
            f.write(f'\nFile Sizes:\n')
            f.write(f'===========\n')
            f.write(f'< 1KB: {len(task2.loc[task2["Size (KB)"] < 1])}\n')
            f.write(f' 1KB ~ <10KB: {task2[(task2["Size (KB)"] >= 1) & (task2["Size (KB)"] < 10)].shape[0]}\n')
            f.write(f'10KB ~ <100KB: {task2[(task2["Size (KB)"] >= 10) & (task2["Size (KB)"] < 100)].shape[0]}\n')
            f.write(f'100KB ~ <1MB: {task2[(task2["Size (KB)"] >= 100) & (task2["Size (KB)"] < 1024)].shape[0]}\n')
            f.write(f'>= 1MB: {task2[task2["Size (KB)"] >= 1024].shape[0]}\n')
            f.write(f'\nContent Types:\n')
            f.write(f'===============\n')
            for type, num in Counter(self.visitURL['Content Type']).items():
                f.write(f'{type}: {num}\n')

'''
Run cmd        
py -m scrapy runspider ./Downloads/hw2/pytest.py
'''