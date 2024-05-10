from bs4 import BeautifulSoup
import time
import requests
from random import randint
from html.parser import HTMLParser
import json
from urllib.parse import unquote

'''
Yahoo!SELECTORs:  "a", attrs = {"class" : "ac-algo fz-l ac-21th lh-24" 
Update attrs to -> attrs = {"class" : "d-ib fz-20 lh-26 td-hu tc va-bot mxw-100p"
'''  

USER_AGENT = {'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.100 Safari/537.36'}
#USER_AGENT = {'User-Agent':'Mozilla/5.0 (Macintosh; Mac OS ; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.100 Safari/537.36'}
SEARCH_SELECTOR = ["a", {"class" : "d-ib fz-20 lh-26 td-hu tc va-bot mxw-100p"}]  
  
SEARCHING_URL = 'http://www.search.yahoo.com/search?p='  # for Yahoo!

class SearchEngine:
    @staticmethod
    def search(query, sleep=True):
        if sleep: # Prevents loading too many pages too soon
            time.sleep(randint(8, 25))
            #time.sleep(randint(10, 100))
        temp_url = '+'.join(query.split()) #for adding + between words for the query
        url = SEARCHING_URL + "<" + temp_url + ">"
        new_results = []
        for page_num in range(1, 4):
            url += '&b=' + str(page_num)
            soup = BeautifulSoup(requests.get(url, headers=USER_AGENT).text, "html.parser")
            out_num = len(new_results)
            new_results += SearchEngine.scrape_search_result(soup, out_num)

        return new_results
    
    @staticmethod
    def scrape_search_result(soup, out_num):
        raw_results = soup.find_all(SEARCH_SELECTOR[0], attrs = SEARCH_SELECTOR[1])
        results = []
        #implement a check to get only 10 results and also check that URLs must not be duplicated
        if not raw_results:
            print('NO RES!')
            return
        for result in raw_results:
            link = result.get('href')
            pos_start, pos_end = link.find("RU=") + 3, link.find("/RK")
            real_link = unquote(link[pos_start : pos_end])
            if real_link not in results:
                results.append(real_link)
            if len(results) + out_num >= 9:
                break            
        return results

''' Performance Comparing Processing '''
def find_overlap(query, yahoo_out, google_out):
    # list of pairs of rank of each overlapped results
    overlapped_results = []
    for yahoo_rank, yahoo_link in enumerate(yahoo_out[query]):
        for google_rank, goole_link in enumerate(google_out[query]):
            if link_compare(yahoo_link, goole_link):
                overlapped_results.append((google_rank, yahoo_rank))
    return overlapped_results

def link_compare(link1, link2):
    return normalize(link1) == normalize(link2)

def normalize(link):
    if link.startswith('https://'):
        normalized_link = link[8:]
    elif link.startswith('http://'):
        normalized_link = link[7:]
    if link.startswith('www.'):
        normalized_link = link[4:]
    if link[-1] == '/':
        normalized_link = link[:-1]
    return normalized_link

def sperman_coefficient(overlapped_results):
    overlapped_num = len(overlapped_results)
    if overlapped_num <= 0:
        return 0
    elif overlapped_num == 1:
        return 1 if overlapped_results[0][0] == overlapped_results[0][1] else 0
    else:
        rank_diff_sqr_sum = sum([(google_rank - yahoo_rank) ** 2 for google_rank, yahoo_rank in overlapped_results])
        rank_coefficient = 1 - ((6 * rank_diff_sqr_sum) / (overlapped_num * (overlapped_num ** 2 - 1)))
        return rank_coefficient

def main_function(queries, google_out):
    yahoo_out, statistics = dict(), dict()
    avg_overlap_num, avg_percent_overlap, avg_coefficient = 0, 0, 0
    queries_num = len(queries)
    #for query in queries:
    for i, query in enumerate(queries):
        yahoo_out[query] = SearchEngine.search(query)
        print('search #', i, 'Finished')
        overlapped_results = find_overlap(query, yahoo_out, google_out)
        coefficient = sperman_coefficient(overlapped_results)

        overlap_num, percent_overlap = len(overlapped_results), len(overlapped_results) / 10.0
        statistics[query] = {
            'Num_of_Overlap' : overlap_num,
            'Percent_Overlap' : percent_overlap,
            'Sperman_Coefficient' : coefficient
            } 
        avg_overlap_num += overlap_num
        avg_percent_overlap += percent_overlap
        avg_coefficient += coefficient

    statistics['Averages'] = {
        'Num_of_Overlap' : avg_overlap_num / queries_num,
        'Percent_Overlap' : avg_percent_overlap / queries_num,
        'Sperman_Coefficient' : avg_coefficient / queries_num
        } 
    '''
    statistics['Averages'] = {
        'Num_of_Overlap' : avg_overlap_num / 100,
        'Percent_Overlap' : avg_percent_overlap / 100,
        'Sperman_Coefficient' : avg_coefficient / 100
        } 
    '''
    return statistics, yahoo_out


''' # File Processing '''
def read_files(query_file, baseline_file):
    with open(query_file, "r") as f1:
        queries = [line.rstrip() for line in f1]
    with open(baseline_file, "r") as f2:
        baselines = json.load(f2)
    return queries, baselines

def write_files(search_result, result_json_file, statics, statics_file):
    with open(result_json_file, "w") as f1:
        f1.write(json.dumps(search_result, indent=2))
        
    with open(statics_file, "w") as f:
        f.write("Queries, Number of Overlapping Results, Percent Overlap, Spearman Coefficient\n")
        for query_idx, val in enumerate(list(statics.values())[:-1]):
            f.write(f"Query {query_idx + 1}, {val['Num_of_Overlap']}, {val['Percent_Overlap'] * 100.0}, {val['Sperman_Coefficient']}\n")
        # processing for the last 'average' line
        query_idx, val = list(statics.items())[-1]
        f.write(f"{query_idx}, {val['Num_of_Overlap']}, {val['Percent_Overlap'] * 100.0}, {val['Sperman_Coefficient']}\n")
        

if __name__ == "__main__":
    query_file = './100QueriesSet2.txt'
    baseline_file = './Google_Result2.json'

    search_queries, google_baselines = read_files(query_file, baseline_file)
    statics, yahoo_outs = main_function(search_queries, google_baselines)
    
    result_out_json_file = './QueriesSet2_out_task1.json'
    statics_out_file = './QueriesSet2_out_task2.csv'
    write_files(yahoo_outs, result_out_json_file, statics, statics_out_file)
    