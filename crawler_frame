import logging
from datamodel.search.datamodel import ProducedLink, OneUnProcessedGroup, robot_manager
from spacetime_local.IApplication import IApplication
from spacetime_local.declarations import Producer, GetterSetter, Getter
from lxml import html, etree
import re, os
from time import time
from collections import defaultdict

try:
    # For python 2
    from urlparse import urlparse, parse_qs
except ImportError:
    # For python 3
    from urllib.parse import urlparse, parse_qs


logger = logging.getLogger(__name__)
LOG_HEADER = "[CRAWLER]"
url_count = (set() 
    if not os.path.exists("successful_urls.txt") else 
    set([line.strip() for line in open("successful_urls.txt").readlines() if line.strip() != ""])) #set if the file doesnt exist, otherwise read all non empty lines in the file and set them 
MAX_LINKS_TO_DOWNLOAD = 3000

# Various structures to collect analytics.
subdomains = defaultdict(set)
invalid_links_from_frontier = 0
out_links_by_page = dict()
file_types = defaultdict(int)
malformed_htmls = 0
stats_file_name = "stats.txt"

@Producer(ProducedLink)
@GetterSetter(OneUnProcessedGroup)
class CrawlerFrame(IApplication):

    def __init__(self, frame):
        self.starttime = time()
        # Set app_id <student_id1>_<student_id2>...
        self.app_id = "69605657_80116792"
        # Set user agent string to IR W17 UnderGrad <student_id1>, <student_id2> ...
        # If Graduate studetn, change the UnderGrad part to Grad.
        self.UserAgentString = "IR W17 UnderGrad testing 69605657, 80116792"
		
        self.frame = frame
        assert(self.UserAgentString != None) 
        assert(self.app_id != "")
        if len(url_count) >= MAX_LINKS_TO_DOWNLOAD:
            self.done = True

    def initialize(self):
        self.count = 0
        l = ProducedLink("http://www.ics.uci.edu", self.UserAgentString)
        print l.full_url
        self.frame.add(l)

    def update(self):
        for g in self.frame.get(OneUnProcessedGroup):
            print "Got a Group"
            outputLinks, urlResps = process_url_group(g, self.UserAgentString)
            for urlResp in urlResps:
                if urlResp.bad_url and self.UserAgentString not in set(urlResp.dataframe_obj.bad_url): #verify url is invalid and the user agent string is not in the set of bad_urls 
                    urlResp.dataframe_obj.bad_url += [self.UserAgentString]
            for l in outputLinks:
                if is_valid(l) and robot_manager.Allowed(l, self.UserAgentString): #if the outpult linke is valid and you have permission to crawl the website 
                    lObj = ProducedLink(l, self.UserAgentString)
                    self.frame.add(lObj)
        if len(url_count) >= MAX_LINKS_TO_DOWNLOAD:
            self.done = True

    def shutdown(self):
        runtime = time() - self.starttime

        # Output analytics.
        with open(stats_file_name, 'w') as stats_file:
            stats_file.write("Subdomains:\n")
            for subdomain in sorted(subdomains, key=lambda x: len(subdomains[x]), reverse=True):
                stats_file.write(subdomain + ' ' + str(len(subdomains[subdomain])) + '\n')

            stats_file.write('\nInvalid links from the frontier:\n' + str(invalid_links_from_frontier) + '\n')

            stats_file.write('\nMalformed or empty HTMLs encountered:\n' + str(malformed_htmls) + '\n')

            if len(out_links_by_page) != 0:
                stats_file.write('\nPage with the most out links (any):\n')
                sorted_pages = sorted(out_links_by_page, key=lambda x: len(out_links_by_page[x]), reverse=True)
                stats_file.write(sorted_pages[0])
                stats_file.write('\n(contains ' + str(len(out_links_by_page[sorted_pages[0]])) + ' links)\n')

                stats_file.write('\nPage with the most out links (only valid):\n')
                sorted_pages = sorted(out_links_by_page, key=lambda x: len(filter(is_valid, out_links_by_page[x])), reverse=True)
                stats_file.write(sorted_pages[0])
                stats_file.write('\n(contains ' + str(len(out_links_by_page[sorted_pages[0]])) + ' valid links)\n')
            
            stats_file.write('\nAverage download time: ' + str(float(runtime) / float(len(url_count))) + '\n')
            stats_file.write('\nFile types encountered in links:\n')
            for ext in sorted(file_types, key=lambda x: file_types[x], reverse=True):
                stats_file.write(ext + ' ' + str(file_types[ext]) + '\n')
            
        print "downloaded ", len(url_count), " in ", runtime, " seconds."
        pass

def save_count(urls): #append URLs to .txt file
    global url_count
    urls = set(urls).difference(url_count)
    url_count.update(urls)
    if len(urls):
        with open("successful_urls.txt", "a") as surls:
            surls.write(("\n".join(urls) + "\n").encode("utf-8"))

def process_url_group(group, useragentstr):
    rawDatas, successfull_urls = group.download(useragentstr, is_valid)
    save_count(successfull_urls)
    return extract_next_links(rawDatas), rawDatas
    
#######################################################################################
'''

'''
def extract_next_links(rawDatas):
    outputLinks = list()
    '''
    rawDatas is a list of objs -> [raw_content_obj1, raw_content_obj2, ....]
    Each obj is of type UrlResponse  declared at L28-42 datamodel/search/datamodel.py
    the return of this function should be a list of urls in their absolute form
    Validation of link via is_valid function is done later (see line 42).
    It is not required to remove duplicates that have already been downloaded. 
    The frontier takes care of that.

    Suggested library: lxml
    '''
    global subdomains
    global invalid_links_from_frontier 
    global out_links_by_page
    global file_types
    global malformed_htmls

    for raw_content_obj in rawDatas:
        if raw_content_obj.is_redirected:
            url = raw_content_obj.final_url
        else:
            url = raw_content_obj.url
        parsed_url = urlparse(url)

        # Check if the link is invalid.
        if not is_valid(url):
            invalid_links_from_frontier += 1
        # Check if the link returned Success code.
        elif raw_content_obj.http_code == 200:
            # Subdomains and different URLs from them for analytics.
            subdomains[parsed_url.netloc].add(url)

            try:
                # Parse content with lxml.
                parsed_html = etree.HTML(raw_content_obj.content)

                if parsed_html is not None:

                    # Get links.
                    links = parsed_html.xpath('//a/@href') + parsed_html.xpath('//A/@HREF')

                    # Out links for analytics.
                    out_links_by_page[url] = links

                    # Check for links with files for analytics.
                    for link in links:
                        suspect_file_type = file_type(link)
                        if suspect_file_type is not None:
                            file_types[suspect_file_type] += 1

                    # Add links for output.
                    outputLinks = outputLinks + filter(is_valid, links)

                else:
                    # If we are here, the content was empty.
                    malformed_htmls += 1

            except etree.XMLSyntaxError:
                # If we are here, lxml failed to parse the content.
                malformed_htmls += 1
        
    return outputLinks

def is_valid(url):
    '''
    Function returns True or False based on whether the url has to be downloaded or not.
    Robot rules and duplication rules are checked separately.

    This is a great place to filter out crawler traps.
    '''
    parsed = urlparse(url)
    if parsed.scheme not in set(["http", "https"]): #check if URL is absolute 
        return False
    try:
        return ".ics.uci.edu" in parsed.hostname \ #from unviersity server 
            and "calendar.php" not in parsed.path.lower() \ #check to see if it is not calendar page aka trap
            and not re.match(".*\.(css|js|bmp|gif|jpe?g|ico" + "|png|tiff?|mid|mp2|mp3|mp4"\
            + "|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf" \
            + "|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso|epub|dll|cnf|tgz|sha1" \
            + "|thmx|mso|arff|rtf|jar|csv"\
            + "|rm|smil|wmv|swf|wma|zip|rar|gz)$", parsed.path.lower()) #compile a string of any file type ending is any of these types 

    except TypeError:
        print ("TypeError for ", parsed)

def file_type(url):
    '''
    Given a URL, return file type if the path ends with a known extension.
    If not, return None.
    '''
    match = re.match(".*\.(css|js|bmp|gif|jpe?g|ico" + "|png|tiff?|mid|mp2|mp3|mp4"\
            + "|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf" \
            + "|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso|epub|dll|cnf|tgz|sha1" \
            + "|thmx|mso|arff|rtf|jar|csv"\
            + "|rm|smil|wmv|swf|wma|zip|rar|gz)$", urlparse(url).path.lower()) #if it matches return if not then no return and checks file type
    if match:
        return match.group(1)
    return None

#is_valid is that it determines if the URL is valid or not and here we filter out the crawler trap (that being calendar.php) 
#while file_type checks for the file type is ending in a specific extension and we keep track of how many times each of those types occur and add them to the stats.txt file
