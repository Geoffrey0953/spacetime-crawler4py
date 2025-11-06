import hashlib
import re
import time
from bs4 import BeautifulSoup
from collections import Counter, defaultdict
from urllib.parse import urlparse, urljoin, urlsplit, urldefrag

stop_words = {
    "a", "about", "above", "after", "again", "against", "all", "am", "an", "and",
    "any", "are", "aren't", "as", "at", "be", "because", "been", "before", "being",
    "below", "between", "both", "but", "by", "can't", "cannot", "could", "couldn't",
    "did", "didn't", "do", "does", "doesn't", "doing", "don't", "down", "during",
    "each", "few", "for", "from", "further", "had", "hadn't", "has", "hasn't", "have",
    "haven't", "having", "he", "he'd", "he'll", "he's", "her", "here", "here's", "hers",
    "herself", "him", "himself", "his", "how", "how's", "i", "i'd", "i'll", "i'm",
    "i've", "if", "in", "into", "is", "isn't", "it", "it's", "its", "itself", "let's",
    "me", "more", "most", "mustn't", "my", "myself", "no", "nor", "not", "of", "off",
    "on", "once", "only", "or", "other", "ought", "our", "ours", "ourselves", "out",
    "over", "own", "same", "shan't", "she", "she'd", "she'll", "she's", "should",
    "shouldn't", "so", "some", "such", "than", "that", "that's", "the", "their",
    "theirs", "them", "themselves", "then", "there", "there's", "these", "they",
    "they'd", "they'll", "they're", "they've", "this", "those", "through", "to", "too",
    "under", "until", "up", "very", "was", "wasn't", "we", "we'd", "we'll", "we're",
    "we've", "were", "weren't", "what", "what's", "when", "when's", "where", "where's",
    "which", "while", "who", "who's", "whom", "why", "why's", "with", "won't", "would",
    "wouldn't", "you", "you'd", "you'll", "you're", "you've", "your", "yours",
    "yourself", "yourselves"
}

duplicate_hashes = set()
page_word_count = {}
subdomain_unique_pages = defaultdict(int)
total_word_count = Counter()
unique_pages = set()
visited = defaultdict(int)

def count_words(resp):
    try:
        soup = BeautifulSoup(resp.raw_response.content, "html.parser")
        for script in soup(["script", "style"]):
            script.decompose()

        text = " ".join(soup.stripped_strings)
        words = [
            w.lower()
            for w in re.findall(r"\b\w+\b", text)
            if w.lower() not in stop_words and len(w) > 1
        ]
        page_word_count[resp.url] = len(words)
        total_word_count.update(words)
    except Exception as e:
        print(f"Error counting words on {resp.url}: {str(e)}")

def scraper(url, resp):
    if resp.status != 200 or resp.raw_response is None:
        return []
    
    # Detect and avoid sets of similar pages with no information
    hash = hashlib.md5(resp.raw_response.content).hexdigest()
    if hash in duplicate_hashes:
        return []
    duplicate_hashes.add(hash)

    # How many unique pages did you find?
    parsed = urlparse(url)
    base_url = urlsplit(url)._replace(fragment='', query='').geturl()  # remove fragment & query
    unique_pages.add(base_url)

    # the number of unique pages detected in each subdomain
    if 'uci.edu' in parsed.netloc:
        subdomain_unique_pages[parsed.netloc] += 1

    count_words(resp)

    links = extract_next_links(url, resp)
    time.sleep(0.5)
    return [link for link in links if is_valid(link)]

def extract_next_links(url, resp):
    # Implementation required.
    # url: the URL that was used to get the page
    # resp.url: the actual url of the page
    # resp.status: the status code returned by the server. 200 is OK, you got the page. Other numbers mean that there was some kind of problem.
    # resp.error: when status is not 200, you can check the error here, if needed.
    # resp.raw_response: this is where the page actually is. More specifically, the raw_response has two parts:
    #         resp.raw_response.url: the url, again
    #         resp.raw_response.content: the content of the page!
    # Return a list with the hyperlinks (as strings) scrapped from resp.raw_response.content
    resp_links = []

    if resp.status != 200 or resp.raw_response is None:
        return resp_links
    
    content = resp.raw_response.content
    if not content or len(content) == 0:
        return resp_links

    soup = BeautifulSoup(content, "html.parser")
    for tag in soup.find_all("a", href=True):
        link = urljoin(url, tag["href"])
        defragged, _ = urldefrag(link)  # remove fragments?
        resp_links.append(defragged)

    return resp_links

def is_valid(url):
    # Decide whether to crawl this url or not. 
    # If you decide to crawl it, return True; otherwise return False.
    # There are already some conditions that return False.
    try:
        parsed = urlparse(url)
        if parsed.scheme not in {"http", "https"}:
            return False
        
        # Detect and avoid infinite traps
        path_depth = parsed.path.split('/')
        if len(path_depth) > 8:
            return False
        
        # pattern based checks
        for p in [
            re.compile(p) for p in [
                r"wics\.ics\.uci\.edu/events/20",
                r"\?share=(facebook|twitter)",
                r"\?action=login",
                r"action=diff&version=",
                r"timeline\?from",
                r"\?version=(?!1$)",
                r"/calendar/",
                r"/archive/",
                r"/ml/datasets.php",
                r"/print/",
                r"/rss/",
                r"/feed/",
                r"/tags/",
                r"/404",
                r"/auth", 
                r"/~eppstein/pix/", 
                r"/~eppstein/pubs",
                r"/category/page/\d+"
            ]
        ]:
            if p.search(url):
                return False
        
        path_pattern = re.sub(r'\d+', 'N', parsed.path)
        visited[path_pattern] += 1
        if visited[path_pattern] > 30:
            return False
        
        if not any(p.match(url) for p in [
            re.compile(r".*\.ics\.uci\.edu/.*"), 
            re.compile(r".*\.cs\.uci\.edu/.*"), 
            re.compile(r".*\.informatics\.uci\.edu/.*"), 
            re.compile(r".*\.stat\.uci\.edu/.*"), 
            re.compile(r".*today\.uci\.edu/department/information_computer_sciences/.*")
        ]):
            return False
        
        # parsing based checks
        if not is_valid_helper(url):
            return False

        return not re.match(
            r".*\.(css|js|bmp|gif|jpe?g|ico"
            + r"|png|tiff?|mid|mp2|mp3|mp4"
            + r"|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf"
            + r"|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names"
            + r"|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso"
            + r"|epub|dll|cnf|tgz|sha1"
            + r"|thmx|mso|arff|rtf|jar|csv"
            + r"|rm|smil|wmv|swf|wma|zip|rar|gz|mpg|img|war|apk|py|ppsx|pps)$", parsed.path.lower())

    except TypeError:
        print ("TypeError for ", parsed)
        raise

def is_valid_helper(url):
    invalid_suffixes = (
        "?share=facebook", "?share=twitter", "?action=login",
        ".zip", ".pdf", ".txt", ".tar.gz", ".bib", ".htm", ".xml",
        ".bam", ".java"
    )

    invalid_prefixes = ("http://www.ics.uci.edu/~eppstein/pix/",)

    if any(url.endswith(suffix) for suffix in invalid_suffixes):
        return False
    if any(url.startswith(prefix) for prefix in invalid_prefixes):
        return False

    if url.startswith("https://wics.ics.uci.edu/events/"):
        return False
    if "wics" in url:
        if "/?afg" in url and not url.endswith("page_id=1"):
            return False
        if "/img_" in url:
            return False
    if "doku.php" in url:
        return False
    if "sli.ics.uci.edu/Classes" in url:
        return False
    if "grape.ics.uci.edu" in url:
        if any(
            pattern in url for pattern in [
                "action=diff&version=", "timeline?from"
            ]
        ) or ("?version=" in url and not url.endswith("?version=1")):
            return False

    return True

def create_report():
    with open("report.txt", 'w', encoding='utf-8') as f:
        f.write("unique pages:\n")
        f.write(f"{len(unique_pages)}\n\n")

        if page_word_count:
            longest_url = max(page_word_count, key=page_word_count.get)
            word_count = page_word_count[longest_url]
        else:
            longest_url, word_count = None, 0
            
        f.write("longest page:\n")
        f.write(f"URL: {longest_url}\n")
        f.write(f"word count: {word_count}\n\n")

        f.write("50 most common words:\n")
        for word, count in total_word_count.most_common(50):
            f.write(f"{word}: {count}\n")
        f.write("\n")

        f.write("subdomains:\n")
        for subdomain, count in sorted(subdomain_unique_pages.items()):
            f.write(f"{subdomain}, {count}\n")

        f.write("\nEnd of report\n")
