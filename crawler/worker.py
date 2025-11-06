from threading import Thread
import time
from inspect import getsource
from utils.download import download
from utils import get_logger
import scraper

class Worker(Thread):
    def __init__(self, worker_id, config, frontier):
        self.logger = get_logger(f"Worker-{worker_id}", "Worker")
        self.config = config
        self.frontier = frontier
        self.worker_id = worker_id
        
        # Basic check for requests in scraper
        assert {getsource(scraper).find(req) for req in {"from requests import", "import requests"}} == {-1}, \
            "Do not use requests in scraper.py"
        assert {getsource(scraper).find(req) for req in {"from urllib.request import", "import urllib.request"}} == {-1}, \
            "Do not use urllib.request in scraper.py"
        
        super().__init__(daemon=True)
        
    def run(self):
        """
        Main worker loop that respects politeness and thread safety.
        """
        consecutive_none_count = 0
        max_consecutive_none = 10  # Stop after 10 consecutive failed attempts
        
        while True:
            tbd_url = self.frontier.get_tbd_url()
            
            if not tbd_url:
                # Check if frontier is truly empty or just in politeness cooldown
                if not self.frontier.has_pending_urls():
                    self.logger.info("Frontier is empty. Stopping Crawler.")
                    break
                else:
                    # URLs exist but all domains are in cooldown
                    consecutive_none_count += 1
                    if consecutive_none_count >= max_consecutive_none:
                        self.logger.info("All domains in cooldown for too long. Stopping.")
                        break
                    
                    # Sleep briefly and retry
                    time.sleep(0.1)
                    continue
            
            # Reset consecutive none count
            consecutive_none_count = 0
            
            # Download and process the URL
            resp = download(tbd_url, self.config, self.logger)
            self.logger.info(
                f"Downloaded {tbd_url}, status <{resp.status}>, "
                f"using cache {self.config.cache_server}.")
            
            # Scrape URLs from the response
            scraped_urls = scraper.scraper(tbd_url, resp)
            
            # Add scraped URLs to frontier
            for scraped_url in scraped_urls:
                self.frontier.add_url(scraped_url)
            
            # Mark URL as complete
            self.frontier.mark_url_complete(tbd_url)
            
            # Note: We don't sleep here because the frontier already handles
            # politeness delays in get_tbd_url()