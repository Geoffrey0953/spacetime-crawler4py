import os
import shelve
import time
from threading import Thread, RLock, Lock
from queue import Queue, Empty
from urllib.parse import urlparse
from collections import defaultdict

from utils import get_logger, get_urlhash, normalize
from scraper import is_valid

class Frontier(object):
    def __init__(self, config, restart):
        self.logger = get_logger("FRONTIER")
        self.config = config
        self.to_be_downloaded = list()
        
        # Thread safety locks
        self.lock = RLock()  # Main lock for frontier operations
        self.domain_locks = defaultdict(Lock)  # Per-domain locks
        self.domain_last_access = {}  # Track last access time per domain
        
        if not os.path.exists(self.config.save_file) and not restart:
            # Save file does not exist, but request to load save.
            self.logger.info(
                f"Did not find save file {self.config.save_file}, "
                f"starting from seed.")
        elif os.path.exists(self.config.save_file) and restart:
            # Save file does exists, but request to start from seed.
            self.logger.info(
                f"Found save file {self.config.save_file}, deleting it.")
            os.remove(self.config.save_file)
        # Load existing save file, or create one if it does not exist.
        self.save = shelve.open(self.config.save_file)
        if restart:
            for url in self.config.seed_urls:
                self.add_url(url)
        else:
            # Set the frontier state with contents of save file.
            self._parse_save_file()
            if not self.save:
                for url in self.config.seed_urls:
                    self.add_url(url)

    def _parse_save_file(self):
        """This function can be overridden for alternate saving techniques."""
        total_count = len(self.save)
        tbd_count = 0
        for url, completed in self.save.values():
            if not completed and is_valid(url):
                self.to_be_downloaded.append(url)
                tbd_count += 1
        self.logger.info(
            f"Found {tbd_count} urls to be downloaded from {total_count} "
            f"total urls discovered.")

    def _get_domain(self, url):
        """Extract domain from URL."""
        try:
            parsed = urlparse(url)
            return parsed.netloc
        except:
            return None

    def get_tbd_url(self):
        """
        Get a URL that's ready to be downloaded, respecting politeness delay.
        Returns None if no URLs are available or all domains are in cooldown.
        """
        with self.lock:
            if not self.to_be_downloaded:
                return None
            
            current_time = time.time()
            politeness_delay = self.config.time_delay
            
            # Try to find a URL whose domain is ready
            for i in range(len(self.to_be_downloaded)):
                url = self.to_be_downloaded[i]
                domain = self._get_domain(url)
                
                if domain is None:
                    continue
                
                # Check if enough time has passed since last access to this domain
                last_access = self.domain_last_access.get(domain, 0)
                time_since_last = current_time - last_access
                
                if time_since_last >= politeness_delay:
                    # This URL is ready to be downloaded
                    self.to_be_downloaded.pop(i)
                    self.domain_last_access[domain] = current_time
                    return url
            
            # No URLs available that respect politeness
            return None

    def add_url(self, url):
        """Add a URL to the frontier in a thread-safe manner."""
        url = normalize(url)
        urlhash = get_urlhash(url)
        
        with self.lock:
            if urlhash not in self.save:
                self.save[urlhash] = (url, False)
                self.save.sync()
                self.to_be_downloaded.append(url)
    
    def mark_url_complete(self, url):
        """Mark a URL as complete in a thread-safe manner."""
        urlhash = get_urlhash(url)
        
        with self.lock:
            if urlhash not in self.save:
                # This should not happen.
                self.logger.error(
                    f"Completed url {url}, but have not seen it before.")
            
            self.save[urlhash] = (url, True)
            self.save.sync()
    
    def has_pending_urls(self):
        """Check if there are any pending URLs."""
        with self.lock:
            return len(self.to_be_downloaded) > 0