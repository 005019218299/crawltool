import os
import requests
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup
import threading
import queue
import time
import MySQLdb
import re
import random
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from collections import deque
import xml.etree.ElementTree as ET

def log(msg, level="INFO"):
    colors = {"ERROR": "\033[91m", "WARN": "\033[93m", "INFO": "\033[0m", "DEBUG": "\033[96m"}
    print(f"{colors.get(level, '')}[{level}] {msg}\033[0m")

class SmartBlogDetector:
    def __init__(self):
        self.blog_indicators = [
            # URL patterns
            re.compile(r'/(blog|post|article|news|story|entry)/[^/]+/?$', re.I),
            re.compile(r'/\d{4}/\d{2}/\d{2}/[^/]+/?$'),  # Date-based URLs
            re.compile(r'/\d{4}/\d{2}/[^/]+/?$'),
            re.compile(r'/[^/]+-\d+\.html?$', re.I),
            re.compile(r'/p/[^/]+/?$', re.I),  # Medium style
            re.compile(r'/[^/]+/[^/]+-[a-f0-9]{8,}/?$', re.I)  # Hash-based
        ]
        
        self.content_keywords = [
            'published', 'author', 'posted', 'written by', 'by:', 'tags:', 'category:',
            'read more', 'continue reading', 'share this', 'comments', 'reply',
            'related posts', 'similar articles', 'next post', 'previous post'
        ]
        
        self.meta_indicators = [
            'article:published_time', 'article:author', 'article:tag',
            'og:type', 'twitter:card'
        ]
    
    def is_blog_article(self, url, soup, content_text):
        score = 0
        
        # URL pattern check (40 points)
        if any(pattern.search(urlparse(url).path) for pattern in self.blog_indicators):
            score += 40
        
        # Content length (20 points)
        if len(content_text) >= 300:
            score += 20
        
        # Meta tags (20 points)
        for meta in self.meta_indicators:
            if soup.find('meta', property=meta) or soup.find('meta', name=meta):
                score += 5
                if score >= 20: break
        
        # Content keywords (15 points)
        text_lower = content_text.lower()
        keyword_count = sum(1 for keyword in self.content_keywords if keyword in text_lower)
        score += min(keyword_count * 3, 15)
        
        # Structure indicators (5 points)
        if soup.find('time') or soup.find(class_=re.compile(r'date|time|publish', re.I)):
            score += 5
        
        return score >= 70  # Higher threshold for blog detection
    
    def analyze_seo_quality(self, soup, url, content_text):
        score = 0
        title = soup.title.string.strip() if soup.title and soup.title.string else ""
        if not title: return 0
        
        if 30 <= len(title) <= 60: score += 25
        
        meta_desc = soup.find("meta", attrs={"name": "description"})
        if meta_desc and meta_desc.get("content"):
            if 120 <= len(meta_desc.get("content").strip()) <= 160: score += 20
        
        if len(soup.find_all("h1")) == 1: score += 15
        if len(soup.find_all("h2")) >= 2: score += 10
        if len(content_text) >= 500: score += 20
        if len(soup.find_all("a", href=True)) >= 5: score += 10
        if soup.find_all("img"): score += 5
        if soup.find("script", type="application/ld+json"): score += 5
        
        return score

class EmailExtractor:
    def __init__(self):
        self.email_pattern = re.compile(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b')
        self.exclude_patterns = [
            r'@example\.',
            r'@test\.',
            r'@domain\.',
            r'@yoursite\.',
            r'@company\.',
            r'noreply@',
            r'no-reply@',
            r'donotreply@'
        ]
        self.exclude_regex = re.compile('|'.join(self.exclude_patterns), re.I)
    
    def extract_emails(self, soup, url):
        emails = set()
        domain = urlparse(url).netloc
        
        # Extract from text content
        text = soup.get_text()
        found_emails = self.email_pattern.findall(text)
        
        # Extract from mailto links
        for link in soup.find_all('a', href=True):
            href = link['href']
            if href.startswith('mailto:'):
                email = href.replace('mailto:', '').split('?')[0]
                if self.email_pattern.match(email):
                    found_emails.append(email)
        
        # Filter and validate emails
        for email in found_emails:
            email = email.lower().strip()
            if (not self.exclude_regex.search(email) and 
                len(email) <= 100 and 
                email.count('@') == 1):
                emails.add(email)
        
        return list(emails)[:5]  # Limit to 5 emails per page

class MegaConnectionPool:
    def __init__(self, mysql_config, pool_size=10):
        self.config = mysql_config
        self.pool = queue.LifoQueue(maxsize=pool_size)
        self.lock = threading.RLock()
        self.max_connections = pool_size
        self.current_connections = 0
        
        # Create connections in parallel
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(self._create_connection) for _ in range(min(10, pool_size))]
            for future in as_completed(futures):
                try:
                    conn = future.result()
                    self.pool.put(conn)
                    self.current_connections += 1
                except Exception as e:
                    log(f"Connection failed: {e}", "ERROR")
        
        log(f"MegaPool: {self.current_connections}/{pool_size} ready", "INFO")
    
    def _create_connection(self):
        return mysql.connector.connect(
            host=self.config['host'],
            user=self.config['user'],
            password=self.config.get('password', os.getenv('DB_PASSWORD', 'Anh12345')),
            database=self.config['database'],
            autocommit=True,
            charset='utf8mb4',
            connect_timeout=2,
            pool_reset_session=False,
            buffered=True
        )
    
    def get_connection(self):
        try:
            conn = self.pool.get_nowait()
            if conn and conn.is_connected():
                return conn
            else:
                self._cleanup_connection(conn)
        except queue.Empty:
            pass
        
        with self.lock:
            if self.current_connections < self.max_connections:
                try:
                    conn = self._create_connection()
                    self.current_connections += 1
                    return conn
                except Exception:
                    pass
        
        try:
            return self.pool.get(timeout=0.3)
        except queue.Empty:
            raise ConnectionError("Pool busy")
    
    def _cleanup_connection(self, conn):
        if conn:
            try: conn.close()
            except: pass
        with self.lock:
            self.current_connections = max(0, self.current_connections - 1)
    
    def return_connection(self, conn):
        if not conn: return
        try:
            if conn.is_connected():
                self.pool.put_nowait(conn)
            else:
                self._cleanup_connection(conn)
        except queue.Full:
            self._cleanup_connection(conn)

class TurboBatchSaver:
    def __init__(self, pool):
        self.pool = pool
        self.article_buffer = deque()
        self.domain_buffer = deque()
        self.email_buffer = deque()
        self.pending_domains = set()
        self.lock = threading.RLock()
        self.batch_size = 1000
        self.last_flush = time.time()
        self.flush_executor = ThreadPoolExecutor(max_workers=6, thread_name_prefix="Flush")
    
    def add_article(self, url, title, seo_score):
        with self.lock:
            self.article_buffer.append((url, title, seo_score))
            if len(self.article_buffer) >= self.batch_size or time.time() - self.last_flush > 10:
                self._async_flush_articles()
    
    def add_domain(self, domain):
        if domain in self.pending_domains: return
        with self.lock:
            self.domain_buffer.append(domain)
            self.pending_domains.add(domain)
            if len(self.domain_buffer) >= self.batch_size or time.time() - self.last_flush > 10:
                self._async_flush_domains()
    
    def add_emails(self, emails, url):
        if not emails: return
        domain = urlparse(url).netloc
        with self.lock:
            for email in emails:
                self.email_buffer.append((email, domain))
            if len(self.email_buffer) >= 500 or time.time() - self.last_flush > 10:
                self._async_flush_emails()
    
    def _async_flush_articles(self):
        if not self.article_buffer: return
        batch = list(self.article_buffer)
        self.article_buffer.clear()
        self.last_flush = time.time()
        self.flush_executor.submit(self._bulk_insert_articles, batch)
    
    def _async_flush_domains(self):
        if not self.domain_buffer: return
        batch = list(self.domain_buffer)
        self.domain_buffer.clear()
        self.last_flush = time.time()
        self.flush_executor.submit(self._bulk_insert_domains, batch)
    
    def _async_flush_emails(self):
        if not self.email_buffer: return
        batch = list(self.email_buffer)
        self.email_buffer.clear()
        self.flush_executor.submit(self._bulk_insert_emails, batch)
    
    def _bulk_insert_articles(self, batch):
        conn = None
        try:
            conn = self.pool.get_connection()
            cursor = conn.cursor()
            cursor.executemany("INSERT IGNORE INTO seo_articles (url, title, seo_score) VALUES (%s, %s, %s)", batch)
            log(f"💾 Saved {len(batch)} articles", "DEBUG")
        except Exception as e:
            log(f"Article error: {e}", "ERROR")
        finally:
            if conn: self.pool.return_connection(conn)
    
    def _bulk_insert_domains(self, batch):
        conn = None
        try:
            conn = self.pool.get_connection()
            cursor = conn.cursor()
            priority = random.randint(1, 5)
            cursor.executemany("INSERT INTO domain_queue (domain, priority, status) VALUES (%s, %s, 'pending') ON DUPLICATE KEY UPDATE priority = GREATEST(priority, VALUES(priority))", [(d, priority) for d in batch])
            log(f"💾 Added {len(batch)} domains", "DEBUG")
        except Exception as e:
            log(f"Domain error: {e}", "ERROR")
        finally:
            if conn: self.pool.return_connection(conn)
    
    def _bulk_insert_emails(self, batch):
        conn = None
        for attempt in range(3):
            try:
                conn = self.pool.get_connection()
                cursor = conn.cursor()
                cursor.executemany("INSERT IGNORE INTO contact_emails (email, domain) VALUES (%s, %s)", batch)
                log(f"📧 Saved {len(batch)} emails", "DEBUG")
                break
            except ConnectionError:
                if attempt < 2:
                    time.sleep(0.1)
                    continue
                log(f"📧 Skipped {len(batch)} emails - pool busy", "DEBUG")
            except Exception as e:
                log(f"Email error: {e}", "ERROR")
                break
            finally:
                if conn: 
                    self.pool.return_connection(conn)
                    conn = None
    
    def force_flush(self):
        with self.lock:
            self._async_flush_articles()
            self._async_flush_domains()
            self._async_flush_emails()

class RouteDiscovery:
    def __init__(self, session):
        self.session = session
        
    def discover_all_routes(self, domain):
        routes = set()
        
        # Sitemap discovery
        routes.update(self._get_sitemap_routes(domain))
        
        # Common paths
        common_paths = [
            '/blog', '/news', '/articles', '/posts', '/stories',
            '/category', '/tag', '/archive', '/feed', '/rss',
            '/sitemap.xml', '/robots.txt'
        ]
        
        for path in common_paths:
            routes.add(f"{domain}{path}")
        
        # Date-based discovery
        for year in ['2024', '2023', '2022']:
            for month in ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']:
                routes.add(f"{domain}/{year}/{month}")
                routes.add(f"{domain}/blog/{year}/{month}")
        
        # Pagination discovery
        for page in range(1, 21):
            routes.add(f"{domain}/blog/page/{page}")
            routes.add(f"{domain}/page/{page}")
        
        return list(routes)
    
    def _get_sitemap_routes(self, domain):
        routes = set()
        sitemap_urls = ['/sitemap.xml', '/sitemap_index.xml', '/sitemaps.xml']
        
        for sitemap_path in sitemap_urls:
            try:
                resp = self.session.get(f"{domain}{sitemap_path}", timeout=3)
                if resp.status_code == 200:
                    root = ET.fromstring(resp.content)
                    for elem in root.iter():
                        if elem.tag.endswith('loc') and elem.text:
                            url = elem.text.strip()
                            if url.startswith('http'):
                                routes.add(url)
            except Exception:
                continue
        
        return routes

class MegaCrawler:
    def __init__(self, mysql_config, max_workers=400, domain_extraction=True):
        self.mysql_config = mysql_config
        self.pool = MegaConnectionPool(mysql_config)
        self._setup_tables()
        
        self.batch_saver = TurboBatchSaver(self.pool)
        self.blog_detector = SmartBlogDetector()
        self.email_extractor = EmailExtractor()
        self.visited = set()
        self.url_queue = queue.Queue(maxsize=790000)
        self.failed_domains = set()
        self.processed_domains = set()
        
        # Single optimized session
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'text/html,application/xhtml+xml',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive'
        })
        
        retry_strategy = Retry(total=1, backoff_factor=0.1, status_forcelist=[429, 500, 502, 503, 504])
        adapter = HTTPAdapter(max_retries=retry_strategy, pool_connections=150, pool_maxsize=300, pool_block=False)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
        
        self.max_workers = max_workers
        self.domain_extraction_enabled = True  # Always enable domain extraction
        self.workers = []
        self.stop_event = threading.Event()
        self.crawl_count = 0
        self.email_count = 0
        self.last_activity = time.time()
        self.stats_lock = threading.RLock()
        
        self.route_discovery = RouteDiscovery(self.session)
        
        # URL FEEDING SYSTEM
        self.url_feeder_thread = threading.Thread(target=self._continuous_url_feeder, daemon=True)
        self.url_feeder_thread.start()
        
        # Sitemap explosion system
        self.sitemap_executor = ThreadPoolExecutor(max_workers=20, thread_name_prefix="Sitemap")
        
        log(f"🚀 MegaCrawler ready for {max_workers} workers with EMAIL extraction", "INFO")
    
    def _continuous_url_feeder(self):
        while not self.stop_event.is_set():
            try:
                if self.url_queue.qsize() < 10000:
                    log("🍽️ FEEDING: Queue low, aggressive URL feeding...", "WARN")
                    self._aggressive_url_feeding()
                time.sleep(5)
            except Exception as e:
                log(f"Feeder error: {e}", "ERROR")
                time.sleep(10)
    
    def _aggressive_url_feeding(self):
        domains = self._get_domains_batch(50)
        
        if domains:
            futures = []
            for domain in domains:
                future = self.sitemap_executor.submit(self._explode_domain_urls, domain)
                futures.append(future)
            
            for future in as_completed(futures, timeout=30):
                try:
                    urls = future.result()
                    for url in urls[:100]:
                        try:
                            self.url_queue.put_nowait(url)
                        except queue.Full:
                            break
                except Exception as e:
                    log(f"Domain explosion error: {e}", "DEBUG")
        
        if self.url_queue.qsize() < 5000:
            self._add_emergency_urls()
    
    def _explode_domain_urls(self, domain):
        # Use the new route discovery system
        try:
            routes = self.route_discovery.discover_all_routes(domain)
            self._mark_domain_completed(domain)
            return routes[:500]  # Limit routes per domain
        except Exception as e:
            log(f"Route discovery error {domain}: {e}", "DEBUG")
            return []
    
    def _get_sitemap_urls(self, domain):
        urls = []
        try:
            for sitemap_path in ['/sitemap.xml', '/sitemap_index.xml', '/sitemaps.xml']:
                try:
                    resp = self.session.get(f"{domain}{sitemap_path}", timeout=3)
                    if resp.status_code == 200 and 'xml' in resp.headers.get('content-type', ''):
                        root = ET.fromstring(resp.content)
                        for elem in root.iter():
                            if elem.tag.endswith('loc') and elem.text:
                                url = elem.text.strip()
                                if url.startswith('http') and self._is_valid_url(url):
                                    urls.append(url)
                        if len(urls) > 100:
                            break
                except Exception:
                    continue
        except Exception:
            pass
        
        return urls[:200]
    
    def _get_domains_batch(self, limit=50):
        conn = None
        try:
            conn = self.pool.get_connection()
            cursor = conn.cursor()
            cursor.execute(f"""
                SELECT domain FROM domain_queue 
                WHERE status = 'pending' 
                ORDER BY priority DESC, added_at ASC 
                LIMIT {limit}
            """)
            domains = [row[0] for row in cursor.fetchall()]
            
            if domains:
                placeholders = ','.join(['%s'] * len(domains))
                cursor.execute(f"""
                    UPDATE domain_queue 
                    SET status = 'crawling', last_crawl = NOW() 
                    WHERE domain IN ({placeholders})
                """, domains)
            
            return domains
        except Exception as e:
            log(f"Get domains error: {e}", "ERROR")
            return []
        finally:
            if conn: self.pool.return_connection(conn)
    
    def _mark_domain_completed(self, domain):
        conn = None
        try:
            conn = self.pool.get_connection()
            cursor = conn.cursor()
            cursor.execute("UPDATE domain_queue SET status = 'completed', last_crawl = NOW() WHERE domain = %s", (domain,))
        except Exception:
            pass
        finally:
            if conn: self.pool.return_connection(conn)
    
    def _add_emergency_urls(self):
        # MASSIVE emergency URL list
        emergency_domains = [
            "https://www.searchenginejournal.com", "https://moz.com", "https://backlinko.com", "https://ahrefs.com",
            "https://blog.hubspot.com", "https://neilpatel.com", "https://contentmarketinginstitute.com", "https://www.semrush.com",
            "https://blog.google", "https://developers.google.com", "https://searchengineland.com", "https://www.searchenginewatch.com",
            "https://www.marketingland.com", "https://www.socialmediaexaminer.com", "https://copyblogger.com", "https://www.quicksprout.com",
            "https://blog.kissmetrics.com", "https://unbounce.com/blog", "https://blog.bufferapp.com", "https://blog.hootsuite.com",
            "https://www.convinceandconvert.com", "https://blog.marketo.com", "https://blog.salesforce.com", "https://www.entrepreneur.com",
            "https://www.inc.com", "https://www.forbes.com", "https://techcrunch.com", "https://mashable.com",
            "https://www.wired.com", "https://www.theverge.com", "https://arstechnica.com", "https://www.engadget.com"
        ]
        
        for domain in emergency_domains:
            # Thêm domain vào domain queue
            self.batch_saver.add_domain(domain)
            
            # Thêm các common paths
            paths = ['/blog', '/news', '/articles', '/posts', '/category', '/tag', '/archive']
            for path in paths:
                try:
                    self.url_queue.put_nowait(f"{domain}{path}")
                    # Thêm pagination
                    for page in range(1, 21):
                        self.url_queue.put_nowait(f"{domain}{path}/page/{page}")
                except queue.Full:
                    break
        
        log(f"🚑 Added {len(emergency_domains)} emergency domains with massive URLs", "INFO")
    
    def _setup_tables(self):
        conn = None
        try:
            conn = self.pool.get_connection()
            cursor = conn.cursor()
            
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS seo_articles (
                id INT AUTO_INCREMENT PRIMARY KEY,
                url VARCHAR(500) NOT NULL,
                title VARCHAR(500),
                seo_score TINYINT UNSIGNED,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE KEY url_idx (url) USING HASH,
                KEY seo_score_idx (seo_score)
            ) ENGINE=InnoDB CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci ROW_FORMAT=COMPRESSED
            """)
            
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS domain_queue (
                id INT AUTO_INCREMENT PRIMARY KEY,
                domain VARCHAR(200) NOT NULL,
                priority TINYINT DEFAULT 1,
                status ENUM('pending', 'crawling', 'completed', 'failed') DEFAULT 'pending',
                last_crawl TIMESTAMP NULL,
                retry_count TINYINT DEFAULT 0,
                added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE KEY domain_idx (domain) USING HASH,
                KEY status_priority_idx (status, priority DESC)
            ) ENGINE=InnoDB ROW_FORMAT=COMPRESSED
            """)
            
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS contact_emails (
                id INT AUTO_INCREMENT PRIMARY KEY,
                email VARCHAR(100) NOT NULL,
                domain VARCHAR(200),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE KEY email_idx (email) USING HASH,
                KEY domain_idx (domain)
            ) ENGINE=InnoDB CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci ROW_FORMAT=COMPRESSED
            """)
            
        except Exception as e:
            log(f"Setup error: {e}", "ERROR")
        finally:
            if conn: self.pool.return_connection(conn)
    
    def extract_content(self, soup):
        for tag in soup(['script', 'style', 'nav', 'header', 'footer', 'aside']):
            tag.decompose()
        
        for selector in ['article', '.post-content', '.entry-content', '.content']:
            elem = soup.select_one(selector)
            if elem:
                return elem.get_text(separator=' ', strip=True)
        
        return soup.body.get_text(separator=' ', strip=True) if soup.body else ""
    
    def process_domain_completely(self, domain):
        """Process a domain by discovering all routes first, then crawling them"""
        if domain in self.processed_domains:
            return
        
        log(f"🔍 Discovering routes for {domain}", "INFO")
        routes = self.route_discovery.discover_all_routes(domain)
        
        log(f"📍 Found {len(routes)} routes for {domain}", "INFO")
        
        # Add all routes to queue
        for route in routes:
            if route not in self.visited:
                try:
                    self.url_queue.put_nowait(route)
                except queue.Full:
                    break
        
        self.processed_domains.add(domain)
        self._mark_domain_completed(domain)
    
    def _extract_all_urls_and_domains(self, soup, current_url, current_domain):
        """Thu thập TẤT CẢ URLs và domains từ trang"""
        parsed_current = urlparse(current_url)
        
        # Thu thập TẤT CẢ links không giới hạn
        for link in soup.find_all("a", href=True):
            href = link['href'].strip()
            if not href or href.startswith('#') or href.startswith('javascript:') or href.startswith('mailto:'):
                continue
            
            # Skip file extensions
            if any(href.lower().endswith(ext) for ext in ['.pdf', '.jpg', '.jpeg', '.png', '.gif', '.css', '.js', '.zip', '.doc', '.docx', '.xls', '.xlsx', '.ppt', '.pptx']):
                continue
            
            # External domain - Thu thập domain mới
            if href.startswith('http'):
                try:
                    parsed_href = urlparse(href)
                    if parsed_href.netloc and parsed_href.netloc != current_domain:
                        # Thêm domain mới
                        new_domain = f"{parsed_href.scheme}://{parsed_href.netloc}"
                        self.batch_saver.add_domain(new_domain)
                        
                        # Thêm luôn URL này vào queue
                        if self._is_valid_url(href):
                            try:
                                self.url_queue.put_nowait(href)
                            except queue.Full:
                                pass
                except Exception:
                    continue
            
            # Internal link - Thêm vào queue
            elif href.startswith('/'):
                full_url = urljoin(f"{parsed_current.scheme}://{current_domain}", href)
                if self._is_valid_url(full_url):
                    try:
                        self.url_queue.put_nowait(full_url)
                    except queue.Full:
                        pass
        
        # Thu thập từ JavaScript và JSON-LD
        self._extract_urls_from_scripts(soup, current_domain)
        
        # Thu thập từ sitemap links
        self._extract_sitemap_references(soup, current_domain)
    
    def _extract_urls_from_scripts(self, soup, domain):
        """Thu thập URLs từ JavaScript và JSON-LD"""
        for script in soup.find_all('script'):
            if script.string:
                # Tìm URLs trong JavaScript
                import re
                url_pattern = re.compile(r'["\']https?://[^"\'\ ]+["\']')
                urls = url_pattern.findall(script.string)
                
                for url_match in urls[:10]:  # Giới hạn 10 URLs per script
                    url = url_match.strip('"\'')
                    if self._is_valid_url(url):
                        try:
                            self.url_queue.put_nowait(url)
                            # Nếu là domain khác, thêm vào domain queue
                            parsed = urlparse(url)
                            if parsed.netloc != domain:
                                self.batch_saver.add_domain(f"{parsed.scheme}://{parsed.netloc}")
                        except queue.Full:
                            break
    
    def _extract_sitemap_references(self, soup, domain):
        """Tìm references đến sitemap"""
        # Tìm trong robots.txt references
        for link in soup.find_all('link', rel='sitemap'):
            if link.get('href'):
                sitemap_url = urljoin(f"https://{domain}", link['href'])
                try:
                    self.url_queue.put_nowait(sitemap_url)
                except queue.Full:
                    pass
    
    def _add_internal_links(self, soup, base_domain):
        """Thêm MASSIVE internal links"""
        count = 0
        for link in soup.find_all("a", href=True):
            if count >= 100:  # Tăng limit lên 100
                break
                
            href = link['href'].strip()
            if href.startswith('/'):
                full_url = urljoin(base_domain, href)
                if self._is_valid_url(full_url):
                    try:
                        self.url_queue.put_nowait(full_url)
                        count += 1
                    except queue.Full:
                        break
    
    def _is_valid_url(self, url):
        if not url or not url.startswith('http'):
            return False
        
        # Chỉ loại bỏ file extensions và admin paths
        invalid_extensions = ['.pdf', '.jpg', '.jpeg', '.png', '.gif', '.css', '.js', '.zip', '.doc', '.docx', '.xls', '.xlsx', '.ppt', '.pptx', '.mp4', '.mp3', '.avi', '.mov']
        invalid_paths = ['/wp-admin/', '/admin/', '/api/', '/wp-content/', '/wp-includes/', '/xmlrpc.php']
        
        url_lower = url.lower()
        
        # Loại bỏ file extensions
        if any(url_lower.endswith(ext) for ext in invalid_extensions):
            return False
        
        # Loại bỏ admin paths
        if any(path in url_lower for path in invalid_paths):
            return False
        
        return True
    
    def crawl_url(self, url):
        if url in self.visited:
            return
        
        parsed_url = urlparse(url)
        domain = parsed_url.netloc
        
        if domain in self.failed_domains:
            return
        
        self.visited.add(url)
        
        try:
            response = self.session.get(url, timeout=1.5, stream=True)
            
            if response.status_code != 200:
                response.close()
                return
            
            content_type = response.headers.get('content-type', '')
            if 'text/html' not in content_type:
                response.close()
                return
            
            content = response.text[:300000]
            response.close()
            
            soup = BeautifulSoup(content, "html.parser")
            
            self.last_activity = time.time()
            with self.stats_lock:
                self.crawl_count += 1
            
            # EMAIL EXTRACTION - Always extract emails from every page
            emails = self.email_extractor.extract_emails(soup, url)
            if emails:
                self.batch_saver.add_emails(emails, url)
                with self.stats_lock:
                    self.email_count += len(emails)
                log(f"📧 Found {len(emails)} emails on {domain}", "INFO")
            
            # AGGRESSIVE DOMAIN & URL EXTRACTION - Thu thập TẤT CẢ
            self._extract_all_urls_and_domains(soup, url, domain)
            
            # Smart blog detection
            content_text = self.extract_content(soup)
            
            if self.blog_detector.is_blog_article(url, soup, content_text):
                # This is a blog article - analyze and save
                title = soup.title.string.strip() if soup.title and soup.title.string else ""
                seo_score = self.blog_detector.analyze_seo_quality(soup, url, content_text)
                
                if seo_score >= 60:
                    self.batch_saver.add_article(url, title, seo_score)
                    log(f"⭐ BLOG: {title[:10]}... (SEO: {seo_score})", "INFO")
            else:
                # Not a blog article - check if we need to process this domain
                if domain not in self.processed_domains:
                    self.process_domain_completely(f"{parsed_url.scheme}://{domain}")
            
            # Thu thập MASSIVE internal links
            self._add_internal_links(soup, f"{parsed_url.scheme}://{domain}")
                        
        except Exception as e:
            if any(x in str(e).lower() for x in ['timeout', 'connection']):
                self.failed_domains.add(domain)
    
    def worker(self):
        while not self.stop_event.is_set():
            try:
                url = self.url_queue.get(timeout=2)
                if url is None:
                    break
                self.crawl_url(url)
                self.url_queue.task_done()
            except queue.Empty:
                time.sleep(0.1)
                continue
            except Exception as e:
                log(f"Worker error: {e}", "DEBUG")
    
    def start_workers(self):
        for i in range(self.max_workers):
            worker = threading.Thread(target=self.worker, daemon=True, name=f"Worker-{i}")
            worker.start()
            self.workers.append(worker)
        log(f"🚀 Launched {self.max_workers} workers", "INFO")
    
    def get_stats(self):
        conn = None
        try:
            conn = self.pool.get_connection()
            cursor = conn.cursor()
            cursor.execute("""
                SELECT COUNT(*) as articles, AVG(seo_score) as avg_score,
                       (SELECT COUNT(*) FROM domain_queue WHERE status = 'pending') as pending,
                       (SELECT COUNT(*) FROM contact_emails) as emails
                FROM seo_articles
            """)
            result = cursor.fetchone()
            return result[0], round(result[1] or 0, 1), result[2], result[3]
        except Exception:
            return 0, 0, 0, 0
        finally:
            if conn: self.pool.return_connection(conn)
    
    def run_forever(self):
        self.start_workers()
        
        try:
            while True:
                articles, avg_seo, pending, total_emails = self.get_stats()
                
                with self.stats_lock:
                    crawl_rate = self.crawl_count
                    email_rate = self.email_count
                    self.crawl_count = 0
                    self.email_count = 0
                
                active_workers = len([w for w in self.workers if w.is_alive()])
                
                log(f"🚀 Queue: {self.url_queue.qsize():,} | Articles: {articles:,} | SEO: {avg_seo} | Emails: {total_emails:,} | Pending: {pending:,} | Workers: {active_workers} | Rate: {crawl_rate}/10s | Email Rate: {email_rate}/10s", "INFO")
                
                self.batch_saver.force_flush()
                
                # Memory management
                if len(self.visited) > 3000000:
                    visited_list = list(self.visited)
                    self.visited = set(visited_list[-1500000:])
                    log("🧹 Memory optimized", "DEBUG")
                
                if len(self.failed_domains) > 50000:
                    self.failed_domains.clear()
                    log("🧹 Reset failed domains", "DEBUG")
                
                time.sleep(10)
                
        except KeyboardInterrupt:
            log("🛑 Shutting down...", "INFO")
            self.stop_event.set()

if __name__ == "__main__":
    mysql_config = {
        "host": "database-2.c2zamiaeuqn1.us-east-1.rds.amazonaws.com",
        "user": "admin", 
        "password": os.getenv('DB_PASSWORD', 'Anh12345'),
        "database": "AWS"
    }
    
    initial_domains = [
        "https://www.searchenginejournal.com",
    ]
    
    try:
        crawler = MegaCrawler(mysql_config, max_workers=400, domain_extraction=False)
        
        for domain in initial_domains:
            crawler.batch_saver.add_domain(domain)
        crawler.batch_saver.force_flush()
        
        log("🚀 Starting MegaCrawler with EMAIL collection...", "INFO")
        crawler.run_forever()
        
    except Exception as e:
        log(f"💥 CRITICAL: {e}", "ERROR")
        sys.exit(1)
