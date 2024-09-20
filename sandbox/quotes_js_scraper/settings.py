# Scrapy settings for quotes_js_scraper project
#
# For simplicity, this file contains only settings considered important or
# commonly used. You can find more settings consulting the documentation:
#
#     https://docs.scrapy.org/en/latest/topics/settings.html
#     https://docs.scrapy.org/en/latest/topics/downloader-middleware.html
#     https://docs.scrapy.org/en/latest/topics/spider-middleware.html

import logging

# Configure logging
LOG_ENABLED = True
LOG_LEVEL = "DEBUG"  # Set the log level to DEBUG to capture all log data
LOG_FILE = "scrapy_log.txt"  # Specify the log file path
LOG_FILE_APPEND = False


def should_abort_request(request):
    allowed_resource_types = {
        "xhr",
        "document",
        "script",
    }  # Define allowed resource types
    return request.resource_type not in allowed_resource_types


PLAYWRIGHT_ABORT_REQUEST = should_abort_request
PLAYWRIGHT_LAUNCH_OPTIONS = {
    # "headless": True,
    "headless": False,
    "timeout": 60000,
}

BOT_NAME = "quotes_js_scraper"

SPIDER_MODULES = ["quotes_js_scraper.spiders"]
NEWSPIDER_MODULE = "quotes_js_scraper.spiders"

DOWNLOAD_HANDLERS = {
    "http": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
    "https": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
}

TWISTED_REACTOR = "twisted.internet.asyncioreactor.AsyncioSelectorReactor"

UNWANTED_STRINGS = ["google", "gumgum", "doubleclick", "adservice", "ads", "analytics"]
WANTED_STRINGS = ["yahoo"]

# PLAYWRIGHT_ACCEPT_REQUEST_PREDICATE = lambda req: (
#     # any(s in req.url for s in WANTED_STRINGS)
#     # and not any(s in req.url for s in UNWANTED_STRINGS)
#     # and req.resourceType in ["document", "xhr"]
#     # and req.referrer == "https://au.finance.yahoo.com/quote/CBA.AX/history/"
#     # and not any(s in req.url for s in UNWANTED_STRINGS)
#     req.referrer
#     == "https://au.finance.yahoo.com/quote/CBA.AX/history/"
# )


# # Define the predicate function
# def accept_request_predicate(req):
#     logging.debug(f"Checking request: {req.url}")
#     logging.debug(f"Referrer: {req.referrer}")
#     if any(s in req.url for s in WANTED_STRINGS):
#         if not any(s in req.url for s in UNWANTED_STRINGS):
#             if req.resourceType in ["document", "xhr"]:
#                 if req.referrer not in ["https://ads.pubmatic.com", "https://blah"]:
#                     logging.debug("Request accepted")
#                     return True
#     logging.debug("Request blocked")
#     return False

# Set default timeout for Playwright
PLAYWRIGHT_DEFAULT_NAVIGATION_TIMEOUT = 60000  # 60 seconds
PLAYWRIGHT_DEFAULT_TIMEOUT = 60000  # 60 seconds


# Crawl responsibly by identifying yourself (and your website) on the user-agent
# USER_AGENT = 'quotes_js_scraper (+http://www.yourdomain.com)'

# Obey robots.txt rules
ROBOTSTXT_OBEY = True

# Configure maximum concurrent requests performed by Scrapy (default: 16)
# CONCURRENT_REQUESTS = 32

# Configure a delay for requests for the same website (default: 0)
# See https://docs.scrapy.org/en/latest/topics/settings.html#download-delay
# See also autothrottle settings and docs
# DOWNLOAD_DELAY = 3
# The download delay setting will honor only one of:
# CONCURRENT_REQUESTS_PER_DOMAIN = 16
# CONCURRENT_REQUESTS_PER_IP = 16

# Disable cookies (enabled by default)
# COOKIES_ENABLED = False

# Disable Telnet Console (enabled by default)
# TELNETCONSOLE_ENABLED = False

# Override the default request headers:
# DEFAULT_REQUEST_HEADERS = {
#   'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
#   'Accept-Language': 'en',
# }

# Enable or disable spider middlewares
# See https://docs.scrapy.org/en/latest/topics/spider-middleware.html
SPIDER_MIDDLEWARES = {
    "quotes_js_scraper.middlewares.QuotesJsScraperSpiderMiddleware": 1,
}

# Enable or disable downloader middlewares
# See https://docs.scrapy.org/en/latest/topics/downloader-middleware.html
DOWNLOADER_MIDDLEWARES = {
    "quotes_js_scraper.middlewares.QuotesJsScraperDownloaderMiddleware": 1,
}

# Enable or disable extensions
# See https://docs.scrapy.org/en/latest/topics/extensions.html
# EXTENSIONS = {
#    'scrapy.extensions.telnet.TelnetConsole': None,
# }

# Configure item pipelines
# See https://docs.scrapy.org/en/latest/topics/item-pipeline.html
# ITEM_PIPELINES = {
#    'quotes_js_scraper.pipelines.QuotesJsScraperPipeline': 300,
# }

# Enable and configure the AutoThrottle extension (disabled by default)
# See https://docs.scrapy.org/en/latest/topics/autothrottle.html
# AUTOTHROTTLE_ENABLED = True
# The initial download delay
# AUTOTHROTTLE_START_DELAY = 5
# The maximum download delay to be set in case of high latencies
# AUTOTHROTTLE_MAX_DELAY = 60
# The average number of requests Scrapy should be sending in parallel to
# each remote server
# AUTOTHROTTLE_TARGET_CONCURRENCY = 1.0
# Enable showing throttling stats for every response received:
# AUTOTHROTTLE_DEBUG = False

# Enable and configure HTTP caching (disabled by default)
# See https://docs.scrapy.org/en/latest/topics/downloader-middleware.html#httpcache-middleware-settings
# HTTPCACHE_ENABLED = True
# HTTPCACHE_EXPIRATION_SECS = 0
# HTTPCACHE_DIR = 'httpcache'
# HTTPCACHE_IGNORE_HTTP_CODES = []
# HTTPCACHE_STORAGE = 'scrapy.extensions.httpcache.FilesystemCacheStorage'
