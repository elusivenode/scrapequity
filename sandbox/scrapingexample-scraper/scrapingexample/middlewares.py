# middlewares.py

from scrapy import signals
from scrapy_playwright.page import PageMethod
import logging


class AbortRequestMiddleware:

    def __init__(self):
        self.abort_keywords = ["google"]

    async def process_request(self, request, spider):
        logging.info(f"AbortRequestMiddleware: processing request {request.url}")
        print(f"this is request.meta{request.meta}")
        if "playwright" in request.meta:
            # Adding PageMethod to abort requests containing specific keywords
            request.meta["playwright_page_methods"] = [
                PageMethod("route", self.abort_requests)
            ]
            print(f"inside the if statement")
            print(f"this is request.meta{request.meta}")

    async def abort_requests(self, route):
        url = route.request.url
        print(f"*** does this ever get called ***")
        if any(keyword in url for keyword in self.abort_keywords):
            logging.info(f"AbortRequestMiddleware: aborted request {route.request.url}")
            await route.abort(self)  # Abort the request
        else:
            await route.continue_(self)  # Continue normally if no keyword matches
