import logging
import requests
import os
import aiohttp
import asyncio


class FriendlyVendor:

    def __init__(self):
        self.api_url = os.environ.get("VENDOR_API_URL")
        self.batch_size = os.environ.get("VENDOR_API_PAGES_BATCH_SIZE")

    async def download_page(self, session, page=1):
        url = f"{self.api_url}users?page={page}"
        async with session.get(url) as resp:
            data = await resp.json()
            return data

    async def download_pages(self, from_page, end_page):
        async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(verify_ssl=False)) as session:

            tasks = []
            for page in range(from_page, end_page):
                tasks.append(asyncio.ensure_future(self.download_page(session, page)))

            return await asyncio.gather(*tasks)

