visualCapitalist = {
    'websites': 'https://www.visualcapitalist.com/category/markets/',
    'post_selectors': ['#mvp-tab-col1 > a:nth-child(1)', None],
    'title_selectors': ['//*[@id="mvp-post-head"]/h1', None],
    'content_selectors': ['//*[@id="mvp-content-main"]', None],
    'image_selectors': ['#mvp-post-head > span > p > a > img', None]

}

import logging
import time
from playwright.async_api import async_playwright
from retry import retry
class Scraper:
    """
    This class provides a method to scrap data from a website using Playwright.
    """
    @retry(tries=3, delay=2)
    async def scrap_from_website(self, website_info):
        """
        Scrapes data from a website using Playwright.

        Args:
            website_info (dict): A dictionary containing website and selectors information.

        Returns:
            tuple: A tuple containing title, content, and image source.
        """

        start_time = time.time()
        logging.info('Scraping data from website...')
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context(user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36")
            context.set_default_navigation_timeout(0)
            page = await context.new_page()

            await page.goto(website_info['websites'], timeout=0)
            if website_info['post_selectors'][1]:
                latest_post_url = await page.locator(website_info['post_selectors'][0]).filter(website_info['post_selectors'][1]).get_attribute('href', timeout=100000)
            else:
                latest_post_url = await page.locator(website_info['post_selectors'][0]).get_attribute('href', timeout=100000)

            if latest_post_url:
                await page.close()
                browser2 = await p.chromium.launch(headless=True)
                context2 = await browser2.new_context(user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36")
                page2 = await context2.new_page()
                logging.info(f'Navigating to {latest_post_url}')
                await page2.goto(latest_post_url, timeout=1000000, wait_until='domcontentloaded')
                logging.info('Getting title and content...')
                if website_info['title_selectors'][1]:
                    title = await page2.locator(website_info['title_selectors'][0]).filter(website_info['title_selectors'][1]).inner_text()
                else:
                    title = await page2.locator(website_info['title_selectors'][0]).inner_text()

                if website_info['content_selectors'][1]:
                    content = await page2.locator(website_info['content_selectors'][0]).filter(website_info['content_selectors'][1]).inner_text()
                else:
                    content = await page2.locator(website_info['content_selectors'][0]).inner_text()
                logging.info('Getting image source...')
                if website_info['image_selectors'][1]:
                    image_src = await page2.locator(website_info['image_selectors'][0]).filter(website_info['image_selectors'][1]).get_attribute('src')
                else:
                    image_src = await page2.locator(website_info['image_selectors'][0]).get_attribute('src')
                logging.info('Done!')

                await page2.close()
                await browser2.close()

                logging.info(f'title:{title}\ncontent:{content}\nimage_src:{image_src}')
                end_time = time.time()
                logging.info(f'Time taken: {end_time - start_time}')
                return title, content, image_src
            else:
                raise ValueError("Latest post element not found.")

