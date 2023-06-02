import pandas as pd
from bs4 import BeautifulSoup
import requests
import os
import time
import re


def clean_content(html):
    content_encoded = sup_sub_encode(str(html))
    content_encoded = BeautifulSoup(content_encoded, 'html.parser').text
    content_encoded_decoded = sup_sub_decode(content_encoded)
    content_cleaned = strip_it(content_encoded_decoded).strip()
    return content_cleaned


def sup_sub_encode(html):
    """Encodes superscript and subscript tags"""
    encoded_html = html.replace('<sup>', 's#p').replace('</sup>', 'p#s').replace('<sub>', 's#b').replace('</sub>',
                                                                                                         'b#s') \
        .replace('<Sup>', 's#p').replace('</Sup>', 'p#s').replace('<Sub>', 's#b').replace('</Sub>', 'b#s')
    return encoded_html


def sup_sub_decode(html):
    """Decodes superscript and subscript tags"""
    decoded_html = html.replace('s#p', '<sup>').replace('p#s', '</sup>').replace('s#b', '<sub>').replace('b#s',
                                                                                                         '</sub>')
    return decoded_html


def strip_it(text):
    return re.sub(r'\s+', ' ', text)


def status_log(r):
    """Pass response as a parameter to this function"""
    url_log_file = 'url_log.txt'
    if not os.path.exists(os.getcwd() + '\\' + url_log_file):
        with open(url_log_file, 'w') as f:
            f.write('url, status_code\n')
    with open(url_log_file, 'a') as file:
        file.write(f'{r.url}, {r.status_code}\n')


def retry(func, retries=3):
    """Decorator function"""
    retry.count = 0

    def retry_wrapper(*args, **kwargs):
        attempt = 0
        while attempt < retries:
            try:
                return func(*args, **kwargs)
            except requests.exceptions.ConnectionError as e:
                attempt += 1
                total_time = attempt * 10
                print(f'Retrying {attempt}: Sleeping for {total_time} seconds, error: ', e)
                time.sleep(total_time)
            if attempt == retries:
                retry.count += 1
                url_log_file = 'url_log.txt'
                if not os.path.exists(os.getcwd() + '\\' + url_log_file):
                    with open(url_log_file, 'w') as f:
                        f.write('url, status_code\n')
                with open(url_log_file, 'a') as file:
                    file.write(f'{args[0]}, requests.exceptions.ConnectionError\n')
            if retry.count == 3:
                print("Stopped after retries, check network connection")
                raise SystemExit

    return retry_wrapper


@retry
def get_soup(url, headers):
    refer = r'https://developer.mozilla.org/en-US/docs/Web/HTTP/Status#server_error_responses'
    r = requests.get(url, headers=headers)
    if r.status_code == 200:
        soup = BeautifulSoup(r.text, 'html.parser')
        return soup
    elif 499 >= r.status_code >= 400:
        print(f'client error response, status code {r.status_code} \nrefer: {refer}')
        status_log(r)
        return None
    elif 599 >= r.status_code >= 500:
        print(f'server error response, status code {r.status_code} \nrefer: {refer}')
        count = 1
        while count != 6:
            print('while', count)
            r = requests.get(url, headers=headers)  # your request get or post
            print('status_code: ', r.status_code)
            if r.status_code == 200:
                soup = BeautifulSoup(r.text, 'html.parser')
                return soup
            else:
                print('retry ', count)
                count += 1
                time.sleep(count * 2)
    else:
        status_log(r)
        return None


class AmazonScraper:
    def __init__(self, src_url, base_url_):
        self.SOURCE_URL = src_url
        self.BASE_URL = base_url_
        self.data_list = []
        self.existing_data_list = None
        self.output_filename = 'amazon_books_bestsellers.csv'
        if os.path.exists(self.output_filename):
            self.existing_data_list = pd.read_csv(self.output_filename).fillna('')

    def book_details_scraper(self, book_url_list):
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/113.0'
        }
        for book_url in book_url_list:
            page_soup = get_soup(book_url, headers)
            if page_soup:
                book_title, book_category, book_rank, book_author, book_original_price, book_offer_price, \
                    savings_percentage = '', '', '', '', '', '', ''
                try:
                    book_title_tag = page_soup.find('span', attrs={'id': 'productTitle'})
                    book_title = clean_content(book_title_tag)
                except:
                    book_title = ''
                print(book_title)
                try:
                    book_category_tag = page_soup.find('span', class_='cat-link')
                    book_category = clean_content(book_category_tag).strip('in ')
                except:
                    book_category = ''
                # print(book_category)
                try:
                    book_rank_tag = page_soup.find('i', class_='a-icon a-icon-addon p13n-best-seller-badge')
                    book_rank = clean_content(book_rank_tag)
                    book_rank = book_rank.replace(' Best Seller', '').replace(' Most Gifted', '')
                except:
                    book_rank = ''
                # print(book_rank)
                try:
                    book_author_tag = page_soup.find('span', class_='author notFaded')
                    book_author = clean_content(book_author_tag).replace(' (Author)', '')
                except:
                    book_author = ''
                # print(book_author)
                try:
                    book_original_price_tag = page_soup.find('span', attrs={'id': 'listPrice'})
                    book_original_price = clean_content(book_original_price_tag).strip('₹')
                except:
                    book_original_price = ''
                # print(book_original_price)
                try:
                    book_offer_price_tag = page_soup.find('span', attrs={'id': 'price'})
                    book_offer_price = clean_content(book_offer_price_tag).strip('₹')
                except:
                    book_offer_price = ''
                # print(book_offer_price)
                try:
                    savings_percentage_tag = page_soup.find('span', attrs={'id': 'savingsPercentage'})
                    savings_percentage = clean_content(savings_percentage_tag).strip('(').strip(')')
                except:
                    savings_percentage = ''
                # print(savings_percentage)
                book_detail_dict = {
                    'title': book_title,
                    'url': book_url,
                    'category': book_category,
                    'rank': book_rank,
                    'author': book_author,
                    'original_price': book_original_price,
                    'price': book_offer_price,
                    'savings_percentage': savings_percentage
                }
                self.data_list.append(book_detail_dict)
                books_df = pd.DataFrame(self.data_list)
                if self.existing_data_list is not None:
                    books_df = pd.concat([self.existing_data_list, self.data_list])
                books_df.drop_duplicates(inplace=True, keep='first')
                books_df.to_csv(self.output_filename, index=False)

    def scrape_books_detail_page_urls(self, soup):
        books_container_list = soup.find_all('div', class_='a-column a-span12 a-text-center _cDEzb_grid-column_2hIsc')
        books_detail_page_urls = [self.BASE_URL + container.find('a', class_='a-link-normal')['href'] for container in
                                  books_container_list]
        if books_detail_page_urls:
            self.book_details_scraper(books_detail_page_urls)

    def scraper(self):
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/113.0'
        }
        next_page_url = self.SOURCE_URL
        while next_page_url is not None:
            page_soup = get_soup(next_page_url, headers)
            if page_soup:
                self.scrape_books_detail_page_urls(page_soup)
                try:
                    next_page_url = self.BASE_URL + page_soup.find('li', class_='a-last').find('a')['href']
                except:
                    next_page_url = None
            else:
                next_page_url = None


if __name__ == '__main__':
    source_url = 'https://www.amazon.in/gp/bestsellers/books/'
    base_url = 'https://www.amazon.in'
    amazon_scraper = AmazonScraper(source_url, base_url)
    amazon_scraper.scraper()
