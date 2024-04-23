# Scraping Hotel Information

import pandas as pd
import os
import logging
import scrapy
from scrapy.crawler import CrawlerProcess

# automate the submission of form requests on Booking.com

df=pd.read_csv(filepath_or_buffer='weather_df.csv')

class BookingSpider(scrapy.Spider):
    name = "booking"
    start_urls = [
        "https://www.booking.com/index.fr.html"
    ]
    cities = df['City'].unique()

    def parse(self, response):  # method used when receiving a response from the website
        for city in self.cities:
            yield scrapy.FormRequest.from_response( #send a new request
                response,
                formdata={'ss': city}, # data sent with the new request
                callback=self.after_search,
                cb_kwargs={'city': city}
            )
    # Run the search results and extract the URLs of each hotel and then pass them to parse_review
    def after_search(self, response, city): # extract hotel link
        hotel_links = response.css('a.a78ca197d0::attr(href)').getall()
        for link in hotel_links:
            full_url = response.urljoin(link)
            yield scrapy.Request(url=full_url, callback=self.parse_review, cb_kwargs={'city': city})

    def parse_review(self, response, city):
        # Extraire les informations détaillées de l'hôtel
        items = {
            'city': city,
            'hotel_name': response.xpath("//h2[@class='d2fee87262 pp-header__title']/text()").get(), #.strip(), 
            'hotel_address': response.xpath('//span[contains(@class, "hp_address_subtitle")]/text()').get(), #.strip(),
            'coordinates': response.xpath('//a[@id="hotel_address"]/@data-atlas-latlng').get(),
            #'general_review': response.xpath('//div[contains(@class, "bui-review-score__text")]/text()').get().strip(),
            'rating': response.xpath('//*[@class="a3b8729ab1 d86cee9b25"]/text()').get(), #.strip(),
            'number_of_reviews': response.xpath('//*[@class="abf093bdfe f45d8e4c32 d935416c47"]/text()').get(), #.strip(),
            'facilities': response.xpath('//span[@class="a5a5a75131"]/text()').getall(),
            'description': response.xpath("//p[@data-testid='property-description']/text()").get(), #.strip(),
            'url': response.url
        }
        yield items


# Name of the file where the results will be saved
filename = "hotel.json"

# If file already exists, delete it before crawling (because Scrapy will 
# concatenate the last and new results otherwise)
if filename in os.listdir('json/'):
        os.remove('json/' + filename) # Supprimer le fichier s'il existe déjà

process = CrawlerProcess(settings = {
    'USER_AGENT': 'Chrome/97.0',
    'LOG_LEVEL': logging.INFO,
    "FEEDS": {
        'json/' + filename : {"format": "json"},
    }
})

process.crawl(BookingSpider)
process.start()# the script will block here until the crawling is finished
        