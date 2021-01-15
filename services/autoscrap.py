from autoscraper import AutoScraper

url = "https://finance.yahoo.com/quote/AAPL/"

wanted_list = ["115.36"]
# wanted_list = ["A Smart, Automatic, Fast and Lightweight Web Scraper for Python", "115.36"]

scraper = AutoScraper()

# Here we can also pass html content via the html parameter instead of the url (html=html_content)
result = scraper.build(url, wanted_list)
print(result)

scraper.save("yahoo-finance")
scraper.load("yahoo-finance")

result2 = scraper.get_result_exact("https://finance.yahoo.com/quote/MSFT/")
print(result2)