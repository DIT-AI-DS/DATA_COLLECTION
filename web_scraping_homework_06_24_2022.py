"""
File: <web_scraping_homework_06_24_2022.>.py
-------------------
"""

# Standard for making HTTP requests in Python
import requests

# With caching, the response will be fetched once,
import requests_cache

# Used to work with Regular Expressions
import re

# Allow to scrape
from bs4 import BeautifulSoup


class ScrapFactory(object):
    """
        classmethod() methods are bound to a class rather than an object.
        Class methods can be called by both class and object. These methods can be called with a class or with an object.
        A class method takes cls as the first parameter while a static method needs no specific parameters.
        A class method can access or modify the class state while a static method can’t access or modify it.
    """

    BASE_URL = None

    # A class method to process the URL and make a get request
    @classmethod
    def fetch(cls, url: str):

        # Connecting to the API
        with requests.Session() as session:
            response_object = session.get(url)
        return response_object.content  # CHANGED from text to content for the B4S

    # The def fetch(cls, url: str): method enhanced by caching the content you scrape so that it’s only downloaded once
    @classmethod
    def fetch_cached(cls, url: str):

        # Connecting to the API
        with requests_cache.CachedSession('scraping_cache') as session:
            response_object = session.get(url)
        return response_object.content

    # A class method to parse the HTML and return a beautifulSoup Object
    @classmethod
    def setBs4Instance(cls, url: str):

        bfs_object = BeautifulSoup(
            cls.fetch(url),
            features="html.parser"
        )
        return bfs_object

    # A class method to find all the anchor tags with "href" and the regular expression countries
    @classmethod
    def cleanByReplace(cls, string: str):

        string = str(string)
        regex_object = re.compile('/countries/\w+')
        string = re.findall(regex_object, string)
        return string[0]  # Remove the first element of the string

    # A class method to build the base URL for further use
    @classmethod
    def formUrlLink(cls, string: str):

        return f'{cls.BASE_URL}{string}'

    # A class method to
    @classmethod
    def getLinkList(cls, SoupList: list):

        return [
            cls.formUrlLink(
                cls.cleanByReplace(link))
            for link in SoupList
        ]

    @classmethod
    def getLinkList_modified(cls, data, pattern: str, result_limit):

        # find all the anchor tags with "href"
        final_urls = []
        for link in data.find_all('a', attrs={'href': re.compile(pattern)}, limit=result_limit):
            temp_url = cls.BASE_URL + link['href']  # extract the href string
            final_urls.append(temp_url)

        del final_urls[:2]
        return final_urls

    @classmethod
    def getImgLink(cls, instanceBs4):

        return instanceBs4.find(class_='tei-graphic')['src']

    @classmethod
    def getImgLink_2(cls, instanceBs4, pattern):

        try:
            return instanceBs4.find(class_=pattern)['src']
        except:
            pass  # The pattern is not found

    @classmethod
    def scrapChildLinkList(cls, SoupUrlList: list):

        newList = []
        for link in SoupUrlList:
            data = cls.setBs4Instance(link)
            data = data.find(class_='tei-graphic')
            if data:
                newList.append(data['src'])
            continue
        return newList


def from_the_course():
    data = BeautifulSoup(
        ScrapFactory.fetch('https://history.state.gov/countries/all'),
        features="html.parser"
    )
    regex = re.compile('/countries/\w+')
    lista = data.find_all(href=regex)
    lista = lista[2:-1]

    instance = ScrapFactory
    childUrl = instance.getLinkList(lista)

    var1 = instance.setBs4Instance(childUrl[0])
    imgLogo = var1.find(class_='tei-graphic')
    imgLogoSrc = imgLogo['src']
    print(imgLogoSrc)


def format_list_printing(list_to_be_printed: list):

    length_list = [len(element) for row in list_to_be_printed for element in row]  # Finding lengths of row elements

    column_width = max(length_list)  # Longest element sets column_width

    # Printing elements with even spacing
    for row in list_to_be_printed:

        row = "".join(element.ljust(column_width + 2) for element in row)

    return row


def assignment_code():

    data = BeautifulSoup(
                        ScrapFactory.fetch_cached('https://history.state.gov/countries/all'),
                        features="html.parser"
                        )

    instance = ScrapFactory

    instance.BASE_URL = "https://history.state.gov"

    # ALL COUNTRIES URLS OBJECT - THE BASELINE OF THE SCRAPING
    all_countries_urls = instance.getLinkList_modified(data, "/countries/\w+", 0)

    assignment_list = []

    for country_url_item in all_countries_urls:

        country_instance = instance.setBs4Instance(country_url_item)  # B4S object for a country

        # THE NAME OF THE COUNTRY OBJECT
        country_name = (country_instance.find(id="static-title").get_text().split("-")[0])

        assignment_dict = {"country_name": country_name, "country_url": country_url_item}  # A dictionary for each country

        # THE FLAG URL OF THE COUNTRY OBJECT
        country_flag = instance.getImgLink_2(instance.setBs4Instance(country_url_item), 'tei-graphic')
        assignment_dict["country_flag"] = country_flag

        # THE PERSONS NAME AND URL INSIDE THE COUNTRY PAGE OBJECT
        all_people_in_a_page = country_instance.find_all(attrs={"class": "tei-p2"})

        list_of_persons_per_country = []

        for people_name_item in all_people_in_a_page:

            for single_name in people_name_item("a"):  # Going through the subclass to get all the people inside
                # Building the dictionary of the people
                dict_of_person = {"name": single_name.get_text(), "url": instance.BASE_URL + single_name.get('href')}
                list_of_persons_per_country.append(dict_of_person)  # Adding them to a list

            # Adding the list of all people in the global dictionary
            assignment_dict["country_persons"] = list_of_persons_per_country

        assignment_list.append(assignment_dict)  # Appending the country dictionary to the global list

    print(assignment_list)


# Execute all function in standalone mode
if __name__ == '__main__':

    assignment_code()

