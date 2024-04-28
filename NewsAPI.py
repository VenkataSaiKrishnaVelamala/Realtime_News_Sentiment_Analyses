# %%
import requests
import time

# Your NewsAPI key
API_KEY = 'df7132d10d34451fa7a40adb7f8016a1'

# List of countries to fetch headlines from
countries = ['us', 'gb', 'ca', 'in', 'jp', 'it']  # Example: United States, United Kingdom, Canada

# NewsAPI endpoint for top headlines
url = 'https://newsapi.org/v2/top-headlines'

# Continuously stream data
while True:
    for country in countries:
        print(f"Headlines from {country.upper()}:")
        print("-" * 50)

        # Parameters for the request
        params = {
            'country': country,
            'language': 'en',  # Set language to English
            'apiKey': API_KEY
        }

        # Send the GET request to NewsAPI
        response = requests.get(url, params=params)

        # Check if the request was successful
        if response.status_code == 200:
            # Parse the JSON response
            data = response.json()

            # Extract and print the headlines and complete articles
            for article in data['articles']:
                print("Headline:", article['title'])
                print("Description:", article['description'])
                print("Content:", article['content'])
                print("-" * 50)  # Separating articles with a line
        else:
            print(f'Error fetching headlines for {country.upper()}:', response.status_code)

        print("\n")

    # Add a delay between each iteration to avoid overwhelming the NewsAPI server
    time.sleep(60)  # Wait for 60 seconds before making the next request


# %%
import requests
import time
from bs4 import BeautifulSoup
from confluent_kafka import Producer
import json

# %%
! pip3 install beautifulsoup4 requests lxml


# %%
import requests
from bs4 import BeautifulSoup
import time

# Your NewsAPI key
API_KEY = 'df7132d10d34451fa7a40adb7f8016a1'  # Replace with your actual API key

# List of countries to fetch headlines from
countries = ['us', 'gb', 'ca', 'in', 'jp', 'it']

# NewsAPI endpoint for top headlines
url = 'https://newsapi.org/v2/top-headlines'




def scrape_article_content(article_url):
    try:
        response = requests.get(article_url)
        response.raise_for_status()  # Raise an exception for HTTP errors
        soup = BeautifulSoup(response.text, 'lxml')
        
        # Find the article body - Adjust this selector according to the site's layout
        # This is a generic selector; you need to customize it for the websites you are scraping
        article_body = soup.find('article')
        if article_body:
            return ' '.join([p.get_text(strip=True) for p in article_body.find_all('p')])
        else:
            return "Article body not found."
    except requests.RequestException as e:
        return f"Failed to retrieve article from {article_url}. Error: {e}"



# %%


# %%
KAFKA_TOPIC = 'newsData'
KAFKA_BROKERS = 'pkc-56d1g.eastus.azure.confluent.cloud:9092'  # Change to your Confluent Cloud broker address
KAFKA_API_KEY = 'PE2K3XKMKQMBKPUS'
KAFKA_API_SECRET = '7IiXWeafLDGoUaUVsrGxjYJR49f1VxDqMmTVV2wNWcu/55dM6Pv/PYZUfxViWEPe'

# %%
producer = Producer({
    'bootstrap.servers': KAFKA_BROKERS,
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': KAFKA_API_KEY,
    'sasl.password': KAFKA_API_SECRET
})

# %%
def delivery_report(err, msg):
    if err is not None:
        print('Message delivery failed:', err)
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

# %%
def send_to_kafka(article):
    # Convert article data to a JSON string and send it to Kafka
    producer.produce(KAFKA_TOPIC, key=str(article.get('id', None)), value=json.dumps(article), callback=delivery_report)
    # Poll to process delivery reports
    producer.poll(0)

# %%
def flush_messages():
    producer.flush()


# %%
# Continuously stream data
while True:
    for country in countries:
        print(f"Headlines from {country.upper()}:")
        print("-" * 50)

        # Parameters for the request
        params = {
            'country': country,
            'language': 'en',
            'apiKey': API_KEY
        }

        # Send the GET request to NewsAPI
        response = requests.get(url, params=params)

        # Check if the request was successful
        if response.status_code == 200:
            data = response.json()

            # Extract and process each article
            for article in data['articles']:
                print("Headline:", article['title'])
                print("URL:", article['url'])
                
                # Scrape the full content of the article
                full_content = scrape_article_content(article['url'])
                print("Full Content:", full_content)
                print("-" * 50)
                
                # Prepare the article data for Kafka
                article_data = {
                    'id': article.get('source', {}).get('id', None),  # or any other unique identifier
                    'headline': article['title'],
                    'url': article['url'],
                    'content': full_content
                }
                
                # Send the data to Kafka
                send_to_kafka(article_data)
                print("Data sent to Kafka:", article_data)
                print("-" * 50)
        else:
            print(f'Error fetching headlines for {country.upper()}:', response.status_code)

        print("\n")

    # Delay to avoid overwhelming the NewsAPI server and to respect rate limits
    time.sleep(60)



