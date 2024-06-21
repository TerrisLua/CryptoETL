# crypto_data_extraction.py
import requests
import json
import os
from dotenv import load_dotenv
from datetime import datetime


def fetch_crypto_data():
    load_dotenv()
    api_key = os.getenv('CMC_API_KEY')
    url = 'https://pro-api.coinmarketcap.com/v1/cryptocurrency/category'
    parameters = {
    'id' : '605e2ce9d41eae1066535f7c',
    'start':'1',
    'limit':'100',
    'convert':'USD'
    }
    headers = {
    'Accepts': 'application/json',
    'X-CMC_PRO_API_KEY': api_key,
    }
    
    crypto_list = []
    response = requests.get(url, params=parameters, headers=headers)
    data = response.json()
    current_date = datetime.utcnow().strftime('%Y-%m-%d')
    
    if response.status_code != 200:
        print(f"Error fetching data: {data['status']['error_message']}")
        return []
                    
    for crypto in data['data']['coins']:
            # Extract relevant information
            name = crypto['name']
            symbol = crypto['symbol']
            price = crypto['quote']['USD']['price']
            percent_change_1h  = crypto['quote']['USD']['percent_change_1h']
            percent_change_24h = crypto['quote']['USD']['percent_change_24h']
            crypto_list.append({
                            'name': name,
                            'symbol': symbol,
                            'price': price,
                            'percent_change_1h' : percent_change_1h,
                            'percent_change_24h' : percent_change_24h,
                            'date': current_date
                        })
    return crypto_list

if __name__ == "__main__":
    data = fetch_crypto_data()
    with open('crypto_data.json', 'w') as f:
        json.dump(data, f)