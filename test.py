import dotenv as e
import os
import requests

# Load environment variables from .env
e.load_dotenv()

api_key = os.getenv("API_KEY")
base_url = os.getenv("SPORTS_URL")

sport = "upcoming"

response = requests.get(
    url=base_url + '/' + sport + '/odds', 
                            params={
                                'api_key': api_key,
                                'regions': 'us',
                                'markets': 'h2h'
                                }
                        )

for line in response.json():
    if line['sport_title'] == 'NFL':
        print('sport_title=' + line['sport_title'])
        print(line['home_team'] + ' vs ' + line['away_team'])
        print('\n')
    