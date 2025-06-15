import requests
          
client_id = 'af50a3a1327a11ef749c1f0f1c68dd50'
client_secret = 'ZWM2YTVjNGMtYzA2NC00YTlhLWE5MDYtMWQwMmI5OTQwNTYz'
token_url = 'https://api.getcarro.com/oauth'
token_payload = {
              'client_id': client_id,
              'client_secret': client_secret
              }
headers = {"Content-Type": "application/json; charset=utf-8"}
token_response = requests.post(token_url, headers=headers, json=token_payload)
access_token = token_response.json()['access_token']
print(access_token)