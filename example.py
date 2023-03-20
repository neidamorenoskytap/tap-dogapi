import requests

response = requests.get("https://api.thedogapi.com/v1/breeds", headers={
    "x-api-key":"live_bd174ZtWujCdSN7gf97gZjHWfApKYRkbNe7gRF90UiXwy9xTosTbPTgAPdfDQiaC"
})
print(response.json())