import requests
import os

def my_test():
    API_KEY = os.getenv("TMDB_API_KEY")  # store your key in .env

    url = f"https://api.themoviedb.org/3/movie/popular?api_key={API_KEY}&language=en-US&page=1"
    resp = requests.get(url)
    resp.raise_for_status()
    data = resp.json()

    for movie in data["results"][:5]:
        print(movie["title"], movie["release_date"], movie["popularity"])
