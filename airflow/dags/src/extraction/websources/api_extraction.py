import logging
import requests
import pandas as pd
from .request_token import accessToken

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s - %(message)s")

class apiExtraction:
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.base_url = 'https://api.spotify.com/v1/'
        self.access_token = None
        self.session = None
    
    def __enter__(self):
        self.logger.info("Initializing API extraction context")
        try:
            token_data = accessToken().request_access_token()
            self.access_token = token_data['access_token']
            self.session = requests.Session()
            self.session.headers.update(self.get_headers())
            self.logger.info("API extraction context initialized successfully")
            return self
        except Exception as e:
            self.logger.error(f"Failed to initialize API extraction context: {str(e)}")
            raise
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            self.logger.error(f"Exception occurred in context: {exc_type.__name__}: {exc_val}")
        else:
            self.logger.info("API extraction context completed successfully")
        
        if self.session:
            self.session.close()
            self.logger.info("HTTP session closed")
        
        return False

    def get_headers(self):
        return {
            'Authorization': f'Bearer {self.access_token}' 
        }
        
    def fetch_data(self, endpoint):
        if not self.session:
            raise RuntimeError("Context manager not properly initialized")
            
        url = self.base_url + endpoint
        
        response = self.session.get(url)
        if response.status_code == 200:
            self.logger.info(f"Successfully fetched data from {endpoint}")
            return response.json()
        else:
            error_msg = f"Failed to fetch data: {response.status_code} - {response.text}"
            self.logger.error(error_msg)
            raise Exception(error_msg)

class newAlbumsExtraction(apiExtraction):
    def __init__(self):
        super().__init__()
        self.logger = logging.getLogger(self.__class__.__name__)

    def extract_albums_data(self, limit: int = 50, max_pages: int = None):
        self.logger.info("Starting album data extraction")
        content = self.fetch_data('browse/new-releases')

        albums_data = {
            "album_id": [],
            "name": [],
            "album_type": [],
            "release_date": [],
            "total_tracks": [],
            "artists_id": []
        }

        offset = 0
        page_count = 0

        while True:
            endpoint = f"browse/new-releases?limit={limit}&offset={offset}"
            content = self.fetch_data(endpoint)

            albums_items = content.get("albums", {}).get("items", [])
            self.logger.info(f"Processing {len(albums_items)} albums at offset {offset}")

            for album in albums_items:
                albums_data["album_id"].append(album.get("id"))
                albums_data["name"].append(album.get("name"))
                albums_data["album_type"].append(album.get("album_type"))
                albums_data["release_date"].append(album.get("release_date"))
                albums_data["total_tracks"].append(album.get("total_tracks"))
                albums_data["artists_id"].append(
                    album.get("artists", [{}])[0].get("id")
                )

            next_url = content.get("albums", {}).get("next")
            if not next_url:
                break

            offset += limit
            page_count += 1

            if max_pages and page_count >= max_pages:
                self.logger.warning(f"Reached max_pages={max_pages}, stopping early")
                break

        df = pd.DataFrame(albums_data)
        self.logger.info(f"Album extraction completed - {len(df)} total records created")
        return df

class categoriesExtraction(apiExtraction):
    def __init__(self):
        super().__init__()
        self.logger = logging.getLogger(self.__class__.__name__)

    def extract_categories_data(self, limit=50, max_pages=None):
        self.logger.info("Starting category data extraction")
        content = self.fetch_data('browse/categories')

        categories_data = {
            'category_id': [],
            'name': [],
            'href': []
        }

        offset = 0
        page_count = 0

        while True:
            endpoint = f'browse/categories?limit={limit}&offset={offset}'
            content = self.fetch_data(endpoint)

            categories_items = content.get("categories", {}).get("items", [])
            if not categories_items:
                break

            self.logger.info(f"Processing {len(categories_items)} categories at offset {offset}")

            for category in categories_items:
                categories_data['category_id'].append(category.get("id"))
                categories_data['name'].append(category.get("name"))
                categories_data['href'].append(category.get("href"))

            offset += limit
            page_count += 1
            total = content.get("categories", {}).get("total", 0)

            if offset >= total:
                break
            if max_pages and page_count >= max_pages:
                self.logger.info(f"Reached max_pages={max_pages}, stopping early")
                break

            df = pd.DataFrame(categories_data)
            self.logger.info(f"Category extraction completed - {len(df)} records created")
            return df

class albumTrackExtraction(apiExtraction):
    def __init__(self):
        super().__init__()
        self.logger = logging.getLogger(self.__class__.__name__)

    def extract_album_track(self, album_id: str, market: str = None):
        self.logger.info("Fetching album tracks")
        tracks_data = {
            "album_id": [],
            "track_id": [],
            "name": [],
            "track_number": [],
            "disc_number": [],
            "duration_ms": [],
            "explicit": [],
            "popularity": [],
            "preview_url": [],
            "uri": [],
            "artists": [],
            "artist_ids": []
        }

        # pagination loop
        offset, limit = 0, 50
        while True:
            endpoint = f"albums/{album_id}/tracks?limit={limit}&offset={offset}"
            if market:
                endpoint += f"&market={market}"

            content = self.fetch_data(endpoint)
            items = content.get("items", [])
            if not items:
                break

            for track in items:
                tracks_data["album_id"].append(album_id)
                tracks_data["track_id"].append(track.get("id"))
                tracks_data["name"].append(track.get("name"))
                tracks_data["track_number"].append(track.get("track_number"))
                tracks_data["disc_number"].append(track.get("disc_number"))
                tracks_data["duration_ms"].append(track.get("duration_ms"))
                tracks_data["explicit"].append(track.get("explicit"))
                tracks_data["popularity"].append(None)  # not returned in this endpoint
                tracks_data["preview_url"].append(track.get("preview_url"))
                tracks_data["uri"].append(track.get("uri"))
                tracks_data["artists"].append([artist.get("name") for artist in track.get("artists", [])])
                tracks_data["artist_ids"].append([artist.get("id") for artist in track.get("artists", [])])

            offset += limit
            if content.get("next") is None:
                break

        tracks_df = pd.DataFrame(tracks_data)
        self.logger.info(f"Extracted {len(tracks_df)} tracks for album {album_id}")

        return tracks_df

class artistsExtraction(apiExtraction):
    def __init__(self):
        super().__init__()
        self.logger = logging.getLogger(self.__class__.__name__)

    def extract_artist_data(self, artist_id: str):
        artist_data = {
            "artist_id": [],
            "name": [],
            "popularity": [],
            "followers_total": [],
            "genres": [],
            "external_urls_spotify": [],
            "href": [],
            "uri": [],
            "type": [],
            "image_url": [],
            "image_height": [],
            "image_width": [],
            "genres_count": [],
            "images_count": []
        }
        endpoint = f"artists/{artist_id}"
        content = self.fetch_data(endpoint)
 
        if content is None:
            self.logger.warning(f"No data returned for artist {artist_id}")
        
        artist_items = [content]  
       
        for artist in artist_items:
            artist_data["artist_id"].append(artist.get("id"))
            artist_data["name"].append(artist.get("name"))
            artist_data["popularity"].append(artist.get("popularity"))
            artist_data["href"].append(artist.get("href"))
            artist_data["uri"].append(artist.get("uri"))
            artist_data["type"].append(artist.get("type"))
            
            # External URLs
            external_urls = artist.get("external_urls", {})
            artist_data["external_urls_spotify"].append(external_urls.get("spotify"))
            
            # Followers
            followers = artist.get("followers", {})
            artist_data["followers_total"].append(followers.get("total", 0))
            
            # Genres
            genres = artist.get("genres", [])
            artist_data["genres"].append(genres)
            artist_data["genres_count"].append(len(genres))
            
            # Images
            images = artist.get("images", [])
            artist_data["images_count"].append(len(images))
            if images:
                artist_data["image_url"].append(images[0].get("url"))
                artist_data["image_height"].append(images[0].get("height"))
                artist_data["image_width"].append(images[0].get("width"))
            else:
                artist_data["image_url"].append(None)
                artist_data["image_height"].append(None)
                artist_data["image_width"].append(None)

        df = pd.DataFrame(artist_data)
        self.logger.info(f"Artists extraction completed - {len(df)} total records created")
        return df