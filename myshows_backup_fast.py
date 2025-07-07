#!/usr/bin/env python3
"""
MyShows.me Fast Backup Script
Optimized version with parallel processing for faster exports
"""

import argparse
import csv
import datetime
import getpass
import hashlib
import json
import logging
import sys
import time
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Optional, Any, Union, Tuple

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


# API endpoints
OLD_API_ROOT = 'http://api.myshows.ru'
NEW_API_ROOT = 'https://api.myshows.me/v2/rpc/'
OAUTH_TOKEN_URL = 'https://myshows.me/oauth/token'

# Performance settings
MAX_WORKERS = 5   # Balanced parallel workers for good performance
BATCH_SIZE = 25   # Optimal batch size for processing
REQUEST_DELAY = 0.1  # Balanced delay between requests

# Error handling settings
MAX_503_RETRIES = 5  # Special handling for 503 errors
BACKOFF_DELAY = 1.0  # Initial delay for exponential backoff

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


class OldAPI:
    """Legacy MyShows API v1 client with MD5 authentication"""
    
    def __init__(self, username: str, password: str):
        self.username = username
        self.password = password
        self.session = self._create_session()
        self._authenticated = False
        
    def _create_session(self) -> requests.Session:
        """Create HTTP session with connection pooling and retry"""
        session = requests.Session()
        retry = Retry(
            total=3,
            backoff_factor=0.2,
            status_forcelist=[500, 502, 503, 504]
        )
        adapter = HTTPAdapter(
            max_retries=retry,
            pool_connections=20,  # Increase connection pool
            pool_maxsize=20
        )
        session.mount('http://', adapter)
        session.mount('https://', adapter)
        return session
        
    def _make_request(self, url: str, **kwargs) -> requests.Response:
        """Execute API request with enhanced error handling for 503s"""
        full_url = OLD_API_ROOT + url if not url.startswith('http') else url
        
        for attempt in range(MAX_503_RETRIES):
            try:
                response = self.session.get(full_url, timeout=15, **kwargs)
                response.raise_for_status()
                return response
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 503 and attempt < MAX_503_RETRIES - 1:
                    # Exponential backoff for 503 errors
                    wait_time = BACKOFF_DELAY * (2 ** attempt)
                    logger.warning(f"503 error for {full_url}, retrying in {wait_time:.1f}s (attempt {attempt + 1}/{MAX_503_RETRIES})")
                    time.sleep(wait_time)
                    continue
                else:
                    logger.error(f"Request failed for {full_url}: {e}")
                    raise
            except requests.exceptions.RequestException as e:
                logger.error(f"Request failed for {full_url}: {e}")
                raise
                
        # This should never be reached due to exceptions above
        raise RuntimeError("All retry attempts failed")
            
    def authenticate(self) -> bool:
        """Authenticate user with MD5 hashed password"""
        try:
            password_md5 = hashlib.md5(self.password.encode()).hexdigest()
            url = f'/profile/login?login={self.username}&password={password_md5}'
            response = self._make_request(url)
            self._authenticated = True
            logger.info(f"Successfully authenticated via API v1 for {self.username}")
            return True
        except Exception as e:
            logger.error(f"Authentication failed: {e}")
            return False
            
    def get_all_shows(self) -> Dict[str, Any]:
        """Retrieve all user's TV shows"""
        if not self._authenticated:
            raise RuntimeError("Authentication required")
        
        response = self._make_request('/profile/shows/')
        return response.json()
        
    def get_show_details(self, show_id: int) -> Dict[str, Any]:
        """Get detailed information about a specific show"""
        response = self._make_request(f'/shows/{show_id}')
        return response.json()
        
    def get_watched_episodes(self, show_id: int) -> Dict[str, Any]:
        """Get list of watched episodes for a show"""
        response = self._make_request(f'/profile/shows/{show_id}/')
        return response.json()
        
    def get_show_data_parallel(self, show_id: int, show_info: Dict[str, Any]) -> Tuple[Dict[str, Any], Dict[str, Any], Dict[str, Any]]:
        """Get show details and episodes in parallel"""
        try:
            show_details = self.get_show_details(show_id)
            episodes = self.get_watched_episodes(show_id)
            return show_info, show_details, episodes
        except Exception as e:
            logger.error(f"Failed to fetch data for show {show_id}: {e}")
            raise


class NewAPI:
    """MyShows API v2.0 client with OAuth 2.0 authentication"""
    
    def __init__(self, client_id: str, client_secret: str, 
                 username: Optional[str] = None, password: Optional[str] = None):
        self.client_id = client_id
        self.client_secret = client_secret
        self.username = username
        self.password = password
        self.session = self._create_session()
        self.access_token = None
        self._request_id = 0
        
    def _create_session(self) -> requests.Session:
        """Create HTTP session with connection pooling and retry"""
        session = requests.Session()
        retry = Retry(
            total=3,
            backoff_factor=0.2,
            status_forcelist=[500, 502, 503, 504]
        )
        adapter = HTTPAdapter(
            max_retries=retry,
            pool_connections=20,
            pool_maxsize=20
        )
        session.mount('http://', adapter)
        session.mount('https://', adapter)
        return session
        
    def authenticate(self) -> bool:
        """Obtain OAuth access token"""
        try:
            data = {
                'grant_type': 'password',
                'client_id': self.client_id,
                'client_secret': self.client_secret,
                'username': self.username,
                'password': self.password
            }
            
            response = self.session.post(OAUTH_TOKEN_URL, data=data, timeout=10)
            response.raise_for_status()
            
            token_data = response.json()
            self.access_token = token_data['access_token']
            logger.info(f"Successfully authenticated via OAuth for {self.username}")
            return True
        except Exception as e:
            logger.error(f"OAuth authentication failed: {e}")
            return False
            
    def _make_rpc_request(self, method: str, params: Optional[Dict] = None) -> Any:
        """Execute JSON-RPC request"""
        self._request_id += 1
        
        headers = {
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {self.access_token}'
        }
        
        payload = {
            'jsonrpc': '2.0',
            'method': method,
            'params': params or {},
            'id': self._request_id
        }
        
        for attempt in range(MAX_503_RETRIES):
            try:
                response = self.session.post(
                    NEW_API_ROOT,
                    json=payload,
                    headers=headers,
                    timeout=15
                )
                response.raise_for_status()
                
                result = response.json()
                if 'error' in result:
                    raise Exception(f"RPC Error: {result['error']}")
                    
                return result.get('result')
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 503 and attempt < MAX_503_RETRIES - 1:
                    # Exponential backoff for 503 errors
                    wait_time = BACKOFF_DELAY * (2 ** attempt)
                    logger.warning(f"503 error for {method}, retrying in {wait_time:.1f}s (attempt {attempt + 1}/{MAX_503_RETRIES})")
                    time.sleep(wait_time)
                    continue
                else:
                    logger.error(f"RPC request failed for {method}: {e}")
                    raise
            except Exception as e:
                logger.error(f"RPC request failed for {method}: {e}")
                raise
                
        # This should never be reached due to exceptions above
        raise RuntimeError("All retry attempts failed")
            
    def get_all_shows(self) -> List[Dict[str, Any]]:
        """Retrieve all user's TV shows across all statuses"""
        all_shows = []
        
        # Fetch shows from all statuses in parallel
        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = {}
            for status in ['watching', 'later', 'cancelled', 'completed']:
                future = executor.submit(self._make_rpc_request, 'lists.Shows', {'list': status})
                futures[future] = status
                
            for future in as_completed(futures):
                status = futures[future]
                try:
                    shows = future.result()
                    for show in shows:
                        show['list_status'] = status
                    all_shows.extend(shows)
                except Exception as e:
                    logger.warning(f"Failed to get shows with status {status}: {e}")
                    
        return all_shows
        
    def get_show_details(self, show_id: int) -> Dict[str, Any]:
        """Get detailed information about a specific show"""
        return self._make_rpc_request('shows.GetById', {'showId': show_id})
        
    def get_watched_episodes(self, show_id: int) -> List[Dict[str, Any]]:
        """Get list of watched episodes for a show"""
        return self._make_rpc_request('shows.GetEpisodes', {
            'showId': show_id,
            'isWatched': True
        })
        
    def get_show_data_parallel(self, show_id: int, show_info: Dict[str, Any]) -> Tuple[Dict[str, Any], Dict[str, Any], List[Dict[str, Any]]]:
        """Get show details and episodes in parallel"""
        try:
            show_details = self.get_show_details(show_id)
            episodes = self.get_watched_episodes(show_id)
            return show_info, show_details, episodes
        except Exception as e:
            logger.error(f"Failed to fetch data for show {show_id}: {e}")
            raise


def safe_join_genres(genres: Any) -> str:
    """Convert genres from various formats to comma-separated string"""
    if not genres:
        return ''
    
    if isinstance(genres, list):
        return ', '.join(str(g) for g in genres)
    elif isinstance(genres, dict):
        return ', '.join(str(v) for v in genres.values())
    else:
        return str(genres)


def process_show_data(show_info: Dict[str, Any], 
                     show_details: Dict[str, Any], 
                     episodes: Union[Dict[str, Any], List[Dict[str, Any]]],
                     api_version: str = 'v1') -> Dict[str, Any]:
    """Process and normalize show data from API response"""
    
    # Extract show ID based on API version
    if api_version == 'v2':
        show_id = show_info.get('show', {}).get('id') or show_info.get('id')
    else:
        show_id = show_info.get('showId')
    
    # Build show data structure
    show_data: Dict[str, Any] = {
        'id': show_id,
        'title': show_details.get('title', ''),
        'titleOriginal': show_details.get('titleOriginal', ''),
        'ruTitle': show_details.get('ruTitle', show_details.get('title', '')),
        'year': show_details.get('year', ''),
        'status': show_info.get('list_status', show_info.get('watchStatus', '')),
        'showStatus': show_details.get('status', ''),
        'rating': show_details.get('rating', 'NA'),
        'myRating': show_info.get('rating', 'NA'),
        'imdbId': show_details.get('imdbId', ''),
        'imdbRating': show_details.get('imdbRating', ''),
        'kinopoiskId': show_details.get('kinopoiskId', ''),
        'kinopoiskRating': show_details.get('kinopoiskRating', ''),
        'country': show_details.get('country', ''),
        'network': show_details.get('network', ''),
        'genres': safe_join_genres(show_details.get('genres', [])),
        'totalEpisodes': show_details.get('totalEpisodes', 0),
        'watchedEpisodes': show_info.get('watchedEpisodes', 0),
        'totalSeasons': len(show_details.get('seasons', [])) if 'seasons' in show_details else show_details.get('totalSeasons', 0),
        'runtime': show_details.get('runtime', ''),
        'image': show_details.get('image', ''),
        'description': show_details.get('description', ''),
        'started': show_details.get('started', ''),
        'ended': show_details.get('ended', ''),
        'episodes': []
    }
    
    # Process episodes data
    if episodes:
        if isinstance(episodes, dict):
            episodes_list = list(episodes.values())
        else:
            episodes_list = episodes
            
        for episode in episodes_list:
            watch_date = episode.get('watchDate', episode.get('watchedAt', ''))
            if watch_date:
                try:
                    if 'T' in str(watch_date):
                        watched = datetime.datetime.fromisoformat(watch_date.replace('Z', '+00:00')).date()
                    else:
                        watched = datetime.datetime.strptime(watch_date, '%d.%m.%Y').date()
                    watch_date_iso = watched.isoformat()
                except (ValueError, AttributeError):
                    watch_date_iso = str(watch_date)
            else:
                watch_date_iso = ''
            
            if api_version == 'v1':
                episode_data = show_details.get('episodes', {}).get(str(episode.get('id', '')), {})
                season_num = episode_data.get('seasonNumber', '')
                episode_num = episode_data.get('episodeNumber', '')
                episode_title = episode_data.get('title', '')
            else:
                season_num = episode.get('seasonNumber', episode.get('season', ''))
                episode_num = episode.get('episodeNumber', episode.get('episode', ''))
                episode_title = episode.get('title', '')
            
            episode_info = OrderedDict([
                ('id', episode.get('id', episode.get('episodeId', ''))),
                ('title', episode_title),
                ('season', season_num),
                ('number', episode_num),
                ('airDate', episode.get('airDate', '')),
                ('watched', watch_date_iso),
                ('rating', episode.get('rating', 'NA'))
            ])
            
            show_data['episodes'].append(episode_info)
    
    if show_data['episodes']:
        show_data['episodes'].sort(key=lambda e: e['watched'] or '')
    
    return show_data


def export_to_csv(shows_data: List[Dict[str, Any]], output_file: str, username: str):
    """Export shows data to CSV format"""
    csv_file = output_file.replace('.json', '.csv') if output_file.endswith('.json') else output_file + '.csv'
    
    with open(csv_file, 'w', newline='', encoding='utf-8') as f:
        rows = []
        
        for show in shows_data:
            base_info = {
                'username': username,
                'show_id': show['id'],
                'title': show['title'],
                'title_original': show['titleOriginal'],
                'title_ru': show['ruTitle'],
                'year': show['year'],
                'my_status': show['status'],
                'show_status': show['showStatus'],
                'site_rating': show['rating'],
                'my_rating': show['myRating'],
                'imdb_id': show['imdbId'],
                'imdb_rating': show['imdbRating'],
                'kinopoisk_id': show['kinopoiskId'],
                'kinopoisk_rating': show['kinopoiskRating'],
                'country': show['country'],
                'network': show['network'],
                'genres': show['genres'],
                'total_episodes': show['totalEpisodes'],
                'watched_episodes': show['watchedEpisodes'],
                'total_seasons': show['totalSeasons'],
                'runtime': show['runtime'],
                'started': show['started'],
                'ended': show['ended'],
                'description': show['description'][:200] + '...' if show['description'] and len(show['description']) > 200 else (show['description'] or '')
            }
            
            if show['episodes']:
                first_watched = min(e['watched'] for e in show['episodes'] if e['watched'])
                last_watched = max(e['watched'] for e in show['episodes'] if e['watched'])
                base_info['first_episode_watched'] = first_watched
                base_info['last_episode_watched'] = last_watched
                base_info['days_watching'] = (datetime.datetime.fromisoformat(last_watched) - 
                                            datetime.datetime.fromisoformat(first_watched)).days if first_watched and last_watched else 0
            else:
                base_info['first_episode_watched'] = ''
                base_info['last_episode_watched'] = ''
                base_info['days_watching'] = 0
            
            rows.append(base_info)
        
        if rows:
            fieldnames = list(rows[0].keys())
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
            
    logger.info(f"Full CSV data exported to: {csv_file}")
    
    # Export lightweight CSV with minimal data
    lite_csv_file = output_file.replace('.json', '_lite.csv') if output_file.endswith('.json') else output_file.replace('.csv', '_lite.csv')
    
    with open(lite_csv_file, 'w', newline='', encoding='utf-8') as f:
        lite_rows = []
        
        for show in shows_data:
            lite_info = {
                'title_original': show['titleOriginal'] or show['title'],
                'title_ru': show['ruTitle'] or show['title'],
                'year': show['year'],
                'my_rating': show['myRating'],
                'status': show['status']
            }
            lite_rows.append(lite_info)
        
        if lite_rows:
            fieldnames = ['title_original', 'title_ru', 'year', 'my_rating', 'status']
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(lite_rows)
            
    logger.info(f"Lightweight CSV exported to: {lite_csv_file}")
    
    return csv_file


def process_show_batch(api: Union[OldAPI, NewAPI], show_batch: List[Tuple[Any, Dict[str, Any]]], 
                      api_version: str, progress_offset: int) -> List[Dict[str, Any]]:
    """Process a batch of shows in parallel"""
    batch_results = []
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {}
        
        # Submit show processing tasks with staggered delays
        for i, (show_id, show_info) in enumerate(show_batch):
            # Add staggered delay before submitting each task
            if i > 0:
                time.sleep(REQUEST_DELAY / MAX_WORKERS)
                
            if api_version == 'v1':
                future = executor.submit(api.get_show_data_parallel, show_info['showId'], show_info)
                futures[future] = (show_id, show_info)
            else:
                if isinstance(show_info, dict):
                    show_id_int = show_info.get('show', {}).get('id') or show_info.get('id')
                    if show_id_int:
                        future = executor.submit(api.get_show_data_parallel, int(show_id_int), show_info)
                        futures[future] = (show_id_int, show_info)
        
        # Process completed tasks
        completed = 0
        for future in as_completed(futures):
            completed += 1
            show_id, original_info = futures[future]
            
            try:
                show_info, show_details, episodes = future.result()
                show_data = process_show_data(show_info, show_details, episodes, api_version)
                batch_results.append(show_data)
                
                # Log progress
                if completed % 10 == 0:
                    logger.info(f"Processed {progress_offset + completed} shows...")
                    
            except Exception as e:
                show_title = original_info.get('title', 'Unknown')
                logger.error(f"Failed to process show {show_title}: {e}")
                
            # Additional delay after processing each result  
            time.sleep(REQUEST_DELAY)
    
    return batch_results


def backup_shows_fast(api: Union[OldAPI, NewAPI], output_file: Optional[str] = None, 
                     api_version: str = 'v1') -> List[Dict[str, Any]]:
    """Fast parallel backup of all shows data"""
    
    start_time = time.time()
    
    # Authenticate
    if not api.authenticate():
        raise RuntimeError("Authentication failed. Please check your credentials.")
    
    # Retrieve all shows
    logger.info("Fetching shows list...")
    all_shows = api.get_all_shows()
    
    if not all_shows:
        logger.warning("No shows found for this user")
        return []
    
    # Convert to list of tuples for batch processing
    if api_version == 'v1':
        show_items = list(all_shows.items())
    else:
        show_items = [(i, show) for i, show in enumerate(all_shows)]
    
    total_shows = len(show_items)
    logger.info(f"Found {total_shows} shows. Starting parallel processing...")
    
    # Process shows in batches
    shows_data = []
    for i in range(0, total_shows, BATCH_SIZE):
        batch = show_items[i:i + BATCH_SIZE]
        logger.info(f"Processing batch {i//BATCH_SIZE + 1}/{(total_shows + BATCH_SIZE - 1)//BATCH_SIZE}")
        
        batch_results = process_show_batch(api, batch, api_version, i)
        shows_data.extend(batch_results)
    
    # Sort shows by first watched episode date
    shows_data.sort(key=lambda show: (
        show['episodes'][0]['watched'] if show['episodes'] else '9999-99-99'
    ))
    
    # Calculate processing time
    processing_time = time.time() - start_time
    logger.info(f"Processing completed in {processing_time:.1f} seconds ({processing_time/60:.1f} minutes)")
    logger.info(f"Average time per show: {processing_time/total_shows:.2f} seconds")
    
    # Prepare final result
    username = getattr(api, 'username', 'unknown')
    result = {
        'metadata': {
            'username': username,
            'backup_date': datetime.datetime.now().isoformat(),
            'total_shows': len(shows_data),
            'api_version': api_version,
            'processing_time_seconds': round(processing_time, 2)
        },
        'shows': shows_data
    }
    
    # Save results
    if output_file:
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(result, f, ensure_ascii=False, indent=2)
        logger.info(f"JSON data saved to: {output_file}")
        
        export_to_csv(shows_data, output_file, username)
    else:
        print(json.dumps(result, ensure_ascii=False, indent=2))
    
    return shows_data


def get_api_version(args) -> str:
    """Determine which API version to use"""
    if args.v1:
        return 'v1'
    elif args.v2:
        return 'v2'
    else:
        print("\nSelect API version:")
        print("1. API v1 (legacy, username/password)")
        print("2. API v2 (OAuth)")
        
        while True:
            choice = input("\nEnter 1 or 2: ").strip()
            if choice == '1':
                return 'v1'
            elif choice == '2':
                return 'v2'
            else:
                print("Invalid choice. Please enter 1 or 2.")


def get_credentials_v1() -> tuple:
    """Get username and password for API v1"""
    print("\n=== API v1 Authentication ===")
    username = input("Username: ").strip()
    password = getpass.getpass("Password: ")
    return username, password


def get_credentials_v2() -> tuple:
    """Get OAuth credentials for API v2"""
    print("\n=== API v2 OAuth Authentication ===")
    print("To obtain OAuth credentials, contact api@myshows.me")
    print()
    
    client_id = input("Client ID: ").strip()
    client_secret = getpass.getpass("Client Secret: ")
    username = input("Username: ").strip()
    password = getpass.getpass("Password: ")
    
    return client_id, client_secret, username, password


def main():
    """Entry point"""
    parser = argparse.ArgumentParser(
        description='Fast export of TV shows data from myshows.me',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:

  Interactive mode (select API version):
    python3 myshows_backup_fast.py
    python3 myshows_backup_fast.py -o backup.json
    
  Specify API version:
    python3 myshows_backup_fast.py -v1 -o backup.json
    python3 myshows_backup_fast.py -v2 -o backup.json
    
  Adjust parallel workers and delays for your connection:
    python3 myshows_backup_fast.py -w 5 -d 0.1 -o backup.json  # More aggressive
    python3 myshows_backup_fast.py -w 2 -d 0.5 -o backup.json  # More conservative
    
Performance notes:
  - Default: 5 parallel workers (balanced for speed and stability)
  - Processes shows in batches of 25
  - Includes exponential backoff for 503 errors  
  - Request delay: 0.1s between requests
  - Typically 3-5x faster than sequential version
  - Use -w and -d to adjust if you get 503 errors
        """
    )
    
    parser.add_argument('-o', '--output', help='Output file path (JSON)')
    parser.add_argument('-v', '--verbose', action='store_true', help='Enable verbose output')
    parser.add_argument('-v1', action='store_true', help='Use API v1 (legacy)')
    parser.add_argument('-v2', action='store_true', help='Use API v2 (OAuth)')
    parser.add_argument('-w', '--workers', type=int, default=5, 
                       help='Number of parallel workers (default: 5)')
    parser.add_argument('-d', '--delay', type=float, default=0.1,
                       help='Delay between requests in seconds (default: 0.1)')
    
    args = parser.parse_args()
    
    # Update global settings
    global MAX_WORKERS, REQUEST_DELAY
    MAX_WORKERS = args.workers
    REQUEST_DELAY = args.delay
    
    if args.v1 and args.v2:
        parser.error("Cannot specify both -v1 and -v2")
    
    if args.verbose:
        logger.setLevel(logging.DEBUG)
    
    try:
        api_version = get_api_version(args)
        
        if api_version == 'v1':
            username, password = get_credentials_v1()
            api = OldAPI(username, password)
        else:
            client_id, client_secret, username, password = get_credentials_v2()
            api = NewAPI(client_id, client_secret, username, password)
        
        print(f"\nUsing API {api_version} with {MAX_WORKERS} parallel workers")
        backup_shows_fast(api, args.output, api_version)
        logger.info("Backup completed successfully!")
        
    except KeyboardInterrupt:
        print("\n\nOperation cancelled by user")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Backup failed: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main() 