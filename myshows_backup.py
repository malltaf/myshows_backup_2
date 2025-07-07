#!/usr/bin/env python3
"""
MyShows.me Backup Script
Export your TV shows data from myshows.me
Supports both legacy API v1 and OAuth-based API v2
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
from typing import Dict, List, Optional, Any, Union

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


# API endpoints
OLD_API_ROOT = 'http://api.myshows.ru'
NEW_API_ROOT = 'https://api.myshows.me/v2/rpc/'
OAUTH_TOKEN_URL = 'https://myshows.me/oauth/token'

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
        """Create HTTP session with retry mechanism"""
        session = requests.Session()
        retry = Retry(
            total=3,
            backoff_factor=0.3,
            status_forcelist=[500, 502, 503, 504]
        )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount('http://', adapter)
        session.mount('https://', adapter)
        return session
        
    def _make_request(self, url: str, **kwargs) -> requests.Response:
        """Execute API request with error handling"""
        full_url = OLD_API_ROOT + url if not url.startswith('http') else url
        try:
            response = self.session.get(full_url, timeout=30, **kwargs)
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed for {full_url}: {e}")
            raise
            
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
        """Create HTTP session with retry mechanism"""
        session = requests.Session()
        retry = Retry(
            total=3,
            backoff_factor=0.3,
            status_forcelist=[500, 502, 503, 504]
        )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount('http://', adapter)
        session.mount('https://', adapter)
        return session
        
    def authenticate(self) -> bool:
        """Obtain OAuth access token"""
        try:
            # Resource Owner Password Credentials Grant
            data = {
                'grant_type': 'password',
                'client_id': self.client_id,
                'client_secret': self.client_secret,
                'username': self.username,
                'password': self.password
            }
            
            response = self.session.post(OAUTH_TOKEN_URL, data=data, timeout=30)
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
        
        try:
            response = self.session.post(
                NEW_API_ROOT,
                json=payload,
                headers=headers,
                timeout=30
            )
            response.raise_for_status()
            
            result = response.json()
            if 'error' in result:
                raise Exception(f"RPC Error: {result['error']}")
                
            return result.get('result')
        except Exception as e:
            logger.error(f"RPC request failed for {method}: {e}")
            raise
            
    def get_all_shows(self) -> List[Dict[str, Any]]:
        """Retrieve all user's TV shows across all statuses"""
        all_shows = []
        
        for status in ['watching', 'later', 'cancelled', 'completed']:
            try:
                shows = self._make_rpc_request('lists.Shows', {'list': status})
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


def safe_join_genres(genres: Any) -> str:
    """Convert genres from various formats to comma-separated string"""
    if not genres:
        return ''
    
    # Convert all elements to strings
    if isinstance(genres, list):
        return ', '.join(str(g) for g in genres)
    elif isinstance(genres, dict):
        # Handle genres as dictionary {id: name}
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
            # Legacy API returns dictionary
            episodes_list = list(episodes.values())
        else:
            # New API returns list
            episodes_list = episodes
            
        for episode in episodes_list:
            # Parse watch date
            watch_date = episode.get('watchDate', episode.get('watchedAt', ''))
            if watch_date:
                try:
                    if 'T' in str(watch_date):  # ISO format
                        watched = datetime.datetime.fromisoformat(watch_date.replace('Z', '+00:00')).date()
                    else:  # Legacy format dd.mm.yyyy
                        watched = datetime.datetime.strptime(watch_date, '%d.%m.%Y').date()
                    watch_date_iso = watched.isoformat()
                except (ValueError, AttributeError):
                    watch_date_iso = str(watch_date)
            else:
                watch_date_iso = ''
            
            # Extract episode information
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
    
    # Sort episodes by watch date
    if show_data['episodes']:
        show_data['episodes'].sort(key=lambda e: e['watched'] or '')
    
    return show_data


def export_to_csv(shows_data: List[Dict[str, Any]], output_file: str, username: str):
    """Export shows data to CSV format for easy import into other tools"""
    csv_file = output_file.replace('.json', '.csv') if output_file.endswith('.json') else output_file + '.csv'
    
    with open(csv_file, 'w', newline='', encoding='utf-8') as f:
        # Prepare flattened data for CSV
        rows = []
        
        for show in shows_data:
            # Base show information
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
            
            # Add aggregated episode information
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
        
        # Write CSV
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


def backup_shows(api: Union[OldAPI, NewAPI], output_file: Optional[str] = None, 
                api_version: str = 'v1') -> List[Dict[str, Any]]:
    """Main function to create backup of all shows data"""
    
    # Authenticate
    if not api.authenticate():
        raise RuntimeError("Authentication failed. Please check your credentials.")
    
    # Retrieve all shows
    logger.info("Fetching shows list...")
    all_shows = api.get_all_shows()
    
    if not all_shows:
        logger.warning("No shows found for this user")
        return []
    
    shows_data = []
    
    # Process shows based on API version
    if api_version == 'v1':
        # v1 returns dict
        total = len(all_shows)
        for index, (show_id, show_info) in enumerate(all_shows.items(), 1):
            try:
                logger.info(f"Processing show {index}/{total}: {show_info.get('title', 'Unknown')}")
                
                show_details = api.get_show_details(show_info['showId'])
                episodes = api.get_watched_episodes(show_info['showId'])
                
                show_data = process_show_data(show_info, show_details, episodes, api_version)
                shows_data.append(show_data)
                
                # Small delay between requests to avoid rate limiting
                time.sleep(0.1)
                
            except Exception as e:
                logger.error(f"Failed to process show {show_id}: {e}")
                continue
    else:
        total = len(all_shows)
        for index, show_info in enumerate(all_shows, 1):
            try:
                # v2 returns list of dicts
                if isinstance(show_info, dict):
                    show_id = show_info.get('show', {}).get('id') or show_info.get('id')
                    show_title = show_info.get('show', {}).get('title') or show_info.get('title', 'Unknown')
                else:
                    logger.error(f"Unexpected show_info type: {type(show_info)}")
                    continue
                
                if not show_id:
                    logger.error(f"No show ID found for {show_title}")
                    continue
                    
                logger.info(f"Processing show {index}/{total}: {show_title}")
                
                show_details = api.get_show_details(int(show_id))
                episodes = api.get_watched_episodes(int(show_id))
                
                show_data = process_show_data(show_info, show_details, episodes, api_version)
                shows_data.append(show_data)
                
                # Small delay between requests to avoid rate limiting
                time.sleep(0.1)
                
            except Exception as e:
                logger.error(f"Failed to process show: {e}")
                continue
    
    # Sort shows by first watched episode date
    shows_data.sort(key=lambda show: (
        show['episodes'][0]['watched'] if show['episodes'] else '9999-99-99'
    ))
    
    # Prepare final result
    username = getattr(api, 'username', 'unknown')
    result = {
        'metadata': {
            'username': username,
            'backup_date': datetime.datetime.now().isoformat(),
            'total_shows': len(shows_data),
            'api_version': api_version
        },
        'shows': shows_data
    }
    
    # Save results
    if output_file:
        # Save JSON
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(result, f, ensure_ascii=False, indent=2)
        logger.info(f"JSON data saved to: {output_file}")
        
        # Export CSV for data analysis
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
        # Interactive selection
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
        description='Export TV shows data from myshows.me',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:

  Interactive mode (select API version):
    python3 myshows_backup.py
    python3 myshows_backup.py -o backup.json
    
  Specify API version:
    python3 myshows_backup.py -v1 -o backup.json
    python3 myshows_backup.py -v2 -o backup.json
    
  Enable verbose output:
    python3 myshows_backup.py -v -o backup.json
    
Output files:
  - JSON file with complete data structure
  - CSV file for easy import into data analysis tools
        """
    )
    
    parser.add_argument('-o', '--output', help='Output file path (JSON)')
    parser.add_argument('-v', '--verbose', action='store_true', help='Enable verbose output')
    parser.add_argument('-v1', action='store_true', help='Use API v1 (legacy)')
    parser.add_argument('-v2', action='store_true', help='Use API v2 (OAuth)')
    
    args = parser.parse_args()
    
    # Validate arguments
    if args.v1 and args.v2:
        parser.error("Cannot specify both -v1 and -v2")
    
    # Configure logging level
    if args.verbose:
        logger.setLevel(logging.DEBUG)
    
    try:
        # Determine API version
        api_version = get_api_version(args)
        
        # Get credentials and create API client
        if api_version == 'v1':
            username, password = get_credentials_v1()
            api = OldAPI(username, password)
        else:
            client_id, client_secret, username, password = get_credentials_v2()
            api = NewAPI(client_id, client_secret, username, password)
        
        # Execute backup
        print(f"\nUsing API {api_version}")
        backup_shows(api, args.output, api_version)
        logger.info("Backup completed successfully!")
        
    except KeyboardInterrupt:
        print("\n\nOperation cancelled by user")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Backup failed: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main() 