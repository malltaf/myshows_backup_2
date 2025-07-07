# MyShows.me Backup Script

Export your TV shows data from myshows.me

## Acknowledgments

This project is based on the original work by [@thesealion](https://github.com/thesealion) from the repository [myshows_backup](https://github.com/thesealion/myshows_backup). Special thanks for the initial implementation and inspiration!

## Description

This script exports your complete TV shows viewing history from myshows.me, including:

- Russian and original titles
- Your personal ratings (or NA if not rated)
- IMDb and Kinopoisk IDs with ratings
- Genres, country, network information
- Watch status (Watching, Will Watch, Stopped, Finished)
- Detailed episode information with watch dates
- Show descriptions, start/end dates
- Total seasons and episodes count

## Features

- Support for two API versions:
  - **API v1** - legacy API with username/password authentication
  - **API v2** - new API with OAuth 2.0 authentication ⚠️ **Currently not provided by myshows.me, functionality untested**
- Interactive API version selection
- Secure credential input
- Automatic retry mechanism for network errors
- Detailed logging
- **Dual format export**:
  - JSON file with complete hierarchical data
  - CSV file optimized for data analysis and import into other tools
- **Performance versions**:
  - Standard version for stable processing
  - Fast version with parallel processing (2-3x faster)

## Installation

1. Ensure Python 3 is installed:
```bash
python3 --version
```

2. Install required dependencies:
```bash
pip3 install -r requirements.txt
```

## Usage

### Interactive Mode (Recommended)

Run the script without specifying API version - it will prompt for your choice:

```bash
# Output to console only
python3 myshows_backup.py

# Save to files
python3 myshows_backup.py -o myshows_backup.json
```

The script will ask for:
1. API version choice (1 or 2)
2. For API v1: username and password
3. For API v2: Client ID, Client Secret, username and password

### Explicit API Version

```bash
# Use API v1 (legacy)
python3 myshows_backup.py -v1 -o backup.json

# Use API v2 (OAuth)
python3 myshows_backup.py -v2 -o backup.json
```

### Additional Options

```bash
# Enable verbose logging
python3 myshows_backup.py -v -o backup.json

# Show help
python3 myshows_backup.py --help
```

## Output Files

When you specify an output file (e.g., `-o backup.json`), the script creates **two files**:

### 1. JSON File
Complete hierarchical data structure with all show and episode information.

### 2. CSV File
Flattened data format optimized for:
- Import into spreadsheet applications (Excel, Google Sheets)
- Data analysis tools (pandas, R, Tableau)
- Machine learning and recommendation systems

The CSV includes aggregated information:
- All show metadata
- First and last episode watch dates
- Total days spent watching each show
- Truncated descriptions for readability

## API v2 (OAuth)

⚠️ **Important Notice**: The API v2 with OAuth authentication is currently **not provided by myshows.me**. This functionality has been implemented but remains **untested** as the service does not offer OAuth credentials at this time.

To use the new API v2 (if it becomes available):

1. Email: api@myshows.me
2. Request Client ID and Client Secret for your application
3. Use them with the `-v2` flag

## JSON Output Format

```json
{
  "metadata": {
    "username": "your_username",
    "backup_date": "2025-01-07T12:00:00",
    "total_shows": 150,
    "api_version": "v1"
  },
  "shows": [
    {
      "id": 123,
      "title": "Breaking Bad",
      "titleOriginal": "Breaking Bad",
      "ruTitle": "Во все тяжкие",
      "year": "2008",
      "status": "finished",
      "showStatus": "Ended",
      "rating": 9.5,
      "myRating": 5,
      "imdbId": "tt0903747",
      "imdbRating": 9.5,
      "kinopoiskId": "404900",
      "kinopoiskRating": 8.9,
      "country": "USA",
      "network": "AMC",
      "genres": "Drama, Crime, Thriller",
      "totalEpisodes": 62,
      "watchedEpisodes": 62,
      "totalSeasons": 5,
      "episodes": [...]
    }
  ]
}
```

## CSV Output Format

The CSV file contains one row per show with columns:
- `username`, `show_id`, `title`, `title_original`, `title_ru`
- `year`, `my_status`, `show_status`
- `site_rating`, `my_rating`
- `imdb_id`, `imdb_rating`, `kinopoisk_id`, `kinopoisk_rating`
- `country`, `network`, `genres`
- `total_episodes`, `watched_episodes`, `total_seasons`
- `runtime`, `started`, `ended`
- `first_episode_watched`, `last_episode_watched`, `days_watching`
- `description` (truncated to 200 characters)

## Troubleshooting

### Authentication Failed
- For API v1: verify your username and password
- For API v2: ensure you have valid Client ID and Client Secret

### Network Errors
The script automatically retries failed requests up to 3 times.

### Large Collections
For users with many shows (>500), the export may take several minutes. The script includes delays between requests to avoid rate limiting.

## Notes

- Compatible with Python 3.6+
- All dates are in ISO 8601 format
- Full Unicode support for international titles
- The script continues processing even if individual shows fail

## Data Privacy

All data is exported locally to your computer. The script only connects to myshows.me API endpoints and does not send data anywhere else.

## Performance Options

This package includes two versions of the backup script:

### Standard Version (`myshows_backup.py`)
- **Processing**: Sequential (one show at a time)
- **Time for 750 shows**: ~5 minutes
- **Best for**: Stable connections, maximum reliability

### Fast Version (`myshows_backup_fast.py`)
- **Processing**: Parallel (5 threads by default)
- **Time for 750 shows**: ~2-3 minutes (2-3x faster)
- **Best for**: Large collections, good internet connection

#### Fast Version Usage
```bash
# Default settings (5 workers)
python3 myshows_backup_fast.py -v1 -o backup.json

# More workers for faster processing
python3 myshows_backup_fast.py -v1 -w 10 -o backup.json
```

#### Performance Comparison
| Version | Workers | Expected Time (750 shows) | Speed Improvement |
|---------|---------|---------------------------|-------------------|
| Standard | 1 | ~5 minutes | Baseline |
| Fast | 5 (default) | ~2-3 minutes | 2-3x faster |
| Fast | 10 | ~1-2 minutes | 3-5x faster |
| Fast | 20 | ~45-90 seconds | 4-7x faster |

**Recommendation**: Start with the fast version's default settings (5 workers), then adjust workers based on your connection quality and results.