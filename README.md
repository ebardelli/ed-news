# ed-news-python

## Overview
This project is a web application built using Python. It serves as a platform for delivering news content and includes features such as dynamic content rendering and an RSS feed.

## Project Structure
```
ed-news-python
├── app.py                # Main entry point of the application
├── requirements.txt      # List of dependencies
├── .gitignore            # Files and directories to ignore by Git
├── templates             # Directory for HTML and RSS templates
│   ├── index.html       # Main HTML template
│   ├── index.html.jinja2 # Jinja2 version of the main HTML template
│   ├── index.rss        # RSS feed template
│   └── index.rss.jinja2  # Jinja2 version of the RSS feed template
├── static                # Directory for static files
│   ├── css              # Directory for CSS files
│   │   └── main.css     # Main CSS styles
│   └── js               # Directory for JavaScript files
│       └── main.js      # Main JavaScript functionality
├── tests                 # Directory for test files
│   └── test_app.py      # Unit tests for the application
└── README.md             # Documentation for the project
```

## Setup Instructions
1. Clone the repository:
   ```
   git clone <repository-url>
   cd ed-news-python
   ```

2. Install the required dependencies:
   ```
   pip install -r requirements.txt
   ```

3. Run the application:
   ```
   python app.py
   ```

## Usage
- Access the web application by navigating to `http://localhost:5000` in your web browser.
- The application supports dynamic content rendering through Jinja2 templates.
- An RSS feed is available at `http://localhost:5000/rss`.

## Cloudflare cache purge (CI)

If you host the site behind Cloudflare you may want to purge the cache automatically after the site is rebuilt.

This repository's Drone CI pipeline includes a `purge cloudflare cache` step which calls the Cloudflare Purge API.

Required Drone secrets:

- `CF_API_TOKEN` — a scoped Cloudflare API token with permission to purge the zone (Zone.Cache Purge: Edit).
- `CF_ZONE_ID` — the Cloudflare Zone ID for your site.

How to create a token:

1. In the Cloudflare dashboard go to My Profile -> API Tokens -> Create Token.
2. Use the `Edit zone cache purge` template or create a custom token with the `Zone.Cache Purge` permission for the target zone.
3. Copy the token and add it to your Drone repository secrets as `CF_API_TOKEN`. Add the zone ID as `CF_ZONE_ID`.

Behavior:

- If the secrets are missing the CI step will skip purging and continue successfully.
- The CI step sends `{"purge_everything":true}` to Cloudflare. If you prefer a more targeted purge (by URL or tag) the step can be adjusted.

## Contributing
Contributions are welcome! Please submit a pull request or open an issue for any enhancements or bug fixes.