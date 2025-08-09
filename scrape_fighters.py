import time
import string
import requests
from bs4 import BeautifulSoup
import pandas as pd
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

BASE_URL_HTTP = "http://ufcstats.com/statistics/fighters?char={}&page=all"
BASE_URL_WWW_HTTP = "http://www.ufcstats.com/statistics/fighters?char={}&page=all"
BASE_URL_HTTPS = "https://ufcstats.com/statistics/fighters?char={}&page=all"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/124.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

def create_retry_session() -> requests.Session:
    retry_strategy = Retry(
        total=3,
        connect=3,
        read=3,
        backoff_factor=0.8,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    sess = requests.Session()
    sess.mount("http://", adapter)
    sess.mount("https://", adapter)
    return sess

session = create_retry_session()

def scrape_letter(letter: str):
    # Prefer HTTP first (HTTPS is refused on this machine), then try www host, then HTTPS last
    candidate_urls = [
        BASE_URL_HTTP.format(letter),
        BASE_URL_WWW_HTTP.format(letter),
        BASE_URL_HTTPS.format(letter),
    ]

    response_text = None
    used_url = None
    last_exception = None

    for url in candidate_urls:
        print(f"[INFO] Scraping: {url}")
        try:
            resp = session.get(url, headers=HEADERS, timeout=30)
            if resp.status_code == 200:
                response_text = resp.text
                used_url = url
                break
            print(f"[WARN] Status {resp.status_code} for {url}")
        except requests.exceptions.RequestException as exc:
            last_exception = exc
            print(f"[WARN] Request failed for {url}: {exc}")
            time.sleep(0.5)

    if response_text is None:
        if last_exception is not None:
            print(f"[ERROR] All attempts failed for {letter}: {last_exception}")
        else:
            print(f"[ERROR] No successful response for {letter}")
        return []

    soup = BeautifulSoup(response_text, "html.parser")
    table = soup.find("table", class_="b-statistics__table")
    if not table:
        print(f"[WARN] No table found for {letter}. Writing debug HTML.")
        with open(f"debug_{letter}.html", "w", encoding="utf-8") as f:
            f.write(response_text)
        return []

    tbody = table.find("tbody")
    if not tbody:
        print(f"[WARN] No <tbody> for {letter}")
        return []

    rows = tbody.find_all("tr")
    fighters = []
    
    for i, row in enumerate(rows):
        data_cells = row.find_all("td")
        
        # Need at least 10 cells for complete data
        if len(data_cells) < 10:
            print(f"[DEBUG] Row {i}: Only {len(data_cells)} cells, skipping")
            continue

        # Extract data according to actual table structure
        first_name = data_cells[0].get_text(strip=True)
        last_name = data_cells[1].get_text(strip=True)
        nickname = data_cells[2].get_text(strip=True)
        height = data_cells[3].get_text(strip=True)
        weight = data_cells[4].get_text(strip=True)
        reach = data_cells[5].get_text(strip=True)
        stance = data_cells[6].get_text(strip=True)
        wins_text = data_cells[7].get_text(strip=True)
        losses_text = data_cells[8].get_text(strip=True)
        draws_text = data_cells[9].get_text(strip=True)

        # Combine first and last name
        full_name = f"{first_name} {last_name}".strip()
        
        # Skip if no meaningful name
        if not full_name or len(full_name) < 2:
            print(f"[DEBUG] Row {i}: No valid name '{full_name}', skipping")
            continue

        def to_int(text_value: str):
            value = text_value.strip()
            return int(value) if value.isdigit() else None

        fighter_data = {
            "first_name": first_name,
            "last_name": last_name,
            "full_name": full_name,
            "nickname": nickname,
            "height": height,
            "weight": weight,
            "reach": reach,
            "stance": stance,
            "wins": to_int(wins_text),
            "losses": to_int(losses_text),
            "draws": to_int(draws_text),
        }
        
        fighters.append(fighter_data)
        
        # Debug output for first few fighters
        if i < 3:
            print(f"[DEBUG] Fighter {i+1}: {full_name} ({nickname}) - {wins_text}W-{losses_text}L-{draws_text}D")

    print(f"[INFO] {letter}: {len(fighters)} fighters parsed from {used_url}")
    return fighters

def main():
    all_fighters = []
    
    # Test with just a few letters first (change to string.ascii_uppercase for full run)
    test_letters = ['S', 'A']  # Just S and A for testing
    
    for letter in test_letters:
        letter_fighters = scrape_letter(letter)
        all_fighters.extend(letter_fighters)
        time.sleep(0.8)  # be polite

    if not all_fighters:
        print("[ERROR] Parsed 0 fighters. Check debug_*.html and selectors.")
        return

    # Create DataFrame
    dataframe = pd.DataFrame(all_fighters)
    
    # Remove duplicates based on full name
    original_count = len(dataframe)
    dataframe = dataframe.drop_duplicates(subset=["full_name"])
    dedupe_count = len(dataframe)
    
    print(f"[INFO] Removed {original_count - dedupe_count} duplicates")
    
    # Save to CSV
    try:
        dataframe.to_csv("ufc_fighters.csv", index=False)
        print(f"[OK] Saved {len(dataframe)} fighters to ufc_fighters.csv ✅")
        
        # Show sample of the data
        print(f"\n[INFO] Sample data:")
        print(dataframe[['full_name', 'nickname', 'height', 'weight', 'wins', 'losses', 'draws']].head())
        
        # Verify CSV file
        import os
        if os.path.exists("ufc_fighters.csv"):
            size = os.path.getsize("ufc_fighters.csv")
            print(f"[INFO] CSV file created successfully ({size} bytes)")
        
    except Exception as e:
        print(f"[ERROR] Failed to save CSV: {e}")

if __name__ == "__main__":
    main()