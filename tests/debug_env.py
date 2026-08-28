import os
from dotenv import load_dotenv

load_dotenv()  # Load .env file
raw_value = os.getenv('TICKERS')
print(f"RAW TICKERS: '{raw_value}'")
print(f"Length: {len(raw_value)}")
print(f"Characters: {[ord(c) for c in raw_value]}")