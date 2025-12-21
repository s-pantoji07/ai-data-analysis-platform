from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import urllib.parse  # Import this
import os

# 1. Define the password separately
raw_password = "sarvesh@1!" 

# 2. Encode the password (turns @ into %40 and ! into %21)
safe_password = urllib.parse.quote_plus(raw_password)

# 3. Construct the URL using the safe password
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    f"postgresql://postgres:{safe_password}@localhost:5432/ai_analytics"
)

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(bind=engine)