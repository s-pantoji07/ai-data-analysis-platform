from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import urllib.parse
import os

# 1. Define the password separately
raw_password = "sarvesh@1!" 

# 2. Encode the password
safe_password = urllib.parse.quote_plus(raw_password)

# 3. Construct the URL
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    f"postgresql://postgres:{safe_password}@localhost:5432/ai_analytics"
)

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# --- ADD THIS FUNCTION ---
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()