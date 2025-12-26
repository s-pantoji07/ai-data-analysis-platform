from fastapi import FastAPI
from app.api.v1 import upload, query, profile, dashboard, metadata,plan
from dotenv import load_dotenv
load_dotenv() # This must run before the parser is initialized
app = FastAPI(title="AI data analysis platform")

app.include_router(upload.router,prefix = "/api/v1/upload", )
app.include_router(profile.router,prefix = "/api/v1/profile",)
app.include_router(query.router,prefix = "/api/v1/query",)
# app.include_router(dashboard.router,prefix = "/api/v1/dashboard",)
app.include_router(metadata.router,prefix = "/api/v1/metadata",)
app.include_router(plan.router, prefix="/api/v1/plan")

app.get("/")
def health_check():
    return {"status":"ok"}