from fastapi import FastAPI
from app.api.v1 import upload, query, profile, dashboard, metadata

app = FastAPI(title="AI data analysis platform")

app.include_router(upload.router,prefix = "/api/v1/upload", )
app.include_router(query.router,prefix = "/api/v1/profile",)
app.include_router(profile.router,prefix = "/api/v1/query",)
app.include_router(dashboard.router,prefix = "/api/v1/dashboard",)
app.include_router(metadata.router,prefix = "/api/v1/metadata",)

app.get("/")
def health_check():
    return {"status":"ok"}