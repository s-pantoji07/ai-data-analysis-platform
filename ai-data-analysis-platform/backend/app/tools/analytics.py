from app.analytics.engine import AnalyticsEngine
from app.analytics.query_models import AnalyticsQuery

engine = AnalyticsEngine()

def run_analytics(query: AnalyticsQuery):
    return engine.execute(query)
