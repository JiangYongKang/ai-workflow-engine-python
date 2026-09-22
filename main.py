from app.config import EngineConfig
from app.service import create_app

app = create_app(EngineConfig())
