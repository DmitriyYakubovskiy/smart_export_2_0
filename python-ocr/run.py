import json
from pathlib import Path
import uvicorn

BASE_DIR = Path(__file__).resolve().parent

if __name__ == '__main__':
    with (BASE_DIR / 'config.json').open(encoding='utf-8') as file:
        config = json.load(file)
    uvicorn.run('app.main:app', host=config['host'], port=int(config['port']),
                reload=config.get('reload', False),
                reload_dirs=[str(BASE_DIR / 'app')], app_dir=str(BASE_DIR))
