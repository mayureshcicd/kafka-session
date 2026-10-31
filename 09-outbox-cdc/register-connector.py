#!/usr/bin/env python3
"""Run after Kafka, PostgreSQL, this app (creates tables), and Connect are ready."""
import json,time
from pathlib import Path
from urllib.request import Request,urlopen
config=json.loads(Path(__file__).with_name('connector.json').read_text())
# PUT is repeatable: it creates or updates this one connector, not unrelated connectors.
request=Request('http://localhost:28083/connectors/'+config['name']+'/config',
                data=json.dumps(config['config']).encode(),method='PUT',headers={'Content-Type':'application/json'})
for attempt in range(60):
    try:
        with urlopen(request,timeout=10) as response: print(response.read().decode())
        break
    except OSError:
        if attempt==59: raise
        time.sleep(1)
