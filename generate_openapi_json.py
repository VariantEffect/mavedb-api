import subprocess
import sys
import os
import json

# Build new whl
subprocess.check_call([sys.executable, '-m', 'build'])

# Find most recent whl in dist
dist_dir = os.path.dirname(os.path.realpath(__file__)) + '/dist'
whls = [os.path.join(dist_dir, basename) for basename in os.listdir(dist_dir) if basename.endswith('.whl')]
most_recent_whl = max(whls, key=os.path.getctime)

# Install built whl
subprocess.check_call([sys.executable, '-m', 'pip', 'install', most_recent_whl])

from mavedb.server_main import customize_openapi_schema

with open('openapi.json', 'w') as f:
    json.dump(customize_openapi_schema(), f)
