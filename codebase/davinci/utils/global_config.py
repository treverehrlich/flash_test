import os
import platform
try:
    from davinci.dev_tools.auth import SECRETS
except ImportError:
    raise ImportError("No auth.py file detected for secrets. Check the DaVinci pip package.")

if not bool(os.environ.get('DAVINCI_PROD', False)) and 'doppler_token_dev' in SECRETS and 'st.dev' in SECRETS['doppler_token_dev']:
    ENV = 'dev'
    DOPPLER_KEY = 'doppler_token_dev'
elif 'doppler_token' in SECRETS and 'st.prd' in SECRETS['doppler_token']:
    ENV = 'prod'
    DOPPLER_KEY = 'doppler_token'
else:
    ENV = 'dev'
    DOPPLER_KEY = 'doppler_token'

PROD = ENV == 'prod'

SYSTEM = platform.system() == 'Linux'
"""
Boolean that detects the production environment.
"""