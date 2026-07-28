import os
from pathlib import Path
from dotenv import load_dotenv

# Load .env file explicitly from the directory where config.py is located
config_dir = Path(__file__).parent
env_file = config_dir / ".env"
load_dotenv(env_file)

# ANMAT WebService (TrazaMed de pyafipws)
#Esto no se uso si usamos pyafipws...
ANMAT_WSDL = os.getenv("ANMAT_WSDL", "https://servicios.pami.org.ar/trazamed.WebService?wsdl")
print(f"Using ANMAT WSDL: {ANMAT_WSDL}")

# Credenciales de APLICACIÓN (tu usuario en ANMAT)
# Estos se usan en los parámetros de los métodos (usuario="...", password="...")
ANMAT_USUARIO = os.getenv("ANMAT_USUARIO")
ANMAT_PASSWORD = os.getenv("ANMAT_PASSWORD")

# NOTA: Las credenciales de TRANSPORTE WSSE (testwservice/testwservicepsw) están
# en anmat_reporter.py y son fijas según especificación ANMAT Disp. 3683/11

# Validate credentials are loaded
if not ANMAT_USUARIO or not ANMAT_PASSWORD:
    import warnings
    warnings.warn(
        "⚠️  ANMAT credentials not found in .env file. "
        "Please set ANMAT_USUARIO and ANMAT_PASSWORD in .env"
    )

FIREBIRD_HOST = os.getenv("FIREBIRD_HOST")
FIREBIRD_PORT = int(os.getenv("FIREBIRD_PORT", 3050))
FIREBIRD_DB = os.getenv("FIREBIRD_DB")
FIREBIRD_USER = os.getenv("FIREBIRD_USER")
FIREBIRD_PASSWORD = os.getenv("FIREBIRD_PASSWORD")
FIREBIRD_ROLE = os.getenv("FIREBIRD_ROLE", "")

SCHEDULER_INTERVAL = int(os.getenv("SCHEDULER_INTERVAL_MINUTES", 5))

# Timeout para conexiones ANMAT y Firebird (segundos)
ANMAT_TIMEOUT = int(os.getenv("ANMAT_TIMEOUT", 15))
FIREBIRD_CONNECT_TIMEOUT = int(os.getenv("FIREBIRD_CONNECT_TIMEOUT", 8))

FIREBIRD_CONN_STRING = f"{FIREBIRD_HOST}/{FIREBIRD_PORT}:{FIREBIRD_DB}"
