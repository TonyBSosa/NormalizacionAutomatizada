import os

class Config:
    # — App —
    SECRET_KEY = os.environ.get("SECRET_KEY", "dev")  # cámbiala en prod
    UPLOAD_FOLDER = os.environ.get("UPLOAD_FOLDER", "uploads")
    MAX_CONTENT_LENGTH = 16 * 1024 * 1024  # 16MB
    ALLOWED_EXT = {"csv"}

    # — Plantilla CSV —
    # Si True, una FK sin destino en el CSV es error; si False, es advertencia (lo resolvemos desde SQL)
    REQUIRE_FK_TARGET_IN_CSV = False

# — Parámetros del análisis —
ANALYSIS_CFG = {
    "sample_rows": 50000,          # None = toda la tabla (cuidado con tablas grandes)
    "infer_singlecol_fds": True,   # Infieren FDs A->B de 1 col determinante (heurística)
    "fd_check_nulls": False,       # Si False, ignora RHS NULL al chequear FD
}
