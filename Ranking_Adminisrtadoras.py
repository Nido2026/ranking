import os
import time
from datetime import date, datetime, timedelta

import pandas as pd
import requests
from dotenv import load_dotenv

# ============================================================
# Credenciales: se leen desde el .env (nunca hardcodeadas aquí)
# ============================================================
RUTA_ENV = r"F:\Bajadas\Phyton\Cryptos\.env"
load_dotenv(dotenv_path=RUTA_ENV)

API_KEY = os.getenv("BUSCAFONDOS_API_KEY")
SUPABASE_URL = os.getenv("SUPABASE_URL", "https://dinrwcwdxmkcbtyxqlbn.supabase.co")
# Para borrar/insertar conviene la service_role (ignora RLS); si no existe, prueba las otras
SUPABASE_KEY = (
    os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    or os.getenv("SUPABASE_KEY")
    or os.getenv("SUPABASE_ANON_KEY")
)
if SUPABASE_KEY:
    SUPABASE_KEY = SUPABASE_KEY.strip().strip('"').strip("'")

if not API_KEY:
    raise RuntimeError(
        f"No se encontró BUSCAFONDOS_API_KEY en {RUTA_ENV}. "
        "Verifica que el archivo .env exista y contenga: BUSCAFONDOS_API_KEY=tu_key_real"
    )
if not SUPABASE_KEY:
    raise RuntimeError(
        f"No se encontró la clave de Supabase en {RUTA_ENV} "
        "(SUPABASE_ANON_KEY / SUPABASE_KEY / SUPABASE_SERVICE_ROLE_KEY)."
    )

HEADERS_API = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
    "Accept": "application/json",
    "X-Api-Key": API_KEY,
}

HEADERS_SUPABASE = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
}

# ============================================================
# Configuración
# ============================================================
URL_BASE = "https://api.buscafondos.com/api/all-funds"
TABLA_RANKING = "ranking_fondos"
LOTE = 500                 # filas por request al insertar

# False -> borra solo las filas de los serieid que se están cargando y las vuelve a insertar
#          (el resto de la tabla queda intacto; no se duplica al ejecutar varias veces)
# True  -> vacía TODA la tabla antes de insertar (la tabla quedará solo con estos fondos)
REEMPLAZAR_TODO = False

# True -> hace todo el cálculo y muestra un resumen, pero NO escribe en Supabase
SOLO_PROBAR = False

# Administradora -> series a filtrar
FILTROS = {
    "BANCHILE ADMINISTRADORA GENERAL DE FONDOS S.A.": ["DIGITAL", "L"],
    "BCI ASSET MANAGEMENT ADMINISTRADORA  GENERAL DE FONDOS S.A.": ["INVERSIONI", "CLASI", "DINAMICA"],
}

# FILTRO PROVISORIO DE PRUEBA: procesa solo esta serieId (1 consulta de historial en vez de ~76).
# Para volver al proceso completo, déjalo en None.
SOLO_SERIEID = None

# Estructura exacta de la tabla ranking_fondos (Supabase)
COLUMNAS_RANKING = [
    "id", "run", "nombre_del_fondo", "serie", "serieid", "administradora", "currency",
    "tac", "fec_ult_val_cta", "valor_cta_ult", "diaria", "semanal", "mensual",
    "semana_en_curso", "dias_30", "vari_api_1_mes_movil", "fec_dia_ant",
    "val_cta_dia_ant", "fec_base_sem_7_dias", "val_cta_base_sem", "fec_cierre_sem_ant",
    "val_cta_cierre_sem_ant", "fec_cierre_mes_ant", "fecha_base_30_dias",
    "val_cta_base_30_dias", "estado", "responsable", "created_at",
]

# Campo de la API -> columna de la tabla (solo los que la API entrega)
MAPEO_API = {
    "run": "run",
    "fundName": "nombre_del_fondo",
    "serie": "serie",
    "serieId": "serieid",
    "agf": "administradora",
    "currency": "currency",
    "tac": "tac",
    "dailyChange": "diaria",
    "monthlyChange": "vari_api_1_mes_movil",
    "price": "valor_cta_ult",
    "price_as_of_date": "fec_ult_val_cta",
}


def norm(texto):
    """Normaliza texto para comparar (mayúsculas, sin espacios sobrantes)."""
    return " ".join(str(texto).upper().split())


def obtener_responsables():
    """Lee serieid -> responsable desde la tabla config_fondos_descarga de Supabase."""
    url = f"{SUPABASE_URL}/rest/v1/config_fondos_descarga"
    params = {"select": "serieid,responsable", "limit": 10000}
    resp = requests.get(url, headers=HEADERS_SUPABASE, params=params, timeout=60)
    resp.raise_for_status()
    filas = resp.json()
    return {str(f["serieid"]).strip(): f.get("responsable") for f in filas if f.get("serieid")}


# ============================================================
# Historial de cuotas desde la API de buscafondos
# (mismo endpoint que usa P2_apikey_v4.py: /api/real_assets/{serieId}/days)
# ============================================================
URL_DAYS = "https://api.buscafondos.com/api/real_assets/{serieid}/days"
DIAS_HISTORIAL = 75     # cubre la base de 30 días y el cierre del mes anterior

# La API limita la cantidad de consultas (error 429). Para una ejecución automática diaria:
# ante un bloqueo el programa se DETIENE de inmediato (sin insistir con el resto de las series)
# y no modifica la tabla de Supabase.
PAUSA_ENTRE_LLAMADAS = 0.6   # segundos entre una serie y la siguiente
REINTENTOS = 3               # reintentos por serie ante 429 / errores 5xx
ESPERA_BASE = 3              # segundos; se duplica en cada reintento (si la API no indica Retry-After)
MAX_ESPERA_SEG = 120         # si la API pide esperar más que esto, se aborta (bloqueo largo)
MAX_FALLOS_CONSECUTIVOS = 3  # series seguidas con error -> se aborta

# Si más de este % de fondos queda sin historial, NO se actualiza la tabla
# (así no se pisan datos buenos con filas incompletas)
MAX_SIN_HISTORIAL_PCT = 10


class ApiBloqueada(Exception):
    """La API de buscafondos está limitando las consultas (429): se detiene todo el proceso."""


def _espera_reintento(r, intento, base):
    try:
        return float(r.headers.get("Retry-After", ""))
    except ValueError:
        return base * (2 ** intento)


def _historial_serie(sesion, serieid):
    """Devuelve DataFrame (fecha, serieid, val_cuota) de una serie, con reintentos acotados."""
    desde = (date.today() - timedelta(days=DIAS_HISTORIAL)).isoformat()
    for intento in range(REINTENTOS + 1):
        r = sesion.get(URL_DAYS.format(serieid=serieid), params={"from_date": desde},
                       headers=HEADERS_API, timeout=60)
        if r.status_code == 429 or r.status_code >= 500:
            espera = _espera_reintento(r, intento, ESPERA_BASE)
            if r.status_code == 429 and (espera > MAX_ESPERA_SEG or intento == REINTENTOS):
                raise ApiBloqueada(f"HTTP 429 al consultar la serie {serieid} (tras {intento} reintentos)" if intento == REINTENTOS and espera <= MAX_ESPERA_SEG else f"HTTP 429 al consultar la serie {serieid} (la API pide esperar {espera:.0f}s)")
            if intento == REINTENTOS:
                r.raise_for_status()
            print(f"    ... {serieid}: HTTP {r.status_code}, reintento {intento + 1}/{REINTENTOS} en {espera:.0f}s")
            time.sleep(espera)
            continue
        r.raise_for_status()
        break
    filas = [
        {"fecha": it["attributes"]["date"], "serieid": serieid,
         "val_cuota": it["attributes"]["price"]}
        for it in r.json().get("data", [])
    ]
    h = pd.DataFrame(filas, columns=["fecha", "serieid", "val_cuota"])
    h["fecha"] = pd.to_datetime(h["fecha"].astype(str).str[:10], errors="coerce").dt.date
    h["val_cuota"] = pd.to_numeric(h["val_cuota"], errors="coerce")
    return h.dropna().drop_duplicates(subset=["fecha"]).sort_values("fecha")


def obtener_historial(serieids):
    """Descarga el historial de cuotas de cada serie. Se detiene si la API bloquea."""
    sesion = requests.Session()
    partes, fallos_seguidos = [], 0
    total = len(serieids)
    for n, sid in enumerate(serieids, 1):
        try:
            h = _historial_serie(sesion, sid)
            fallos_seguidos = 0
            if h.empty:
                print(f"    (!) {sid}: la API no devolvió precios")
            else:
                partes.append(h)
        except ApiBloqueada:
            raise
        except Exception as e:
            fallos_seguidos += 1
            print(f"    (!) {sid}: error al obtener historial: {e}")
            if fallos_seguidos >= MAX_FALLOS_CONSECUTIVOS:
                raise ApiBloqueada(f"{fallos_seguidos} series seguidas con error; se detiene para no agravar el problema")
        if n % 10 == 0:
            print(f"  Historial: {n}/{total} series")
        time.sleep(PAUSA_ENTRE_LLAMADAS)

    if not partes:
        return pd.DataFrame(columns=["fecha", "serieid", "val_cuota"])
    return pd.concat(partes, ignore_index=True)


def _ultimo_hasta(serie_hist, limite):
    """Último registro (fecha, valor) con fecha <= limite."""
    sub = serie_hist[serie_hist["fecha"] <= limite]
    if sub.empty:
        return None, None
    fila = sub.iloc[-1]
    return fila["fecha"], float(fila["val_cuota"])


def _pct(actual, base):
    if actual is None or base in (None, 0):
        return None
    return round((actual / base - 1) * 100, 4)


def calcular_metricas(historial, serieid, ultimo=None):
    """Calcula los campos de ranking_fondos que dependen del historial de cuotas.

    ultimo = (fecha, valor) con el último valor cuota que entrega la API (price,
    price_as_of_date). Se agrega al historial si es más reciente que el último
    registro guardado, para que las variaciones partan del dato más nuevo.
    """
    out = {}
    h = historial[historial["serieid"] == serieid].sort_values("fecha")
    if ultimo is not None and ultimo[0] is not None and pd.notna(ultimo[1]):
        f_api, v_api = ultimo
        if h.empty or f_api > h.iloc[-1]["fecha"]:
            fila = pd.DataFrame([{"fecha": f_api, "serieid": serieid, "val_cuota": float(v_api)}])
            h = pd.concat([h, fila], ignore_index=True)
    if h.empty:
        return out

    f_ult, v_ult = h.iloc[-1]["fecha"], float(h.iloc[-1]["val_cuota"])
    out["fec_ult_val_cta"], out["valor_cta_ult"] = f_ult, v_ult

    # Día anterior: registro inmediatamente anterior al último
    f_ant, v_ant = _ultimo_hasta(h, f_ult - timedelta(days=1))
    out["fec_dia_ant"], out["val_cta_dia_ant"] = f_ant, v_ant
    out["diaria"] = _pct(v_ult, v_ant)

    # Semanal: base = último registro con fecha <= ult - 7 días
    f7, v7 = _ultimo_hasta(h, f_ult - timedelta(days=7))
    out["fec_base_sem_7_dias"], out["val_cta_base_sem"] = f7, v7
    out["semanal"] = _pct(v_ult, v7)

    # Semana en curso: base = cierre de la semana anterior (último registro antes del lunes de esta semana)
    lunes = f_ult - timedelta(days=f_ult.weekday())
    fs, vs = _ultimo_hasta(h, lunes - timedelta(days=1))
    out["fec_cierre_sem_ant"], out["val_cta_cierre_sem_ant"] = fs, vs
    out["semana_en_curso"] = _pct(v_ult, vs)

    # Mensual (mes en curso): base = cierre del mes anterior
    fm, vm = _ultimo_hasta(h, f_ult.replace(day=1) - timedelta(days=1))
    out["fec_cierre_mes_ant"] = fm
    out["mensual"] = _pct(v_ult, vm)

    # 30 días: base = último registro con fecha <= ult - 30 días
    f30, v30 = _ultimo_hasta(h, f_ult - timedelta(days=30))
    out["fecha_base_30_dias"], out["val_cta_base_30_dias"] = f30, v30
    out["dias_30"] = _pct(v_ult, v30)
    return out


# ============================================================
# Actualización de la tabla ranking_fondos en Supabase
# ============================================================
def _limpiar(valor):
    """Convierte valores de pandas/numpy a algo serializable en JSON."""
    if valor is None or valor is pd.NaT:
        return None
    if isinstance(valor, (datetime, date, pd.Timestamp)):
        return valor.isoformat()[:10]
    if hasattr(valor, "item"):            # numpy -> python
        valor = valor.item()
    try:
        if pd.isna(valor):
            return None
    except (TypeError, ValueError):
        pass
    if isinstance(valor, str):
        valor = valor.strip()
        return valor or None
    return valor


def _url_tabla():
    return f"{SUPABASE_URL}/rest/v1/{TABLA_RANKING}"


def _ids_a_reemplazar(serieids):
    """ids de las filas existentes de esos serieid (o de toda la tabla si REEMPLAZAR_TODO)."""
    params = {"select": "id", "order": "id.asc"}
    if not REEMPLAZAR_TODO:
        lista = ",".join(f'"{s}"' for s in serieids)
        params["serieid"] = f"in.({lista})"
    ids, offset, paso = [], 0, 1000
    while True:
        r = requests.get(_url_tabla(), headers=HEADERS_SUPABASE,
                         params={**params, "limit": paso, "offset": offset}, timeout=60)
        r.raise_for_status()
        lote = r.json()
        ids.extend(f["id"] for f in lote)
        if len(lote) < paso:
            return ids
        offset += paso


def actualizar_tabla(df_out):
    # id y created_at se omiten: id es identity (GENERATED ALWAYS) y created_at tiene default now()
    columnas = [c for c in COLUMNAS_RANKING if c not in ("id", "created_at")]
    filas = [{c: _limpiar(v) for c, v in reg.items()}
             for reg in df_out[columnas].to_dict(orient="records")]
    serieids = [f["serieid"] for f in filas if f.get("serieid")]

    print(f"\nFilas listas para Supabase: {len(filas)}")
    if SOLO_PROBAR:
        print("SOLO_PROBAR=True: no se escribió nada en Supabase. Primera fila:")
        print(filas[0])
        return

    # 1. Ubicar las filas actuales que serán reemplazadas (aún no se borra nada)
    ids_viejos = _ids_a_reemplazar(serieids)
    print(f"Filas actuales a reemplazar: {len(ids_viejos)}")

    # 2. Insertar primero las filas nuevas (el id lo genera Supabase)
    h_ins = {**HEADERS_SUPABASE, "Prefer": "return=minimal"}
    for i in range(0, len(filas), LOTE):
        lote = filas[i:i + LOTE]
        r = requests.post(_url_tabla(), headers=h_ins, json=lote, timeout=120)
        if not r.ok:
            raise RuntimeError(
                f"Error insertando filas {i}-{i + len(lote)}: {r.status_code} {r.text}. "
                "Las filas anteriores no se borraron."
            )
        print(f"  Insertadas {min(i + LOTE, len(filas))}/{len(filas)}")

    # 2b. Recién ahora se borran las filas viejas (por id)
    h_borrar = {**HEADERS_SUPABASE, "Prefer": "return=minimal"}
    for i in range(0, len(ids_viejos), 200):
        grupo = ",".join(str(x) for x in ids_viejos[i:i + 200])
        r = requests.delete(_url_tabla(), headers=h_borrar, params={"id": f"in.({grupo})"}, timeout=60)
        if not r.ok:
            raise RuntimeError(f"Error al borrar filas antiguas: {r.status_code} {r.text}")
    print(f"Filas antiguas borradas: {len(ids_viejos)}")

    # 3. Verificación
    lista = ",".join(f'"{s}"' for s in serieids)
    r = requests.get(_url_tabla(), headers={**HEADERS_SUPABASE, "Prefer": "count=exact", "Range": "0-0"},
                     params={"select": "id", "serieid": f"in.({lista})"}, timeout=60)
    r.raise_for_status()
    total = int(r.headers["Content-Range"].split("/")[-1])
    print(f"Filas de estos fondos en la tabla ahora: {total} (esperadas: {len(filas)})")
    if total != len(filas):
        print("AVISO: el conteo no coincide; revisa duplicados o permisos (RLS).")
    print("¡Éxito! Tabla ranking_fondos actualizada.")


def _get_all_funds(reintentos=3, espera_base=20):
    """Consulta all-funds; ante 429 espera poco y, si persiste, detiene el proceso."""
    for intento in range(reintentos + 1):
        r = requests.get(URL_BASE, headers=HEADERS_API, timeout=120)
        if r.status_code != 429 and r.status_code < 500:
            r.raise_for_status()
            return r
        espera = _espera_reintento(r, intento, espera_base)
        if r.status_code == 429 and (espera > MAX_ESPERA_SEG or intento == reintentos):
            raise ApiBloqueada(f"HTTP 429 al consultar all-funds (tras {intento} reintentos)" if intento == reintentos and espera <= MAX_ESPERA_SEG else f"HTTP 429 al consultar all-funds (la API pide esperar {espera:.0f}s)")
        if intento == reintentos:
            r.raise_for_status()
        print(f"  La API pidió esperar (HTTP {r.status_code}). Reintento {intento + 1}/{reintentos} en {espera:.0f}s...")
        time.sleep(espera)


def generar_ranking():
    try:
        print("Consultando API de buscafondos...")
        response = _get_all_funds()
        df = pd.DataFrame(response.json()["data"])

        # 1. Filtro por administradora + series
        df["_agf_n"] = df["agf"].map(norm)
        df["_serie_n"] = df["serie"].map(norm)

        partes = []
        for administradora, series in FILTROS.items():
            mask = (df["_agf_n"] == norm(administradora)) & (df["_serie_n"].isin([norm(s) for s in series]))
            sub = df[mask].copy()
            print(f"  {administradora.strip()} | series {series}: {len(sub)} fondos")
            if sub.empty:
                print("    (!) Sin resultados: revisa nombre de administradora o series en la API.")
            partes.append(sub)

        df_filtrado = pd.concat(partes, ignore_index=True)
        if df_filtrado.empty:
            print("No se encontraron fondos con los filtros indicados.")
            return

        # 1b. Filtro provisorio de prueba por serieId
        if SOLO_SERIEID:
            df_filtrado = df_filtrado[df_filtrado["serieId"].astype(str) == str(SOLO_SERIEID)].copy()
            print(f"  FILTRO DE PRUEBA ACTIVO: solo serieId {SOLO_SERIEID} -> {len(df_filtrado)} fondo(s)")
            if df_filtrado.empty:
                print("  El serieId indicado no está entre los fondos filtrados por administradora y serie.")
                return

        # 2. Ordenar por rentabilidad mensual (desc)
        df_filtrado["monthlyChange"] = pd.to_numeric(df_filtrado["monthlyChange"], errors="coerce")
        df_filtrado = df_filtrado.sort_values(by="monthlyChange", ascending=False).reset_index(drop=True)

        # 3. Armar salida con la estructura de ranking_fondos
        df_out = pd.DataFrame(index=df_filtrado.index, columns=COLUMNAS_RANKING)
        for campo_api, col in MAPEO_API.items():
            if campo_api in df_filtrado.columns:
                df_out[col] = df_filtrado[campo_api]

        # 4. Cruce con config_fondos_descarga por serieid -> responsable
        print("Cruzando con config_fondos_descarga (Supabase)...")
        responsables = obtener_responsables()
        df_out["serieid"] = df_out["serieid"].astype(str).str.strip()
        df_out["responsable"] = df_out["serieid"].map(responsables)
        sin_resp = df_out["responsable"].isna().sum()
        print(f"  Con responsable: {len(df_out) - sin_resp} | Sin cruce: {sin_resp}")

        # 4b. Métricas desde el historial de valores cuota
        print("Descargando historial de cuotas desde la API de buscafondos...")
        historial = obtener_historial(df_out["serieid"].tolist())
        sin_hist = []
        df_out["fec_ult_val_cta"] = pd.to_datetime(df_out["fec_ult_val_cta"], errors="coerce").dt.date
        for idx, sid in df_out["serieid"].items():
            f_api = df_out.at[idx, "fec_ult_val_cta"]
            v_api = pd.to_numeric(df_out.at[idx, "valor_cta_ult"], errors="coerce")
            f_api = None if pd.isna(f_api) else f_api
            met = calcular_metricas(historial, sid, ultimo=(f_api, v_api))
            # Sin historial solo se puede dejar el último valor de la API
            if len(historial[historial["serieid"] == sid]) == 0:
                sin_hist.append(sid)
                met = {}
            for col, val in met.items():
                df_out.at[idx, col] = val
        print(f"  Con historial: {len(df_out) - len(sin_hist)} | Sin historial: {len(sin_hist)}")
        for sid in sin_hist:
            print(f"    - sin historial: {sid}")

        pct_sin = len(sin_hist) / len(df_out) * 100
        if pct_sin > MAX_SIN_HISTORIAL_PCT:
            print(f"\nABORTADO: {len(sin_hist)} de {len(df_out)} fondos ({pct_sin:.0f}%) quedaron sin historial "
                  f"(límite {MAX_SIN_HISTORIAL_PCT}%). No se modificó la tabla ranking_fondos.")
            print("Vuelve a ejecutar en unos minutos o sube PAUSA_ENTRE_LLAMADAS.")
            raise SystemExit(1)

        # Reordenar por mensual (mes en curso) descendente
        df_out["mensual"] = pd.to_numeric(df_out["mensual"], errors="coerce")
        df_out = df_out.sort_values("mensual", ascending=False, na_position="last").reset_index(drop=True)

        # 5. Subir directo a Supabase (sin archivo local)
        actualizar_tabla(df_out)

    except ApiBloqueada as e:
        print(f"\nPROCESO DETENIDO: {e}")
        print("La API de buscafondos está limitando las consultas. No se modificó la tabla ranking_fondos.")
        print("Intenta nuevamente más tarde (en la próxima ejecución programada).")
        raise SystemExit(2)
    except Exception as e:
        print(f"Error: {e}")
        raise SystemExit(1)


if __name__ == "__main__":
    generar_ranking()