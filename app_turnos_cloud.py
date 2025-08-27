# app_turnos_cloud.py
# Tablero de turnos en la nube con:
# - Login/registro Supabase (roles: admin / viewer)
# - Carga de Excel/CSV (solo admin) con upsert deduplicado por row_id
# - Normalización de fechas a UTC (timestamptz)
# - Filtros por fecha y campos
# - KPIs, evolución temporal, resumen por período, heatmap horario y curva horaria
# - Exportación CSV/Excel
#
# Requisitos (requirements.txt):
#   streamlit>=1.36
#   pandas>=2.2
#   openpyxl>=3.1
#   supabase>=2.5.3
#   numpy>=1.26

import os
import io
import hashlib
from datetime import datetime
import numpy as np
import pandas as pd
import streamlit as st
from supabase import create_client, Client

# =========================
# CONFIGURACIÓN / SECRETS
# =========================
st.set_page_config(page_title="Tablero de Turnos (Cloud)", layout="wide")

# Leer primero de st.secrets (Streamlit Cloud), o de variables de entorno en local
SUPABASE_URL = (st.secrets.get("SUPABASE_URL") if hasattr(st, "secrets") else None) or os.getenv("SUPABASE_URL")
SUPABASE_ANON_KEY = (st.secrets.get("SUPABASE_ANON_KEY") if hasattr(st, "secrets") else None) or os.getenv("SUPABASE_ANON_KEY")

if not SUPABASE_URL or not SUPABASE_ANON_KEY:
    st.error("Faltan variables de entorno SUPABASE_URL y/o SUPABASE_ANON_KEY.")
    st.stop()

supabase: Client = create_client(SUPABASE_URL, SUPABASE_ANON_KEY)

# Columnas esperadas en los excels del sistema
REQUIRED_COLS = [
    "Fecha", "Hora", "Tipo Turno", "Paciente", "DNI", "Teléfonos", "Mail",
    "Cobertura", "Ubicación", "Efector", "Procedimiento",
    "Domicilio", "Localidad", "Edad", "Estado", "Atendido"
]

# =========================
# UTILIDADES
# =========================
def read_any(file):
    """Lee CSV/Excel intentando separadores comunes en CSV."""
    name = file.name.lower()
    if name.endswith(".csv"):
        for sep in [",", ";", "\t"]:
            file.seek(0)
            try:
                df = pd.read_csv(file, sep=sep, encoding="utf-8")
                if df.shape[1] > 1:
                    return df
            except Exception:
                continue
        file.seek(0)
        return pd.read_csv(file)
    else:
        return pd.read_excel(file, engine="openpyxl")

def to_iso_utc(ts):
    """Convierte pandas/py datetime a ISO-8601 UTC (terminado en Z). Devuelve None si no es válido."""
    if pd.isna(ts):
        return None
    try:
        ts = pd.to_datetime(ts, errors="coerce")
        if pd.isna(ts):
            return None
        # si no tiene tz, asumimos hora de Argentina y pasamos a UTC
        if ts.tzinfo is None:
            ts = ts.tz_localize("America/Argentina/Buenos_Aires", nonexistent="NaT", ambiguous="NaT")
        ts = ts.tz_convert("UTC")
        return ts.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    except Exception:
        return None

def safe_json_value(v):
    """Vuelve serializable cualquier valor común de pandas/numpy para JSON."""
    if v is None:
        return None
    if isinstance(v, float) and (np.isnan(v) or np.isinf(v)):
        return None
    if pd.isna(v):
        return None
    if isinstance(v, (pd.Timestamp, )):
        return to_iso_utc(v)
    if isinstance(v, (np.integer, )):
        return int(v)
    if isinstance(v, (np.floating, )):
        return float(v)
    return v

def safe_json_records(df: pd.DataFrame):
    """Convierte un DataFrame a lista de dicts 100% JSON-serializable."""
    records = []
    for _, row in df.iterrows():
        rec = {}
        for k, v in row.items():
            rec[k] = safe_json_value(v)
        records.append(rec)
    return records

def fetch_profiles(user_id):
    res = supabase.table("profiles").select("role").eq("user_id", user_id).single().execute()
    if res.data:
        return res.data.get("role", "viewer")
    return "viewer"

def fetch_turnos(date_from=None, date_to=None, page_size=5000, max_pages=40):
    """Trae datos paginando. Aplica filtro por fecha_hora en el servidor si se pasa."""
    q = supabase.table("turnos").select("*", count="exact")
    if date_from and date_to:
        q = q.gte("fecha_hora", date_from.isoformat()).lte("fecha_hora", date_to.isoformat())

    all_rows = []
    from_i = 0
    for _ in range(max_pages):
        page = q.range(from_i, from_i + page_size - 1).execute()
        rows = page.data or []
        all_rows.extend(rows)
        if len(rows) < page_size:
            break
        from_i += page_size

    if not all_rows:
        return pd.DataFrame()
    return pd.DataFrame(all_rows)

# =========================
# AUTENTICACIÓN
# =========================
st.sidebar.title("🔐 Acceso")
if "session" not in st.session_state:
    st.session_state.session = None
if "role" not in st.session_state:
    st.session_state.role = None

if st.session_state.session is None:
    choice = st.sidebar.radio("Elegí una opción", ["Ingresar", "Registrarme"])
    email = st.sidebar.text_input("Email")
    password = st.sidebar.text_input("Contraseña", type="password")
    if choice == "Ingresar":
        if st.sidebar.button("Iniciar sesión"):
            try:
                auth_resp = supabase.auth.sign_in_with_password({"email": email, "password": password})
                st.session_state.session = auth_resp
                st.session_state.role = fetch_profiles(auth_resp.user.id)
                st.rerun()
            except Exception as e:
                st.sidebar.error(f"No se pudo iniciar sesión: {e}")
    else:
        if st.sidebar.button("Crear cuenta"):
            try:
                supabase.auth.sign_up({"email": email, "password": password})
                st.success("Cuenta creada. Ahora iniciá sesión.")
            except Exception as e:
                st.sidebar.error(f"No se pudo registrar: {e}")
    st.stop()

user = st.session_state.session.user
role = st.session_state.role or "viewer"
st.sidebar.success(f"Conectado como: {user.email} ({role})")
if st.sidebar.button("Cerrar sesión"):
    supabase.auth.sign_out()
    st.session_state.session = None
    st.rerun()

st.title("🏥 Tablero de Turnos (Cloud con Supabase)")

# =========================
# ADMIN: CARGA DE EXCEL/CSV
# =========================
if role == "admin":
    st.header("📥 Cargar/Unificar Excel/CSV (solo administradores)")
    files = st.file_uploader("Subí uno o varios archivos", type=["xlsx","xls","csv"], accept_multiple_files=True)

    if files:
        st.write("Archivos seleccionados:")
        for f in files:
            st.write(f"- {f.name}")

        if st.button("➕ Subir y unificar en base"):
            all_new = []
            for f in files:
                df = read_any(f)
                missing = [c for c in REQUIRED_COLS if c not in df.columns]
                if missing:
                    st.error(f"❌ {f.name}: faltan columnas {missing}. No se sube.")
                    continue

                # ---- construir fecha_hora (Argentina -> UTC) ----
                fh = pd.to_datetime(
                    df["Fecha"].astype(str).str.strip() + " " + df["Hora"].astype(str).str.strip(),
                    dayfirst=True,
                    errors="coerce"
                )
                try:
                    fh = fh.dt.tz_localize("America/Argentina/Buenos_Aires", nonexistent="NaT", ambiguous="NaT").dt.tz_convert("UTC")
                except Exception:
                    try:
                        fh = fh.dt.tz_convert("UTC")
                    except Exception:
                        pass
                df["fecha_hora"] = fh

                # ---- row_id estable (texto) ----
                row_parts = (
                    df["DNI"].astype(str).fillna("")
                    + "|" + df["Fecha"].astype(str).fillna("")
                    + "|" + df["Hora"].astype(str).fillna("")
                    + "|" + df["Ubicación"].astype(str).fillna("")
                    + "|" + df["Procedimiento"].astype(str).fillna("")
                )
                df["row_id"] = row_parts.apply(lambda x: hashlib.sha1(x.encode("utf-8")).hexdigest())

                # ---- renombrar a snake_case ----
                rename = {
                    "Fecha": "fecha",
                    "Hora": "hora",
                    "Tipo Turno": "tipo_turno",
                    "Paciente": "paciente",
                    "DNI": "dni",
                    "Teléfonos": "telefonos",
                    "Mail": "mail",
                    "Cobertura": "cobertura",
                    "Ubicación": "ubicacion",
                    "Efector": "efector",
                    "Procedimiento": "procedimiento",
                    "Domicilio": "domicilio",
                    "Localidad": "localidad",
                    "Edad": "edad",
                    "Estado": "estado",
                    "Atendido": "atendido",
                }
                df = df.rename(columns=rename)

                cols = ["row_id","fecha","hora","tipo_turno","paciente","dni","telefonos","mail",
                        "cobertura","ubicacion","efector","procedimiento","domicilio","localidad",
                        "edad","estado","atendido","fecha_hora"]
                df = df[cols].copy()

                # trazabilidad
                df["user_id"] = user.id

                all_new.append(df)

            if all_new:
                data = pd.concat(all_new, ignore_index=True)

                # Normalizar fecha_hora a ISO-UTC y tipos JSON-serializables
                if "fecha_hora" in data.columns:
                    data["fecha_hora"] = data["fecha_hora"].apply(to_iso_utc)

                # Upsert en lotes chicos + payload seguro
                BATCH = 200
                total = 0
                for i in range(0, len(data), BATCH):
                    chunk = data.iloc[i:i+BATCH].copy()
                    payload = safe_json_records(chunk)
                    supabase.table("turnos").upsert(payload, on_conflict="row_id").execute()
                    total += len(chunk)

                st.success(f"✅ Subidos (upsert) {total:,} registros.")
            else:
                st.warning("No se subieron datos (errores de columnas).")
else:
    st.info("Ingresaste con rol de **lectura**. Si necesitás subir datos, pedile al administrador que te cambie a 'admin'.")

# =========================
# FILTROS (RANGO DE FECHAS)
# =========================
st.header("🔎 Filtros")
today = datetime.utcnow()
default_from = pd.Timestamp(today) - pd.Timedelta(days=90)
date_from, date_to = st.slider(
    "Rango de fechas",
    min_value=pd.Timestamp(today - pd.Timedelta(days=365*5)).to_pydatetime(),
    max_value=pd.Timestamp(today + pd.Timedelta(days=1)).to_pydatetime(),
    value=(default_from.to_pydatetime(), pd.Timestamp(today).to_pydatetime())
)

df = fetch_turnos(date_from=date_from, date_to=date_to)
if df.empty:
    st.warning("No hay datos en el rango/condiciones seleccionadas.")
    st.stop()

# =========================
# FILTROS POR CAMPOS
# =========================
def ms(label, series):
    vals = sorted([str(x) for x in pd.Series(series).dropna().astype(str).unique()])
    if not vals: return None
    return st.multiselect(label, vals)

c1, c2, c3, c4 = st.columns(4)
with c1:
    sel_ubic = ms("Ubicación", df.get("ubicacion"))
with c2:
    sel_cob = ms("Cobertura", df.get("cobertura"))
with c3:
    sel_proc = ms("Procedimiento", df.get("procedimiento"))
with c4:
    sel_efec = ms("Efector", df.get("efector"))

c5, c6, c7, c8 = st.columns(4)
with c5:
    sel_estado = ms("Estado", df.get("estado"))
with c6:
    sel_atendido = ms("Atendido", df.get("atendido"))
with c7:
    sel_localidad = ms("Localidad", df.get("localidad"))
with c8:
    sel_tipo = ms("Tipo Turno", df.get("tipo_turno"))

dff = df.copy()

def apply_in(d, col, vals):
    if col in d.columns and vals:
        return d[d[col].astype(str).isin(vals)]
    return d

dff = apply_in(dff, "ubicacion", sel_ubic)
dff = apply_in(dff, "cobertura", sel_cob)
dff = apply_in(dff, "procedimiento", sel_proc)
dff = apply_in(dff, "efector", sel_efec)
dff = apply_in(dff, "estado", sel_estado)
dff = apply_in(dff, "atendido", sel_atendido)
dff = apply_in(dff, "localidad", sel_localidad)
dff = apply_in(dff, "tipo_turno", sel_tipo)

# =========================
# KPIs
# =========================
st.header("📈 Indicadores")
k1, k2, k3, k4 = st.columns(4)
with k1:
    st.metric("Atenciones (filtrado)", len(dff))
with k2:
    st.metric("Pacientes únicos (DNI)", pd.Series(dff.get("dni")).nunique(dropna=True) if "dni" in dff.columns else "—")
with k3:
    st.metric("Centros activos", pd.Series(dff.get("ubicacion")).nunique(dropna=True) if "ubicacion" in dff.columns else "—")
with k4:
    edad_num = pd.to_numeric(dff.get("edad"), errors="coerce") if "edad" in dff.columns else pd.Series(dtype=float)
    st.metric("Edad promedio", f"{edad_num.mean():.1f}" if not edad_num.dropna().empty else "—")

# =========================
# GRÁFICOS
# =========================
st.header("📊 Gráficos")

# Evolución temporal
if "fecha_hora" in dff.columns:
    try:
        dff["_fh"] = pd.to_datetime(dff["fecha_hora"], errors="coerce", utc=True)
        st.write("**Evolución en el tiempo**")
        freq = st.selectbox("Frecuencia", ["Día", "Semana", "Mes"], index=2)
        rule = {"Día": "D", "Semana": "W", "Mes": "MS"}[freq]
        ts = dff.set_index("_fh").sort_index()
        serie = ts.groupby(pd.Grouper(freq=rule)).size().rename("Atenciones")
        st.line_chart(serie)
    except Exception:
        st.caption("No se pudo graficar la serie temporal (fecha_hora inválida).")

# Barras principales
if not dff.empty:
    if "ubicacion" in dff.columns:
        st.write("**Atenciones por centro (Top 15)**")
        st.bar_chart(dff["ubicacion"].astype(str).value_counts().head(15))
    if "procedimiento" in dff.columns:
        st.write("**Atenciones por procedimiento (Top 15)**")
        st.bar_chart(dff["procedimiento"].astype(str).value_counts().head(15))
    if "cobertura" in dff.columns:
        st.write("**Distribución por cobertura (Top 15)**")
        st.bar_chart(dff["cobertura"].astype(str).value_counts().head(15))
    if "efector" in dff.columns:
        st.write("**Ranking de efectores (Top 15)**")
        st.bar_chart(dff["efector"].astype(str).value_counts().head(15))

# Resumen por período
st.header("📅 Resumen por período")
if "fecha_hora" in dff.columns:
    try:
        dff["_fh"] = pd.to_datetime(dff["fecha_hora"], errors="coerce", utc=True)
        period = st.selectbox("Periodo de resumen", ["Mensual", "Semanal"], index=0)
        if period == "Mensual":
            dff["_Periodo"] = dff["_fh"].dt.to_period("M").dt.to_timestamp()
        else:
            dff["_Periodo"] = dff["_fh"].dt.to_period("W-MON").dt.start_time
        if "ubicacion" in dff.columns:
            pivot = dff.pivot_table(index="_Periodo", columns="ubicacion", values="row_id", aggfunc="count", fill_value=0).sort_index()
            pivot_total = pd.DataFrame({"Total": pivot.sum(axis=1)}).join(pivot)
        else:
            pivot_total = dff.groupby("_Periodo").size().rename("Total").to_frame()
        st.dataframe(pivot_total, use_container_width=True)
        st.bar_chart(pivot_total["Total"])
        st.download_button(
            "Descargar resumen CSV",
            data=pivot_total.to_csv(index=True).encode("utf-8-sig"),
            file_name=f"resumen_{'mensual' if period=='Mensual' else 'semanal'}.csv",
            mime="text/csv"
        )
    except Exception:
        st.caption("No se pudo calcular el resumen por período.")

# Heatmap + Curva horaria
st.header("🔥 Heatmap de demanda por hora")
if "fecha_hora" in dff.columns and not dff.empty:
    try:
        dff["_fh"] = pd.to_datetime(dff["fecha_hora"], errors="coerce", utc=True)
        dff["_hora"] = dff["_fh"].dt.hour
        dff["_dow"] = dff["_fh"].dt.dayofweek  # 0=Mon
        dow_names = {0:"Lun",1:"Mar",2:"Mié",3:"Jue",4:"Vie",5:"Sáb",6:"Dom"}
        dff["_dow_name"] = dff["_dow"].map(dow_names)
        heat = dff.pivot_table(index="_dow_name", columns="_hora", values="row_id", aggfunc="count", fill_value=0)
        heat = heat.reindex(["Lun","Mar","Mié","Jue","Vie","Sáb","Dom"])
        st.dataframe(heat, use_container_width=True)
        st.write("**Curva horaria promedio (todas las fechas)**")
        curve = heat.sum(axis=0)  # total por hora
        st.line_chart(curve)
    except Exception:
        st.caption("No se pudo generar el heatmap/curva horaria.")

# =========================
# TABLA Y EXPORT
# =========================
st.header("📋 Datos filtrados")
default_cols = ["fecha","hora","ubicacion","procedimiento","efector","cobertura","paciente","dni","edad","estado","atendido","localidad"]
keep_cols = [c for c in default_cols if c in dff.columns]
show_cols = st.multiselect("Columnas a mostrar", list(dff.columns), default=keep_cols if keep_cols else list(dff.columns)[:12])
st.dataframe(dff[show_cols] if show_cols else dff, use_container_width=True)

st.subheader("⬇️ Exportar")
csv_bytes = (dff[show_cols] if show_cols else dff).to_csv(index=False).encode("utf-8-sig")
st.download_button("Descargar CSV (filtrado)", data=csv_bytes, file_name="turnos_filtrado.csv", mime="text/csv")

def to_excel_bytes(df_to_export: pd.DataFrame) -> bytes:
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        (df_to_export[show_cols] if show_cols else df_to_export).to_excel(writer, index=False, sheet_name="Filtrado")
    return buf.getvalue()

xlsx_bytes = to_excel_bytes(dff)
st.download_button("Descargar Excel (filtrado)", data=xlsx_bytes, file_name="turnos_filtrado.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

csv = df.to_csv(index=False).encode("utf-8")
st.download_button("⬇️ Descargar CSV", csv, "turnos_filtrados.csv", "text/csv")
