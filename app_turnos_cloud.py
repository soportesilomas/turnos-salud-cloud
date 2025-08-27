import os
import io
import hashlib
from datetime import datetime
import pandas as pd
import streamlit as st
import altair as alt

# --- Supabase client ---
from supabase import create_client, Client

# ============== CONFIG ==============
st.set_page_config(page_title="Tablero de Turnos (Cloud)", layout="wide")

# Leer variables desde st.secrets (Streamlit Cloud) o desde entorno local
SUPABASE_URL = (st.secrets.get("SUPABASE_URL")
                if hasattr(st, "secrets") else None) or os.getenv("SUPABASE_URL")
SUPABASE_ANON_KEY = (st.secrets.get("SUPABASE_ANON_KEY")
                     if hasattr(st, "secrets") else None) or os.getenv("SUPABASE_ANON_KEY")

if not SUPABASE_URL or not SUPABASE_ANON_KEY:
    st.error("Faltan variables de entorno SUPABASE_URL y/o SUPABASE_ANON_KEY.")
    st.stop()

supabase: Client = create_client(SUPABASE_URL, SUPABASE_ANON_KEY)

REQUIRED_COLS = [
    "Fecha", "Hora", "Tipo Turno", "Paciente", "DNI", "Teléfonos", "Mail",
    "Cobertura", "Ubicación", "Efector", "Procedimiento",
    "Domicilio", "Localidad", "Edad", "Estado", "Atendido"
]

# ============== UTILS ==============
def read_any(file):
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

def fetch_profiles(user_id):
    res = supabase.table("profiles").select("role").eq("user_id", user_id).single().execute()
    if res.data:
        return res.data.get("role", "viewer")
    return "viewer"

def fetch_turnos(date_from=None, date_to=None, page_size=5000, max_pages=40):
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

# ============== AUTH UI ==============
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

# ============== ADMIN: CARGA DE DATOS ==============
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

                # ---- convertir fecha_hora a UTC ----
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

                # ---- row_id hash ----
                row_parts = (
                    df["DNI"].astype(str).fillna("")
                    + "|" + df["Fecha"].astype(str).fillna("")
                    + "|" + df["Hora"].astype(str).fillna("")
                    + "|" + df["Ubicación"].astype(str).fillna("")
                    + "|" + df["Procedimiento"].astype(str).fillna("")
                )
                df["row_id"] = row_parts.apply(lambda x: hashlib.sha1(x.encode("utf-8")).hexdigest())

                # ---- renombrar columnas ----
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
                df["user_id"] = user.id

                all_new.append(df)

            if all_new:
                data = pd.concat(all_new, ignore_index=True)

                # ---- upsert en lotes ----
                BATCH = 200
                total = 0
                for i in range(0, len(data), BATCH):
                    chunk = data.iloc[i:i+BATCH].copy()
                    if "fecha_hora" in chunk.columns:
                        fh_iso = pd.to_datetime(chunk["fecha_hora"], errors="coerce", utc=True).dt.tz_convert("UTC").dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
                        chunk["fecha_hora"] = fh_iso
                    payload = chunk.to_dict(orient="records")
                    supabase.table("turnos").upsert(payload, on_conflict="row_id").execute()
                    total += len(chunk)

                st.success(f"✅ Subidos (upsert) {total:,} registros.")
            else:
                st.warning("No se subieron datos (errores de columnas).")
else:
    st.info("Ingresaste con rol de **lectura**. Si necesitás subir datos, pedile al administrador que te cambie a 'admin'.")

# ============== FILTROS Y DASHBOARD ==============
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

# Filtros adicionales
col1, col2, col3 = st.columns(3)
with col1:
    ubic_sel = st.multiselect("Ubicación", options=sorted(df["ubicacion"].dropna().unique()))
    if ubic_sel:
        df = df[df["ubicacion"].isin(ubic_sel)]
with col2:
    tipo_sel = st.multiselect("Tipo de Turno", options=sorted(df["tipo_turno"].dropna().unique()))
    if tipo_sel:
        df = df[df["tipo_turno"].isin(tipo_sel)]
with col3:
    estado_sel = st.multiselect("Estado", options=sorted(df["estado"].dropna().unique()))
    if estado_sel:
        df = df[df["estado"].isin(estado_sel)]

st.write(f"Total de registros filtrados: {len(df):,}")

# ============== KPIs ==============
st.subheader("📊 Indicadores Clave (KPIs)")
col1, col2, col3 = st.columns(3)
col1.metric("Cantidad de turnos", f"{len(df):,}")
if "estado" in df.columns:
    cancelados = df[df["estado"].str.contains("cancel", case=False, na=False)]
    col2.metric("Cancelados", f"{len(cancelados):,}")
else:
    col2.metric("Cancelados", "N/D")
if "atendido" in df.columns:
    atendidos = df[df["atendido"].str.lower().eq("si")]
    col3.metric("Atendidos", f"{len(atendidos):,}")
else:
    col3.metric("Atendidos", "N/D")

# ============== Gráfico de barras por mes ==============
st.subheader("📈 Evolución mensual de turnos")
df["fecha"] = pd.to_datetime(df["fecha"], errors="coerce", dayfirst=True)
df["mes"] = df["fecha"].dt.to_period("M").astype(str)
mes_counts = df.groupby("mes").size().reset_index(name="cantidad")
chart = alt.Chart(mes_counts).mark_bar().encode(
    x=alt.X("mes", sort=None),
    y="cantidad"
)
st.altair_chart(chart, use_container_width=True)

# ============== Heatmap horario ==============
st.subheader("🌡️ Mapa de calor por hora del día")
df["hora_dt"] = pd.to_datetime(df["hora"], format="%H:%M", errors="coerce")
df["hora_num"] = df["hora_dt"].dt.hour
heat = df.groupby(["mes", "hora_num"]).size().reset_index(name="cantidad")
heatmap = alt.Chart(heat).mark_rect().encode(
    x=alt.X("hora_num:O", title="Hora del día"),
    y=alt.Y("mes:O", title="Mes"),
    color=alt.Color("cantidad:Q", scale=alt.Scale(scheme="reds")),
    tooltip=["mes", "hora_num", "cantidad"]
)
st.altair_chart(heatmap, use_container_width=True)

# ============== Exportar CSV ==============
st.subheader("📤 Exportar datos filtrados")
csv = df.to_csv(index=False).encode("utf-8")
st.download_button("⬇️ Descargar CSV", csv, "turnos_filtrados.csv", "text/csv")
