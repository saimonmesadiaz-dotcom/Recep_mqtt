"""
Centro de Comando · Telemetría MQTT
-----------------------------------
App de Streamlit con estética de base de comando espacial. Se conecta a un
broker MQTT, se suscribe a un tópico y muestra el último mensaje recibido
como paneles de telemetría.

Ejecutar:  streamlit run app.py
"""

import json
import threading
from datetime import datetime
from html import escape

import paho.mqtt.client as mqtt
import streamlit as st


# ═════════════════════════════════════════════════════════════════════════════
# CONSTANTES
# ═════════════════════════════════════════════════════════════════════════════
TIMEOUT_SEGUNDOS = 5
KEEPALIVE = 60

DEFAULTS = {
    "broker": "broker.mqttdashboard.com",
    "port": 1883,
    "topic": "Sensor/THP2",
    "client_id": "streamlit_client",
}

PUBLIC_BROKERS = [
    "broker.mqttdashboard.com",
    "test.mosquitto.org",
    "broker.hivemq.com",
]

# Reconoce nombres comunes de variables para asignarles icono y unidad.
# La clave es un fragmento del nombre (en minúsculas).
SENSOR_META = {
    "temp": ("🌡️", "°C"),
    "hum": ("💧", "%"),
    "pres": ("🧭", "hPa"),
    "press": ("🧭", "hPa"),
    "alt": ("⛰️", "m"),
    "lux": ("☀️", "lx"),
    "luz": ("☀️", "lx"),
    "bat": ("🔋", "%"),
    "volt": ("⚡", "V"),
    "co2": ("🌫️", "ppm"),
}


# ═════════════════════════════════════════════════════════════════════════════
# CONFIGURACIÓN DE LA PÁGINA Y ESTILOS
# ═════════════════════════════════════════════════════════════════════════════
def configurar_pagina() -> None:
    st.set_page_config(
        page_title="Centro de Comando · MQTT",
        page_icon="🛰️",
        layout="centered",
    )


def inyectar_estilos() -> None:
    st.markdown(
        """
        <style>
        @import url('https://fonts.googleapis.com/css2?family=Orbitron:wght@500;700;900&family=Rajdhani:wght@400;500;600;700&display=swap');

        :root {
            --espacio:   #050914;
            --panel:     rgba(11, 22, 44, 0.78);
            --cian:      #35E0F2;
            --cian-tenue: rgba(53, 224, 242, 0.28);
            --violeta:   #8B5CF6;
            --texto:     #D6E6F5;
            --apagado:   #7F96B2;
            --alerta:    #FF5C6C;
            --campo:     #0A1529;
        }

        /* ── Fondo: nebulosa + campo de estrellas ───── */
        .stApp {
            background:
                radial-gradient(ellipse at 15% 0%, rgba(139, 92, 246, .28), transparent 55%),
                radial-gradient(ellipse at 95% 35%, rgba(53, 224, 242, .13), transparent 50%),
                radial-gradient(ellipse at 40% 100%, rgba(255, 79, 216, .10), transparent 55%),
                var(--espacio);
            color: var(--texto);
            font-family: 'Rajdhani', sans-serif;
        }
        .stApp::before, .stApp::after {
            content: ""; position: fixed; inset: 0;
            pointer-events: none; z-index: 0;
        }
        .stApp::before {
            background-image:
                radial-gradient(1px 1px at 20px 30px, #fff, transparent),
                radial-gradient(1px 1px at 90px 140px, #cfe8ff, transparent),
                radial-gradient(1.5px 1.5px at 160px 70px, #fff, transparent),
                radial-gradient(1px 1px at 230px 190px, #b9d4ff, transparent);
            background-size: 260px 220px;
            opacity: .55;
        }
        .stApp::after {
            background-image:
                radial-gradient(1px 1px at 60px 90px, #fff, transparent),
                radial-gradient(1.5px 1.5px at 190px 20px, #ffe9ff, transparent),
                radial-gradient(1px 1px at 120px 200px, #fff, transparent);
            background-size: 340px 280px;
            animation: titilar 7s ease-in-out infinite alternate;
        }
        @keyframes titilar { from { opacity: .2; } to { opacity: .75; } }

        [data-testid="stHeader"] { background: transparent; }
        .block-container {
            position: relative; z-index: 1;
            padding-top: 2.5rem; max-width: 780px;
        }

        /* ── Texto base ─────────────────────────────── */
        .stApp p, .stApp li, .stApp label, .stApp h1, .stApp h2, .stApp h3,
        [data-testid="stWidgetLabel"] p, summary p {
            color: var(--texto) !important;
            font-family: 'Rajdhani', sans-serif;
            font-size: 1.05rem;
        }
        .stApp h2, .stApp h3 {
            font-family: 'Orbitron', sans-serif !important;
            font-size: 1.05rem !important;
            letter-spacing: .12em;
            text-transform: uppercase;
        }
        [data-testid="stCaptionContainer"] p {
            color: var(--apagado) !important; font-size: .95rem;
        }

        /* ── Encabezado con radar ───────────────────── */
        .encabezado { display: flex; align-items: center; gap: 1.2rem; }
        .radar {
            flex: none; width: 66px; height: 66px; border-radius: 50%;
            position: relative; overflow: hidden;
            border: 1px solid var(--cian);
            background: repeating-radial-gradient(
                circle, transparent 0 9px, rgba(53, 224, 242, .22) 9px 10px);
            box-shadow: 0 0 20px rgba(53, 224, 242, .4),
                        inset 0 0 14px rgba(53, 224, 242, .25);
        }
        .radar::before {
            content: ""; position: absolute; inset: 0; border-radius: 50%;
            background: conic-gradient(
                from 0deg, rgba(53, 224, 242, .6), transparent 30%);
            animation: barrido 4s linear infinite;
        }
        .radar::after {
            content: ""; position: absolute; top: 17px; left: 41px;
            width: 4px; height: 4px; border-radius: 50%;
            background: #fff; box-shadow: 0 0 6px #fff;
        }
        @keyframes barrido { to { transform: rotate(360deg); } }

        .titulo {
            font-family: 'Orbitron', sans-serif; font-weight: 900;
            font-size: 1.9rem; letter-spacing: .1em; text-transform: uppercase;
            color: #fff; margin: 0; line-height: 1.1;
            text-shadow: 0 0 18px rgba(53, 224, 242, .65);
        }
        .subtitulo {
            color: var(--apagado); font-size: 1.1rem; font-weight: 500;
            letter-spacing: .04em; margin: .25rem 0 0 0;
        }

        /* ── Indicador de enlace ────────────────────── */
        .enlace {
            display: inline-flex; align-items: center; gap: .6rem;
            margin: 1.4rem 0 .4rem 0; padding: .4rem 1rem;
            background: var(--panel); border: 1px solid var(--cian-tenue);
            font-weight: 600; letter-spacing: .06em; color: var(--texto);
            clip-path: polygon(0 0, calc(100% - 10px) 0, 100% 10px,
                               100% 100%, 10px 100%, 0 calc(100% - 10px));
        }
        .enlace .estado { color: var(--apagado); }
        .punto {
            width: 9px; height: 9px; border-radius: 50%;
            background: var(--apagado);
        }
        .punto.activo {
            background: #3DFFB0; box-shadow: 0 0 10px #3DFFB0;
            animation: pulso 1.8s ease-in-out infinite;
        }
        .punto.error { background: var(--alerta); box-shadow: 0 0 10px var(--alerta); }
        @keyframes pulso { 50% { opacity: .35; } }

        /* ── Paneles de telemetría ──────────────────── */
        .rejilla {
            display: grid; gap: 1.1rem; margin: 1rem 0 .5rem 0;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
        }
        .lectura {
            position: relative; padding: 1.2rem 1.4rem 1.3rem;
            background: linear-gradient(160deg,
                rgba(18, 36, 70, .85), rgba(7, 14, 30, .92));
            border: 1px solid var(--cian-tenue);
            box-shadow: inset 0 0 28px rgba(53, 224, 242, .07);
            clip-path: polygon(0 0, calc(100% - 18px) 0, 100% 18px,
                               100% 100%, 18px 100%, 0 calc(100% - 18px));
        }
        .lectura::before, .lectura::after {
            content: ""; position: absolute; width: 16px; height: 16px;
            border: 2px solid var(--cian);
            filter: drop-shadow(0 0 4px var(--cian));
        }
        .lectura::before { top: 0; left: 0; border-right: 0; border-bottom: 0; }
        .lectura::after  { bottom: 0; right: 0; border-left: 0; border-top: 0; }

        .lectura .etiqueta {
            display: flex; align-items: center; gap: .5rem;
            color: var(--apagado); font-weight: 600; font-size: 1.05rem;
            letter-spacing: .08em; text-transform: uppercase;
        }
        .lectura .valor {
            font-family: 'Orbitron', sans-serif; font-weight: 700;
            font-size: 2rem; color: #fff; margin-top: .5rem; line-height: 1.15;
            text-shadow: 0 0 14px rgba(53, 224, 242, .6);
        }
        .lectura .unidad {
            font-family: 'Rajdhani', sans-serif; font-weight: 600;
            font-size: 1.1rem; color: var(--cian);
            margin-left: .35rem; text-shadow: none;
        }

        /* ── Sin señal / falla ──────────────────────── */
        .vacio, .falla {
            padding: 2.2rem 1.5rem; text-align: center;
            color: var(--apagado); font-size: 1.1rem;
            background: var(--panel);
            border: 1px dashed var(--cian-tenue);
        }
        .vacio strong, .falla strong {
            display: block; margin-bottom: .35rem;
            font-family: 'Orbitron', sans-serif; letter-spacing: .12em;
            text-transform: uppercase; font-size: 1rem; color: var(--texto);
        }
        .falla { border: 1px solid rgba(255, 92, 108, .55);
                 background: rgba(255, 92, 108, .08); }
        .falla strong { color: var(--alerta); }

        /* ── Botón principal ────────────────────────── */
        .stButton > button {
            background: linear-gradient(90deg,
                rgba(53, 224, 242, .18), rgba(139, 92, 246, .32));
            border: 1px solid var(--cian); border-radius: 2px;
            padding: .85rem 1rem;
            box-shadow: 0 0 18px rgba(53, 224, 242, .25);
            transition: box-shadow .2s ease, background .2s ease;
        }
        .stButton > button p {
            color: #E8FBFF !important;
            font-family: 'Orbitron', sans-serif; font-weight: 700;
            letter-spacing: .16em; text-transform: uppercase;
            font-size: .95rem;
        }
        .stButton > button:hover {
            border-color: #fff;
            background: linear-gradient(90deg,
                rgba(53, 224, 242, .32), rgba(139, 92, 246, .5));
            box-shadow: 0 0 28px rgba(53, 224, 242, .55);
        }
        .stButton > button:focus-visible { outline: 2px solid #fff; outline-offset: 3px; }

        /* ── Barra lateral ──────────────────────────── */
        section[data-testid="stSidebar"] {
            background: rgba(5, 10, 22, .96);
            border-right: 1px solid var(--cian-tenue);
        }
        [data-baseweb="input"], [data-baseweb="base-input"] {
            background: var(--campo) !important;
            border-color: var(--cian-tenue) !important;
            border-radius: 2px !important;
        }
        [data-baseweb="input"] input, [data-baseweb="base-input"] input {
            color: var(--texto) !important; background: var(--campo) !important;
            font-family: 'Rajdhani', sans-serif; font-size: 1.05rem;
        }
        [data-baseweb="input"]:focus-within {
            border-color: var(--cian) !important;
            box-shadow: 0 0 10px rgba(53, 224, 242, .4);
        }
        .stNumberInput button { background: var(--campo) !important; color: var(--texto) !important; }

        /* ── Expanders, avisos y código ─────────────── */
        [data-testid="stExpander"] {
            background: var(--panel); border: 1px solid var(--cian-tenue);
            border-radius: 2px;
        }
        [data-testid="stAlert"] {
            background: rgba(53, 224, 242, .07);
            border: 1px solid var(--cian-tenue); border-radius: 2px;
        }
        [data-testid="stCode"] pre, [data-testid="stJson"] {
            background: var(--campo) !important;
        }
        [data-testid="stCode"] code { color: var(--cian) !important; }
        [data-testid="stSpinner"] p { color: var(--cian) !important; }

        @media (prefers-reduced-motion: reduce) {
            .radar::before, .stApp::after, .punto.activo { animation: none; }
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


# ═════════════════════════════════════════════════════════════════════════════
# ESTADO DE LA SESIÓN
# ═════════════════════════════════════════════════════════════════════════════
def inicializar_estado() -> None:
    st.session_state.setdefault("sensor_data", None)
    st.session_state.setdefault("last_update", None)


# ═════════════════════════════════════════════════════════════════════════════
# LÓGICA MQTT
# ═════════════════════════════════════════════════════════════════════════════
def crear_cliente(client_id: str) -> mqtt.Client:
    """Crea el cliente compatible con paho-mqtt 1.x y 2.x."""
    try:
        return mqtt.Client(mqtt.CallbackAPIVersion.VERSION1, client_id=client_id)
    except AttributeError:  # paho-mqtt 1.x
        return mqtt.Client(client_id=client_id)


def decodificar_payload(raw: bytes):
    """Devuelve el JSON si es válido; de lo contrario, el texto plano."""
    texto = raw.decode(errors="replace")
    try:
        return json.loads(texto)
    except json.JSONDecodeError:
        return texto


def obtener_mensaje_mqtt(broker: str, port: int, topic: str, client_id: str):
    """Se suscribe al tópico y devuelve el primer mensaje recibido.

    Retorna el payload (dict o str) o un dict {"error": ...} si falla
    la conexión o se agota el tiempo de espera.
    """
    recibido = threading.Event()
    resultado = {}

    def on_message(client, userdata, message):
        resultado["payload"] = decodificar_payload(message.payload)
        recibido.set()

    client = crear_cliente(client_id)
    client.on_message = on_message

    try:
        client.connect(broker, port, KEEPALIVE)
        client.subscribe(topic)
        client.loop_start()

        if not recibido.wait(timeout=TIMEOUT_SEGUNDOS):
            return {
                "error": (
                    f"No llegó ningún mensaje en {TIMEOUT_SEGUNDOS} s. "
                    "Revisa que el tópico sea correcto y que el sensor "
                    "esté publicando."
                )
            }
        return resultado["payload"]

    except Exception as e:
        return {"error": str(e)}

    finally:
        client.loop_stop()
        try:
            client.disconnect()
        except Exception:
            pass


# ═════════════════════════════════════════════════════════════════════════════
# COMPONENTES DE INTERFAZ
# ═════════════════════════════════════════════════════════════════════════════
def render_sidebar() -> dict:
    """Dibuja la barra lateral y devuelve la configuración elegida."""
    with st.sidebar:
        st.subheader("Parámetros de enlace")

        broker = st.text_input(
            "Broker MQTT",
            value=DEFAULTS["broker"],
            help="Dirección del servidor MQTT.",
        )
        port = st.number_input(
            "Puerto",
            value=DEFAULTS["port"],
            min_value=1,
            max_value=65535,
            help="Normalmente 1883 para conexiones sin cifrar.",
        )
        topic = st.text_input(
            "Tópico",
            value=DEFAULTS["topic"],
            help="Canal al que quieres suscribirte.",
        )
        client_id = st.text_input(
            "ID del cliente",
            value=DEFAULTS["client_id"],
            help="Identificador único para esta conexión.",
        )

        st.caption("Brokers públicos para pruebas")
        st.code("\n".join(PUBLIC_BROKERS), language=None)

    return {
        "broker": broker.strip(),
        "port": int(port),
        "topic": topic.strip(),
        "client_id": client_id.strip(),
    }


def estado_del_enlace() -> tuple[str, str]:
    """Devuelve (clase CSS, texto) según el último resultado."""
    data = st.session_state.sensor_data
    if data is None:
        return "", "Sin señal"
    if isinstance(data, dict) and "error" in data:
        return "error", "Falla de enlace"
    return "activo", "Enlace activo"


def render_encabezado(topic: str) -> None:
    st.markdown(
        """
        <div class="encabezado">
            <div class="radar"></div>
            <div>
                <h1 class="titulo">Centro de comando</h1>
                <div class="subtitulo">Telemetría del sensor MQTT en tiempo real</div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    clase, texto = estado_del_enlace()
    st.markdown(
        f"""
        <div class="enlace">
            <span class="punto {clase}"></span>
            <span>{escape(topic or "sin tópico")}</span>
            <span class="estado">{texto}</span>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_ayuda() -> None:
    with st.expander("Instrucciones de operación"):
        st.markdown(
            """
            1. Completa el **broker**, el **puerto** y el **tópico** en la barra lateral.
            2. Elige un **ID de cliente** único para esta conexión.
            3. Pulsa **Solicitar telemetría** para recibir el mensaje más reciente.

            Si el sensor publica con poca frecuencia, es posible que
            tengas que esperar a su próxima transmisión.
            """
        )


def meta_para(clave: str) -> tuple[str, str]:
    """Devuelve (icono, unidad) según el nombre de la variable."""
    nombre = clave.lower()
    for fragmento, meta in SENSOR_META.items():
        if fragmento in nombre:
            return meta
    return ("📈", "")


def formatear_valor(valor) -> str:
    if isinstance(valor, float):
        return f"{valor:.2f}".rstrip("0").rstrip(".")
    return str(valor)


def render_tarjetas(data: dict) -> None:
    """Muestra cada campo del JSON como un panel de telemetría."""
    tarjetas = []
    for clave, valor in data.items():
        icono, unidad = meta_para(str(clave))
        unidad_html = f'<span class="unidad">{unidad}</span>' if unidad else ""
        tarjetas.append(
            f"""
            <div class="lectura">
                <div class="etiqueta">{icono} {escape(str(clave))}</div>
                <div class="valor">{escape(formatear_valor(valor))}{unidad_html}</div>
            </div>
            """
        )
    st.markdown(
        f'<div class="rejilla">{"".join(tarjetas)}</div>',
        unsafe_allow_html=True,
    )


def render_estado_vacio() -> None:
    st.markdown(
        """
        <div class="vacio">
            <strong>Sin señal</strong>
            Pulsa «Solicitar telemetría» para abrir el enlace y recibir el último mensaje.
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_falla(mensaje: str) -> None:
    st.markdown(
        f"""
        <div class="falla">
            <strong>No se pudo obtener la telemetría</strong>
            {escape(mensaje)}
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_resultados() -> None:
    data = st.session_state.sensor_data

    if data is None:
        render_estado_vacio()
        return

    if isinstance(data, dict) and "error" in data:
        render_falla(data["error"])
        return

    st.subheader("Última telemetría")

    ultima = st.session_state.last_update
    if ultima:
        st.caption(f"Recibida el {ultima:%d/%m/%Y} a las {ultima:%H:%M:%S}")

    if isinstance(data, dict):
        render_tarjetas(data)
        with st.expander("Ver JSON completo"):
            st.json(data)
    else:
        st.info("El mensaje recibido no es JSON; se muestra como texto.")
        st.code(data, language=None)


# ═════════════════════════════════════════════════════════════════════════════
# FLUJO PRINCIPAL
# ═════════════════════════════════════════════════════════════════════════════
def main() -> None:
    configurar_pagina()
    inyectar_estilos()
    inicializar_estado()

    config = render_sidebar()
    render_encabezado(config["topic"])
    render_ayuda()
    st.write("")

    if st.button("Solicitar telemetría", use_container_width=True, type="primary"):
        if not config["broker"] or not config["topic"]:
            st.warning("Completa el broker y el tópico para continuar.")
        else:
            with st.spinner("Estableciendo enlace con el broker…"):
                st.session_state.sensor_data = obtener_mensaje_mqtt(
                    config["broker"],
                    config["port"],
                    config["topic"],
                    config["client_id"],
                )
                st.session_state.last_update = datetime.now()
            st.rerun()

    st.write("")
    render_resultados()


if __name__ == "__main__":
    main()
