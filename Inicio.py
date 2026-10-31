"""
Lector de Sensor MQTT
---------------------
App de Streamlit que se conecta a un broker MQTT, se suscribe a un tópico
y muestra el último mensaje recibido en tarjetas de lectura.

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
        page_title="Lector de Sensor MQTT",
        page_icon="📡",
        layout="centered",
    )


def inyectar_estilos() -> None:
    st.markdown(
        """
        <style>
        @import url('https://fonts.googleapis.com/css2?family=Sora:wght@500;600;700&family=Nunito+Sans:wght@400;600;700&display=swap');

        :root {
            --tinta:   #12263A;
            --pizarra: #5B6B7A;
            --niebla:  #EEF4F7;
            --linea:   #D5E1E8;
            --senal:   #14938E;
            --senal-2: #0E6F6B;
            --alerta:  #D9534F;
        }

        html, body, [class*="css"], .stApp {
            font-family: 'Nunito Sans', sans-serif;
        }
        .stApp { background: var(--niebla); }
        .block-container { padding-top: 2.5rem; max-width: 760px; }

        h1, h2, h3, .titulo {
            font-family: 'Sora', sans-serif !important;
            color: var(--tinta);
            letter-spacing: -0.01em;
        }

        /* ── Encabezado ─────────────────────────────── */
        .encabezado {
            display: flex; align-items: center; gap: 1rem;
            margin-bottom: 0.25rem;
        }
        .encabezado .icono {
            width: 54px; height: 54px; border-radius: 16px;
            display: grid; place-items: center; font-size: 1.7rem;
            background: var(--tinta); color: white;
        }
        .encabezado h1 { margin: 0; padding: 0; font-size: 1.9rem; }
        .subtitulo { color: var(--pizarra); margin: 0 0 1.5rem 0; }

        /* ── Chip del tópico ────────────────────────── */
        .chip {
            display: inline-flex; align-items: center; gap: .45rem;
            padding: .3rem .8rem; border-radius: 999px;
            background: white; border: 1px solid var(--linea);
            color: var(--tinta); font-size: .85rem; font-weight: 600;
        }
        .punto {
            width: 8px; height: 8px; border-radius: 50%;
            background: var(--senal);
        }
        .punto.inactivo { background: #B8C4CC; }
        .punto.error    { background: var(--alerta); }

        /* ── Tarjetas de lectura ────────────────────── */
        .rejilla {
            display: grid; gap: 1rem; margin: 1rem 0 .5rem 0;
            grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
        }
        .lectura {
            background: white; border: 1px solid var(--linea);
            border-radius: 18px; padding: 1.2rem 1.3rem;
            border-top: 4px solid var(--senal);
        }
        .lectura .etiqueta {
            color: var(--pizarra); font-size: .95rem; font-weight: 600;
            display: flex; align-items: center; gap: .4rem;
        }
        .lectura .valor {
            font-family: 'Sora', sans-serif; font-weight: 700;
            font-size: 2.2rem; color: var(--tinta); line-height: 1.15;
            margin-top: .35rem;
        }
        .lectura .unidad {
            font-size: 1rem; font-weight: 500; color: var(--pizarra);
            margin-left: .25rem;
        }

        /* ── Estados vacío / error ──────────────────── */
        .vacio {
            background: white; border: 1.5px dashed var(--linea);
            border-radius: 18px; padding: 2.2rem 1.5rem;
            text-align: center; color: var(--pizarra);
        }
        .vacio strong { color: var(--tinta); display: block; margin-bottom: .3rem; }

        /* ── Botón principal ────────────────────────── */
        .stButton > button {
            background: var(--senal); color: white; border: none;
            border-radius: 14px; padding: .75rem 1rem;
            font-family: 'Sora', sans-serif; font-weight: 600;
            transition: background .15s ease;
        }
        .stButton > button:hover  { background: var(--senal-2); color: white; }
        .stButton > button:focus-visible {
            outline: 3px solid var(--tinta); outline-offset: 2px;
        }

        /* ── Barra lateral ──────────────────────────── */
        section[data-testid="stSidebar"] {
            background: white; border-right: 1px solid var(--linea);
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

    Retorna el payload (dict o str), un dict {"error": ...} si falla,
    o {"error": "..."} si se agota el tiempo de espera.
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
        st.subheader("⚙️ Conexión")

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

        st.caption("Brokers públicos para pruebas:")
        st.code("\n".join(PUBLIC_BROKERS), language=None)

    return {
        "broker": broker.strip(),
        "port": int(port),
        "topic": topic.strip(),
        "client_id": client_id.strip(),
    }


def render_encabezado(topic: str) -> None:
    st.markdown(
        """
        <div class="encabezado">
            <div class="icono">📡</div>
            <h1>Lector de sensor MQTT</h1>
        </div>
        <p class="subtitulo">Consulta la última lectura publicada en tu tópico.</p>
        """,
        unsafe_allow_html=True,
    )

    data = st.session_state.sensor_data
    if data is None:
        estado = "inactivo"
    elif isinstance(data, dict) and "error" in data:
        estado = "error"
    else:
        estado = ""

    st.markdown(
        f'<span class="chip"><span class="punto {estado}"></span>'
        f"{escape(topic or 'sin tópico')}</span>",
        unsafe_allow_html=True,
    )


def render_ayuda() -> None:
    with st.expander("ℹ️ Cómo usar la app"):
        st.markdown(
            """
            1. Completa los datos del **broker**, **puerto** y **tópico** en la barra lateral.
            2. Elige un **ID de cliente** único para esta conexión.
            3. Pulsa **Obtener datos** para recibir el mensaje más reciente.

            Si el sensor publica con poca frecuencia, es posible que
            tengas que esperar a su próxima publicación.
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
    """Muestra cada campo del JSON como una tarjeta de lectura."""
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
            <strong>Aún no hay lecturas</strong>
            Pulsa «Obtener datos» para conectarte y recibir el último mensaje.
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_resultados() -> None:
    data = st.session_state.sensor_data

    if data is None:
        render_estado_vacio()
        return

    st.subheader("Última lectura")

    if isinstance(data, dict) and "error" in data:
        st.error(f"No se pudo obtener el dato. {data['error']}")
        return

    ultima = st.session_state.last_update
    if ultima:
        st.caption(f"Actualizado el {ultima:%d/%m/%Y} a las {ultima:%H:%M:%S}")

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
    st.write("")
    render_ayuda()

    if st.button("Obtener datos", use_container_width=True, type="primary"):
        if not config["broker"] or not config["topic"]:
            st.warning("Completa el broker y el tópico para continuar.")
        else:
            with st.spinner("Conectando al broker y esperando datos…"):
                st.session_state.sensor_data = obtener_mensaje_mqtt(
                    config["broker"],
                    config["port"],
                    config["topic"],
                    config["client_id"],
                )
                st.session_state.last_update = datetime.now()
            st.rerun()

    render_resultados()


if __name__ == "__main__":
    main()
