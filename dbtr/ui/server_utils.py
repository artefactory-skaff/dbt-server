from http.server import SimpleHTTPRequestHandler
import socketserver
import threading
import streamlit as st

from dbtr.common.remote_server import DbtServer
from dbtr.cli.cloud_providers.gcp import get_auth_token  # TODO: multicloud support

@st.cache_resource
def get_server(url: str):
    return DbtServer(server_url=url, token_generator=get_auth_token)

class ProxyHTTPRequestHandler(SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        self.server_instance = kwargs.pop('server_instance', None)
        super().__init__(*args, **kwargs)

    def do_GET(self):
        server = self.server_instance
        server_url = server.server_url
        url = f"{server_url}{self.path}"
        response = server.session.get(url)
        self.send_response(response.status_code)
        for key, value in response.headers.items():
            self.send_header(key, value)
        self.end_headers()
        self.wfile.write(response.content)

def run_proxy_server(server_instance, port):
    while True:
        try:
            handler = lambda *args, **kwargs: ProxyHTTPRequestHandler(*args, server_instance=server_instance, **kwargs)
            with socketserver.TCPServer(("", port), handler) as httpd:
                httpd.serve_forever()
        except OSError as e:
            if e.errno == 48:  # Address already in use
                port += 1
            else:
                raise

def start_threaded_proxy_server(port=6847):
    if 'proxy_thread' not in st.session_state:
        server: DbtServer = st.session_state["server"]
        st.session_state['proxy_thread'] = threading.Thread(target=run_proxy_server, args=(server, port))
        st.session_state['proxy_thread'].daemon = True
        st.session_state['proxy_thread'].start()
    return f"http://localhost:{port}"
