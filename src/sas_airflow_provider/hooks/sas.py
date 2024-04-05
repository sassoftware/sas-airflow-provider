from __future__ import annotations

from airflow.hooks.base import BaseHook
import base64
import urllib.parse
import requests
import urllib3
from urllib3.exceptions import InsecureRequestWarning

class SasHook(BaseHook):
    """Hook to manage connection to SAS"""

    conn_name_attr = 'sas_conn_id'
    default_conn_name = 'sas_default'
    conn_type = 'sas'
    hook_name = 'SAS'

    def __init__(self, conn_id: str = None) -> None:
        super().__init__()
        self.client_secret = None
        self.client_id = None
        self.conn_id = conn_id
        self.host = None
        self.login = None
        self.password = None
        self.token = None
        self.sas_conn = None
        self.cert_verify = True
        self.grant_type = None

    def get_conn(self, http_timeout=None):
        """Returns a SAS connection."""
        if self.conn_id is None:
            self.conn_id = self.default_conn_name
        conn = self.get_connection(self.conn_id)
        self.host = conn.host
        self.login = conn.login
        self.password = conn.password

        extras = conn.extra_dejson
        self.token = extras.get("token")
        self.client_id = extras.get("client_id")
        self.grant_type = extras.get("grant_type", "password")
        self.client_secret = ""
        if not self.client_id:
            self.client_id = "sas.cli"
        else:
            self.client_secret = extras.get("client_secret")  # type: ignore

        self.cert_verify = extras.get("ssl_certificate_verification", True)
        if not self.cert_verify:
            self.log.info(f"TLS verification is turned off")
        elif isinstance(self.cert_verify, str):
            self.log.info("Using custom TLS CA certificate bundle file")

        if not self.sas_conn:
            self.sas_conn = self._create_session_for_connection(http_timeout=http_timeout)

        return self.sas_conn

    def _create_session_for_connection(self, http_timeout=None):
        self.log.info(f"Creating session for connection named %s to host %s",
                      self.conn_id,
                      self.host)

        if not self.cert_verify:
            # disable insecure HTTP requests warnings
            urllib3.disable_warnings(InsecureRequestWarning)

        if not self.token:
            # base 64 encode the api client auth and pass in authorization header
            auth_str = f"{self.client_id}:{self.client_secret}"
            auth_bytes = auth_str.encode("ascii")
            auth_header = base64.b64encode(auth_bytes).decode("ascii")
            my_headers = {"Authorization": f"Basic {auth_header}"}

            payload = {"grant_type": self.grant_type}
            if self.login:
                payload["username"] = self.login
                payload["password"] = self.password

            self.log.info("Get oauth token (see the the https://github.com/sassoftware/sas-airflow-provider?tab=readme-ov-file#create-a-connection-to-sas if this crashes)")
            response = requests.post(
                    f"{self.host}/SASLogon/oauth/token", 
                    data=payload, 
                    verify=self.cert_verify,
                    headers=my_headers,
                    timeout=http_timeout
            )
            if response.status_code != 200:
                raise RuntimeError(f"Get token failed with status code: {response.status_code}") 
                
            r = response.json()
            self.token = r["access_token"]

        session = requests.Session()

        # set up standard headers
        session.headers.update({"Authorization": f"bearer {self.token}"})
        session.headers.update({"Accept": "application/json"})
        session.headers.update({"Content-Type": "application/json"})

        # set to false if using self-signed certs
        session.verify = self.cert_verify

        # prepend the root url for all operations on the session, so that consumers can just provide
        # resource uri without the protocol and host
        root_url = self.host
        session.get = lambda *args, **kwargs: requests.Session.get(  # type: ignore
            session, urllib.parse.urljoin(root_url, args[0]), *args[1:], **kwargs
        )
        session.post = lambda *args, **kwargs: requests.Session.post(  # type: ignore
            session, urllib.parse.urljoin(root_url, args[0]), *args[1:], **kwargs
        )
        session.put = lambda *args, **kwargs: requests.Session.put(  # type: ignore
            session, urllib.parse.urljoin(root_url, args[0]), *args[1:], **kwargs
        )
        session.delete = lambda *args, **kwargs: requests.Session.delete(  # type: ignore
            session, urllib.parse.urljoin(root_url, args[0]), *args[1:], **kwargs
        )
        return session
