import json
import os
from pathlib import Path

import boto3
import hvac
from dagster import ConfigurableResource
from pydantic import PrivateAttr


class Vault(ConfigurableResource):
    vault_addr: str
    vault_token: str | None = None
    vault_role: str | None = None
    # can be one of ["github", "aws-iam", "token", "kubernetes", "oidc", "jwt"]
    vault_auth_type: str = "kubernetes"
    auth_mount: str | None = None
    verify_tls: bool = True
    _client: hvac.Client = PrivateAttr(default=None)

    def _auth_aws_iam(self):
        self._initialize_client()
        session = boto3.Session()
        credentials: boto3.iam.Credentials = session.get_credentials()

        self._client.auth.aws.iam_login(
            credentials.access_key,
            credentials.secret_key,
            credentials.token,
            use_token=True,
            role=self.vault_role,
            mount_point=self.auth_mount or "aws",
        )

    def _auth_github(self):
        self._initialize_client()
        gh_token = os.environ.get("GITHUB_TOKEN")
        self._client.auth.github.login(
            token=gh_token,
            use_token=True,
            mount_point=self.auth_mount or "github",
        )

    def _auth_kubernetes(self):
        self._initialize_client()
        with Path("/var/run/secrets/kubernetes.io/serviceaccount/token").open() as f:
            jwt = f.read()
        self._client.auth.kubernetes.login(
            role=self.vault_role,
            jwt=jwt,
            use_token=True,
            mount_point=self.auth_mount or "kubernetes",
        )

    def _get_token_cache_path(self) -> Path:
        """Get the path to the token cache file."""
        cache_dir = Path.home() / ".vault" / "token_cache"
        cache_dir.mkdir(parents=True, exist_ok=True)
        # Use vault_addr and role to create unique cache file
        cache_key = f"{self.vault_addr}_{self.vault_role or 'default'}".replace(
            "/", "_"
        ).replace(":", "_")
        return cache_dir / f"{cache_key}.json"

    def _load_cached_token(self) -> str | None:
        """Load a cached token if it exists and is still valid."""
        cache_path = self._get_token_cache_path()
        if not cache_path.exists():
            return None

        try:
            with cache_path.open() as f:
                cache_data = json.load(f)

            cached_token = cache_data.get("token")
            if not cached_token:
                return None

            # Try using the cached token
            self._client.token = cached_token

            # Check if token is still valid by making a simple API call
            if self._client.is_authenticated():
                return cached_token

            # Token is invalid, remove cache file
            cache_path.unlink(missing_ok=True)

        except (json.JSONDecodeError, OSError):
            # Cache file is corrupted or unreadable, remove it
            cache_path.unlink(missing_ok=True)

        return None

    def _save_token_to_cache(self, token: str):
        """Save a token to the cache file."""
        cache_path = self._get_token_cache_path()
        try:
            with cache_path.open("w") as f:
                json.dump({"token": token}, f)
            # Set restrictive permissions (owner read/write only)
            cache_path.chmod(0o600)
        except OSError:
            # If we can't save the cache, continue anyway
            pass

    def _auth_jwt(self):
        """Authenticate using JWT/OIDC with a pre-obtained JWT token.

        This method is suitable for non-interactive environments where a JWT token
        is already available (e.g., from a file or environment variable).
        The token can be provided via:
        - VAULT_JWT_TOKEN environment variable
        - VAULT_JWT_TOKEN_PATH environment variable (path to file containing token)
        """
        self._initialize_client()
        jwt_token = os.environ.get("VAULT_JWT_TOKEN")
        if not jwt_token:
            # Try reading from a file path if provided
            jwt_token_path = os.environ.get("VAULT_JWT_TOKEN_PATH")
            if jwt_token_path:
                with Path(jwt_token_path).open() as f:
                    jwt_token = f.read().strip()

        if not jwt_token:
            err_msg = (
                "JWT token is required. Set VAULT_JWT_TOKEN"
                " or VAULT_JWT_TOKEN_PATH environment variable"
            )
            raise ValueError(err_msg)

        self._client.auth.oidc.jwt_login(
            role=self.vault_role,
            jwt=jwt_token,
            use_token=True,
            path=self.auth_mount or "oidc",
        )

    def _auth_oidc(self):
        """Authenticate using interactive OIDC (browser-based OAuth flow).

        This method requires an interactive environment with a web browser.
        It opens a browser window for authentication and handles the OAuth callback.

        Tokens are cached in ~/.vault/token_cache/ to avoid repeated browser flows.
        The cache is keyed by vault_addr and vault_role.

        Note: This method is not suitable for automated/non-interactive environments
        like Kubernetes pods or CI/CD pipelines. Use 'jwt' auth_type instead for
        those scenarios.
        """
        self._initialize_client()

        # Try to use a cached token first
        cached_token = self._load_cached_token()
        if cached_token:
            return

        # Import here to avoid dependency issues if not using interactive OIDC
        import urllib.parse  # noqa: PLC0415
        import webbrowser  # noqa: PLC0415
        from http.server import BaseHTTPRequestHandler, HTTPServer  # noqa: PLC0415

        # HTML page that auto-closes the browser window after successful auth
        SELF_CLOSING_PAGE = """
<!doctype html>
<html>
<head>
<script>
// Closes IE, Edge, Chrome, Brave
window.onload = function load() {
  window.open('', '_self', '');
  window.close();
};
</script>
</head>
<body>
  <p>Authentication successful, you can close the browser now.</p>
  <script>
    // Needed for Firefox security
    setTimeout(function() {
          window.close()
    }, 5000);
  </script>
</body>
</html>
"""

        # Configuration
        callback_port = int(os.environ.get("VAULT_OIDC_CALLBACK_PORT", "8250"))
        redirect_uri = os.environ.get(
            "VAULT_OIDC_REDIRECT_URI", f"http://localhost:{callback_port}/oidc/callback"
        )

        # Request authorization URL
        auth_url_response = self._client.auth.oidc.oidc_authorization_url_request(
            role=self.vault_role,
            redirect_uri=redirect_uri,
        )

        auth_url = auth_url_response.get("data", {}).get("auth_url", "")
        if not auth_url:
            err_msg = "Failed to get OIDC authorization URL from Vault"
            raise ValueError(err_msg)

        # Parse nonce and state from auth URL
        params = urllib.parse.parse_qs(auth_url.split("?")[1])
        auth_url_nonce = params["nonce"][0]
        auth_url_state = params["state"][0]

        # Open browser for authentication
        webbrowser.open(auth_url)

        # Start local HTTP server to handle callback
        class HttpServ(HTTPServer):
            def __init__(self, *args, **kwargs):
                HTTPServer.__init__(self, *args, **kwargs)
                self.code = None

        class AuthHandler(BaseHTTPRequestHandler):
            def do_GET(self):
                params = urllib.parse.parse_qs(self.path.split("?")[1])
                self.server.code = params["code"][0]  # type: ignore[attr-defined]
                self.send_response(200)
                self.end_headers()
                self.wfile.write(SELF_CLOSING_PAGE.encode())

            def log_message(self, format, *args):  # noqa: A002
                pass  # Suppress log messages

        server_address = ("", callback_port)
        httpd = HttpServ(server_address, AuthHandler)
        httpd.handle_request()

        if not httpd.code:
            err_msg = "Failed to receive authorization code from OIDC provider"
            raise ValueError(err_msg)

        # Complete the authentication
        auth_result = self._client.auth.oidc.oidc_callback(
            code=httpd.code,
            path=self.auth_mount or "oidc",
            nonce=auth_url_nonce,
            state=auth_url_state,
        )

        # Set the token
        client_token = auth_result.get("auth", {}).get("client_token")
        if client_token:
            self._client.token = client_token
            # Cache the token for future use
            self._save_token_to_cache(client_token)
        else:
            err_msg = "Failed to obtain client token from OIDC authentication"
            raise ValueError(err_msg)

    def authenticate(self):
        if self.vault_auth_type == "aws-iam":
            self._auth_aws_iam()
        elif self.vault_auth_type == "github":
            self._auth_github()
        elif self.vault_auth_type == "kubernetes":
            self._auth_kubernetes()
        elif self.vault_auth_type == "jwt":
            self._auth_jwt()
        elif self.vault_auth_type == "oidc":
            self._auth_oidc()
        elif self.vault_auth_type == "token":
            if not self.vault_token:
                err_msg = "Vault token is required for token authentication"
                raise ValueError(err_msg)
            self.client.token = self.vault_token
        else:
            err_msg = f"Invalid auth type: {self.vault_auth_type}"
            raise ValueError(err_msg)

    @property
    def client(self) -> hvac.Client:
        self._initialize_client()
        if not self._client.is_authenticated():
            self.authenticate()
        return self._client

    def _initialize_client(self):
        if self._client is None:
            self._client = hvac.Client(url=self.vault_addr, verify=self.verify_tls)
