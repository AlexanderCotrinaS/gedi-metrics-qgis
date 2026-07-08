"""
GEDIMetrics v1.0.3 — downloader.py

Authentication strategy (in priority order):
  1. Bearer Token  — user pastes a NASA EarthData token in the GUI.
                     Uses ONLY data.lpdaac.earthdatacloud.nasa.gov (CloudFront).
                     NO connection to urs.earthdata.nasa.gov needed.
                     ✓ Works on university / corporate firewalls.

  2. netrc / Basic — legacy fallback: username + password via OAuth redirect.
                     Requires direct access to urs.earthdata.nasa.gov:443.
                     Only used when no token is provided.

Other fixes carried from v1.0.2:
  - Explicit (connect, read) timeouts on all requests
  - HTTPAdapter with exponential-backoff Retry
  - Pre-flight TCP check with actionable error messages
  - Proxy support (auto-detect OS proxy or manual URL)
"""

import os
import socket
import requests
import getpass
import netrc
from urllib.parse import urlparse
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ── Timeout constants ──────────────────────────────────────────────────────────
CONNECT_TIMEOUT = 20
READ_TIMEOUT = 360
PREFLIGHT_TIMEOUT = 8

# ── Retry constants ────────────────────────────────────────────────────────────
RETRY_TOTAL = 5
RETRY_BACKOFF = 1.5
RETRY_STATUS_LIST = [429, 500, 502, 503, 504]


def _make_retry():
    """Build a Retry object compatible with both old and new urllib3.

    urllib3 >= 1.26 renamed ``method_whitelist`` to ``allowed_methods``.
    Older versions (shipped with some macOS / system Pythons) only accept
    the former. We try the modern name first and fall back silently.
    """
    common = dict(
        total=RETRY_TOTAL,
        backoff_factor=RETRY_BACKOFF,
        status_forcelist=RETRY_STATUS_LIST,
        raise_on_status=False,
    )
    try:
        return Retry(allowed_methods=["GET", "HEAD"], **common)
    except TypeError:
        return Retry(method_whitelist=["GET", "HEAD"], **common)


# ── Connectivity probe ─────────────────────────────────────────────────────────
def _check_host_reachable(host: str, port: int = 443,
                          timeout: int = PREFLIGHT_TIMEOUT) -> bool:
    try:
        sock = socket.create_connection((host, port), timeout=timeout)
        sock.close()
        return True
    except (socket.timeout, OSError):
        return False


# ── Proxy builder ──────────────────────────────────────────────────────────────
def _build_proxy_dict(proxy_url=None, proxy_user=None,
                      proxy_pass=None, auto_detect=True) -> dict:
    if proxy_url and proxy_url.strip():
        url = proxy_url.strip()
        if proxy_user and proxy_pass:
            p = urlparse(url)
            url = f"{p.scheme}://{proxy_user}:{proxy_pass}@{p.netloc}{p.path}"
        return {"https": url, "http": url}
    if auto_detect:
        import urllib.request
        detected = urllib.request.getproxies()
        if detected:
            print(f"[Proxy] System proxy detected: {detected}")
            return detected
    return {}


# ── Token-based session (no URS contact needed) ────────────────────────────────
class SessionToken(requests.Session):
    """
    Authenticates every request with:
        Authorization: Bearer <token>

    The token is obtained once from urs.earthdata.nasa.gov/user_tokens
    by the USER in their browser — the plugin never contacts URS at runtime.
    All actual data downloads go to *.earthdatacloud.nasa.gov (CloudFront/AWS),
    which is reachable from university networks.
    """

    def __init__(self, token: str, proxy_url=None, proxy_user=None,
                 proxy_pass=None, proxy_auto=True):
        super().__init__()
        self.headers.update({"Authorization": f"Bearer {token}"})
        self._configure(proxy_url, proxy_user, proxy_pass, proxy_auto)

    def _configure(self, proxy_url, proxy_user, proxy_pass, proxy_auto):
        proxy_dict = _build_proxy_dict(proxy_url, proxy_user, proxy_pass, proxy_auto)
        if proxy_dict:
            self.proxies.update(proxy_dict)
            print(f"[Downloader] Proxy: {list(proxy_dict.values())[0]}")

        retry = _make_retry()
        adapter = HTTPAdapter(max_retries=retry)
        self.mount("https://", adapter)
        self.mount("http://", adapter)

    def get(self, url, **kwargs):
        kwargs.setdefault("timeout", (CONNECT_TIMEOUT, READ_TIMEOUT))
        return super().get(url, **kwargs)

    def rebuild_auth(self, prepared_request, response):
        """Keep Bearer token on redirects within *.nasa.gov; strip elsewhere."""
        url = prepared_request.url
        if url and "nasa.gov" not in url:
            prepared_request.headers.pop("Authorization", None)


# ── netrc / Basic session (legacy, requires URS access) ───────────────────────
class SessionNASA(requests.Session):
    """
    Legacy Basic-auth session via .netrc.
    Requires direct TCP access to urs.earthdata.nasa.gov:443.
    Used only when no Bearer token is provided.
    """

    AUTH_HOST = "urs.earthdata.nasa.gov"

    def __init__(self, username=None, password=None,
                 proxy_url=None, proxy_user=None,
                 proxy_pass=None, proxy_auto=True):
        super().__init__()
        self.username, self.password = self._load_credentials(username, password)
        self.auth = (self.username, self.password)

        proxy_dict = _build_proxy_dict(proxy_url, proxy_user, proxy_pass, proxy_auto)
        if proxy_dict:
            self.proxies.update(proxy_dict)

        retry = _make_retry()
        adapter = HTTPAdapter(max_retries=retry)
        self.mount("https://", adapter)
        self.mount("http://", adapter)

    def _load_credentials(self, username, password):
        if username and password:
            return username, password
        for path in [os.path.expanduser("~/.netrc"),
                     os.path.expanduser("~/_netrc")]:
            if os.path.exists(path):
                try:
                    creds = netrc.netrc(path).authenticators(self.AUTH_HOST)
                    if creds:
                        print(f"[Auth] Credentials from {path}")
                        return creds[0], creds[2]
                except Exception as exc:
                    print(f"[Auth] Could not parse {path}: {exc}")
        print("[Downloader] No .netrc credentials — prompting.")
        user = input("EarthData username : ")
        pwd = getpass.getpass("EarthData password : ")
        return user, pwd

    def get(self, url, **kwargs):
        kwargs.setdefault("timeout", (CONNECT_TIMEOUT, READ_TIMEOUT))
        return super().get(url, **kwargs)

    def rebuild_auth(self, prepared_request, response):
        headers = prepared_request.headers
        url = prepared_request.url
        if "Authorization" in headers:
            orig = requests.utils.urlparse(response.request.url).hostname
            redir = requests.utils.urlparse(url).hostname
            if orig != redir and redir != self.AUTH_HOST and orig != self.AUTH_HOST:
                del headers["Authorization"]


# ── GEDIDownloader ─────────────────────────────────────────────────────────────
class GEDIDownloader:
    """
    Downloads GEDI HDF5 granules from NASA EarthData.

    v1.0.3 — Bearer Token authentication
    ──────────────────────────────────────
    University / corporate firewalls block urs.earthdata.nasa.gov (IP
    198.118.243.33, not on CloudFront). Bearer Token authentication avoids
    that host entirely: the user generates a token once in their browser
    and pastes it into the plugin. All downloads then use only
    *.earthdatacloud.nasa.gov (CloudFront IPs), which universities allow.

    Auth priority:
        1. bearer_token  → SessionToken  (no URS contact, firewall-safe)
        2. username/pwd  → SessionNASA   (legacy, needs URS access)

    Args:
        save_path     : directory for downloaded .h5 files
        bearer_token  : NASA EarthData Bearer token (recommended)
        username      : EarthData username (fallback if no token)
        password      : EarthData password (fallback if no token)
        proxy_url     : manual proxy URL
        proxy_user    : proxy username
        proxy_pass    : proxy password
        proxy_auto    : auto-detect OS proxy settings
    """

    def __init__(self, persist_login=False, save_path=None,
                 bearer_token=None,
                 username=None, password=None,
                 proxy_url=None, proxy_user=None, proxy_pass=None,
                 proxy_auto=True):

        self.save_path = save_path or ""

        if bearer_token and bearer_token.strip():
            # ── Token auth path ────────────────────────────────────────────────
            print("[Downloader] Auth mode: Bearer Token (firewall-safe).")
            print("[Downloader] Skipping URS check — token auth does not need it.")
            self.session = SessionToken(
                token=bearer_token.strip(),
                proxy_url=proxy_url,
                proxy_user=proxy_user,
                proxy_pass=proxy_pass,
                proxy_auto=proxy_auto,
            )
        else:
            # ── Legacy netrc/Basic auth path ───────────────────────────────────
            print("[Downloader] Auth mode: username/password (legacy).")
            print("[Downloader] Checking connectivity to urs.earthdata.nasa.gov ...")
            if not _check_host_reachable("urs.earthdata.nasa.gov"):
                print(
                    "\n[Downloader] ⚠  Cannot reach urs.earthdata.nasa.gov:443\n"
                    "\n"
                    "  Your network (university / corporate firewall) blocks this host.\n"
                    "  USERNAME + PASSWORD WILL NOT WORK.\n"
                    "\n"
                    "  ✓  SOLUTION — use a Bearer Token instead:\n"
                    "    1. Open in your browser (it works even if curl cannot):\n"
                    "       https://urs.earthdata.nasa.gov/user_tokens\n"
                    "    2. Click 'Generate Token'\n"
                    "    3. Copy the token\n"
                    "    4. Paste it in GEDIMetrics → EarthData tab → 'Bearer Token'\n"
                    "\n"
                    "  The plugin will still try, but downloads will likely fail.\n"
                )
            else:
                print("[Downloader] ✓  URS reachable — using username/password.")

            self.session = SessionNASA(
                username=username,
                password=password,
                proxy_url=proxy_url,
                proxy_user=proxy_user,
                proxy_pass=proxy_pass,
                proxy_auto=proxy_auto,
            )

    # ── private ───────────────────────────────────────────────────────────────
    def __write_chunks(self, content_iter, save_path):
        with open(save_path, "wb") as fh:
            for chunk in content_iter:
                if chunk:
                    fh.write(chunk)

    def __precheck_file(self, file_path, expected_size):
        name = os.path.basename(file_path)
        if not os.path.exists(file_path):
            print(f"[Downloader] → {name}")
            return False
        actual = os.path.getsize(file_path)
        if actual != expected_size:
            print(f"[Downloader] Incomplete ({actual}/{expected_size} B), retrying: {name}")
            os.remove(file_path)
            return False
        print(f"[Downloader] Already complete, skipping: {name}")
        return True

    # ── public ────────────────────────────────────────────────────────────────
    def download_granule(self, url: str, chunk_size_kb: int = 256) -> bool:
        filename = url.split("/")[-1]
        file_path = os.path.join(self.save_path, filename)

        if "GEDI" not in filename:
            print(f"[Downloader] Invalid URL: {url}")
            return False

        try:
            resp = self.session.get(url, stream=True)
        except requests.exceptions.ConnectTimeout:
            print(f"[Downloader] Connect timeout: {filename}")
            return False
        except requests.exceptions.ProxyError as exc:
            print(f"[Downloader] Proxy error: {exc}")
            return False
        except requests.exceptions.ReadTimeout:
            print(f"[Downloader] Read timeout: {filename}")
            return False
        except requests.exceptions.ConnectionError as exc:
            print(f"[Downloader] Connection error: {exc}")
            return False

        if resp.status_code == 401:
            print(
                f"[Downloader] HTTP 401 Unauthorized: {filename}\n"
                "  → Token may be expired. Generate a new one at:\n"
                "    https://urs.earthdata.nasa.gov/user_tokens"
            )
            return False

        if not resp.ok:
            print(f"[Downloader] HTTP {resp.status_code}: {filename}")
            return False

        content_length = resp.headers.get("content-length")
        if not content_length:
            print(f"[Downloader] No content-length, skipping: {filename}")
            return False

        expected = int(content_length)
        if self.__precheck_file(file_path, expected):
            return True

        self.__write_chunks(resp.iter_content(chunk_size=chunk_size_kb * 1024), file_path)

        actual = os.path.getsize(file_path)
        if actual != expected:
            print(f"[Downloader] Size mismatch ({actual}/{expected}): {filename}")
            return False
        return True

    def download_files(self, files_url: list, max_retries: int = 3) -> list:
        for url, _ in files_url:
            if self.download_granule(url):
                continue
            print(f"[Downloader] Retrying: {url.split('/')[-1]}")
            for attempt in range(1, max_retries + 1):
                print(f"[Downloader]   attempt {attempt}/{max_retries}")
                if self.download_granule(url):
                    break
            else:
                print(f"[Downloader] Giving up after {max_retries} retries.")
        return files_url
