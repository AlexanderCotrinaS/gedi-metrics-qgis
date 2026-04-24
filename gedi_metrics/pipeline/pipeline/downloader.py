import os
import requests
import getpass
import netrc

class SessionNASA(requests.Session):
    """
    Simple EarthData session using requests + .netrc/_netrc credentials.
    """

    AUTH_HOST = "urs.earthdata.nasa.gov"

    def __init__(self, username=None, password=None):
        super().__init__()
        self.username, self.password = self._load_credentials(username, password)
        self.auth = (self.username, self.password)

    def _load_credentials(self, username, password):
        if username and password:
            return username, password

        # Try .netrc / _netrc
        netrc_paths = [os.path.expanduser("~/.netrc"), os.path.expanduser("~/_netrc")]
        for path in netrc_paths:
            if os.path.exists(path):
                try:
                    creds = netrc.netrc(path).authenticators(self.AUTH_HOST)
                    if creds:
                        return creds[0], creds[2]
                except Exception:
                    continue

        # Fallback to prompt (CLI use; in GUI we expect netrc to exist)
        print("[Downloader] Introduce your credentials to access the Data Repository")
        user = input("Username : ")
        pwd = getpass.getpass()
        return user, pwd

    def rebuild_auth(self, prepared_request, response):
        """
        Strip auth on redirects to non-URS hosts to avoid leaking credentials.
        """
        headers = prepared_request.headers
        url = prepared_request.url
        if "Authorization" in headers:
            original_parsed = requests.utils.urlparse(response.request.url)
            redirect_parsed = requests.utils.urlparse(url)
            if (
                original_parsed.hostname != redirect_parsed.hostname
                and redirect_parsed.hostname != self.AUTH_HOST
                and original_parsed.hostname != self.AUTH_HOST
            ):
                del headers["Authorization"]
        return

class GEDIDownloader:
	"""
	The GEDIDownloader :class: implements a downloading mechanism for a given NASA Repository link, while keeping
	an authorization session alive.

	It implements a file chunk downloading mechanism and a file checking step to skip a download or not.

	Args:
		persist_login: Choice to persist login and save to a .netrc file. See Earthdata Access API for more info:
					   https://earthaccess.readthedocs.io/en/latest/howto/authenticate/
		save_path: Absolute path to save the downloaded files. If None, saves to current working directory (script).
	"""

	def __init__(self, persist_login=False, save_path=None):
		self.save_path = save_path if save_path is not None else ""
		print("Logging in EarthData...")
		# earthaccess is unavailable for QGIS; rely on requests + netrc.
		self.session = SessionNASA()
	
	def __download(self, content, save_path):
		"""Download"""
		written = 0
		with open(save_path, "wb") as file:
			for chunk in content:
				# Filter out keep alive chunks
				if not chunk:
					continue
				file.write(chunk)
				written += len(chunk)

	def __precheck_file(self, file_path, size):
		"""
		Prechecking file mechanism function - if not exists or is corrupted (not equal to the download size), it downloads the file.
		"""
		# File does not exist in save_path
		if not os.path.exists(file_path):
			print(f"[Downloader] Downloading granule and saving \"{file_path}\"...")
			return False

		# File exists but not complete, restart download
		if os.path.getsize(file_path) != size:
			print(f"[Downloader] File at \"{file_path}\" exists but corrupted. Downloading again...")
			# Delete file and restart download
			os.remove(file_path)
			return False

		# File exists and complete, skip download
		print(f"[Downloader] File at \"{file_path}\" exists. Skipping download...")
		return True


	def download_granule(self, url, chunk_size=128):
		"""
		This function downloads the file from a given URL. Must keep a Login Session alive.
		Args:
			url: NASA Repo URL to download the file.
			chunk_size: Specify chunk size for download in kilobytes. Defaults to 128 KB.
		"""
		filename = url.split("/")[-1]

		# If even the filename does not have "GEDI" in it, do not download
		if not "GEDI" in filename:
			print(f"[Downloader] Invalid URL {url}. Please check URL and download again.")
			return False

		file_path = os.path.join(self.save_path, filename)
		chunk_size = chunk_size * 1024 # KB chunk

		http_response = self.session.get(url, stream=True)

		# If http response other than OK 200, user needs to check credentials
		if not http_response.ok:
			print(f"[Downloader] Invalid credentials for Login session. You may want to delete the credentials on the '.netrc' file and start over.")
			return False

		response_length = http_response.headers.get('content-length')
		if response_length is None:
			print("[Downloader] Missing content-length header; skipping download.")
			return False

		# If file not exists, download
		if not self.__precheck_file(file_path, int(response_length)):
			self.__download(http_response.iter_content(chunk_size=chunk_size), file_path)

		# Check file integrity / if it downloaded correctly
		if not os.path.getsize(file_path) == int(response_length):
			# If not downloaded correctly, send message for download retry
			return False

		return True

	def download_files(self, files_url):
		"""
		This function downloads a list of files with given URLs. Must keep a Login Session alive.
		Args:
			files_url: A list containing GEDI files URLs from EarthData Repository
		"""

		# Start download for every granule
		for g in files_url:
			if not self.download_granule(g[0]):
				retries = 3
				print(f"[Downloader] Fail download for link {g}. Retrying...")
				while retries > 0:
					print(f"Retry {retries}")
					if self.download_granule(g[0]):
						break
					retries -= 1
				if retries == 0:
					print(f"[Downloader] Fail download for link {g}. Skipping...")
		return files_url
