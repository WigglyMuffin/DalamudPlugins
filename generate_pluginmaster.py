import json
import os
import requests
from pathlib import Path
from zipfile import ZipFile
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass


@dataclass
class Config:
    """Configuration settings for the plugin master generator."""
    branch: str
    plugins_dir: Path
    output_files: Dict[str, Path]
    plugin_outputs: Dict[str, str]
    repository_list: Dict[str, Dict[str, str]]
    external_plugins: Dict[str, Dict[str, str]]
    download_urls: Dict[str, str]
    required_manifest_keys: List[str]
    field_duplicates: Dict[str, List[str]]
    plugin_aliases: Dict[str, Dict[str, Any]]
    repo: str = "WigglyMuffin/DalamudPlugins"
    global_api_level: int = 13

    @classmethod
    def _load_plugin_sources(cls) -> Tuple[Dict[str, Path], Dict[str, Dict[str, str]], Dict[str, str]]:
        """Load plugin sources from plugin-sources.json if it exists.

        Returns (output_files, repository_list, plugin_outputs).
        Falls back to hardcoded defaults if file missing or broken.
        """
        default_output_files = {"default": Path("./pluginmaster.json")}

        sources_path = Path("./plugin-sources.json")
        if not sources_path.exists():
            print("plugin-sources.json not found, no repository plugins will be processed")
            return default_output_files, {}, {}

        try:
            with open(sources_path, 'r', encoding='utf-8') as f:
                sources = json.load(f)

            output_files = {}
            for key, filename in sources.get("outputs", {}).items():
                output_files[key] = Path(f"./{filename}")
            if not output_files:
                output_files = default_output_files

            repository_list = {}
            plugin_outputs = {}
            for plugin_name, plugin_config in sources.get("plugins", {}).items():
                if not plugin_config.get("enabled", True):
                    print(f"Skipping disabled plugin: {plugin_name}")
                    continue
                repository_list[plugin_name] = {
                    "url": plugin_config["url"],
                    "token": plugin_config.get("token", "GITHUB_TOKEN")
                }
                plugin_outputs[plugin_name] = plugin_config.get("output", "default")

            print(f"Loaded {len(repository_list)} plugins from plugin-sources.json with {len(output_files)} output(s)")
            return output_files, repository_list, plugin_outputs

        except Exception as e:
            print(f"Error loading plugin-sources.json: {e}, no repository plugins will be processed")
            return default_output_files, {}, {}

    @classmethod
    def load_default(cls) -> 'Config':
        """Load default configuration."""
        branch = os.environ.get("GITHUB_REF", "main").split("refs/heads/")[-1]
        base_url = "https://github.com/WigglyMuffin/DalamudPlugins/raw/{branch}/plugins/{plugin_name}"

        output_files, repository_list, plugin_outputs = cls._load_plugin_sources()

        plugin_aliases = {}

        return cls(
            branch=branch,
            plugins_dir=Path("./plugins"),
            output_files=output_files,
            plugin_outputs=plugin_outputs,
            repository_list=repository_list,
            external_plugins={},
            download_urls={
                "main": f"{base_url}/latest.zip",
                "testing": f"{base_url}/testing/latest.zip",
                "global": f"{base_url}/global/latest.zip"
            },
            required_manifest_keys=[
                "Author", "Name", "Punchline", "Description", "Tags",
                "InternalName", "RepoUrl", "Changelog", "AssemblyVersion",
                "ApplicableVersion", "DalamudApiLevel", "TestingAssemblyVersion",
                "TestingDalamudApiLevel", "IconUrl", "ImageUrls", "LastUpdate",
                "DownloadCount", "DownloadLinkInstall", "DownloadLinkUpdate", "DownloadLinkTesting"
            ],
            field_duplicates={
                "DownloadLinkInstall": ["DownloadLinkUpdate"]
            },
            plugin_aliases=plugin_aliases
        )

class PluginProcessor:
    """Handles processing of individual plugin manifests."""
    
    def __init__(self, config: Config):
        self.config = config

    def extract_manifest_from_zip(self, zip_path: Path, plugin_name: str) -> Optional[Dict[str, Any]]:
        """Extract and parse manifest from a plugin ZIP file."""
        try:
            with ZipFile(zip_path) as z:
                manifest_data = z.read(f"{plugin_name}.json").decode("utf-8")
                manifest = json.loads(manifest_data)
                return manifest
        except Exception as e:
            print(f"Error reading manifest from {zip_path}: {e}")
            return None

    def process_plugin_directory(self, plugin_dir: Path) -> List[Dict[str, Any]]:
        """Process a single plugin directory and return list of manifests."""
        manifests = []
        plugin_name = plugin_dir.name
        
        main_zip = plugin_dir / "latest.zip"
        if not main_zip.exists():
            return manifests

        base_manifest = self.extract_manifest_from_zip(main_zip, plugin_name)
        if not base_manifest:
            return manifests

        testing_zip = plugin_dir / "testing" / "latest.zip"
        if testing_zip.exists():
            testing_manifest = self.extract_manifest_from_zip(testing_zip, plugin_name)
            if testing_manifest:
                base_manifest["TestingAssemblyVersion"] = testing_manifest.get("AssemblyVersion")
                base_manifest["TestingDalamudApiLevel"] = testing_manifest.get("DalamudApiLevel")

        manifests.append(base_manifest)

        global_zip = plugin_dir / "global" / "latest.zip"
        if global_zip.exists():
            global_manifest = self.extract_manifest_from_zip(global_zip, plugin_name)
            if global_manifest:
                global_manifest["Name"] = f"{global_manifest['Name']} (API{self.config.global_api_level})"
                manifests.append(global_manifest)

        return manifests

    def trim_manifest(self, manifest: Dict[str, Any]) -> Dict[str, Any]:
        """Keep only required keys from manifest."""
        return {k: manifest[k] for k in self.config.required_manifest_keys if k in manifest}

    def add_download_links(self, manifest: Dict[str, Any]) -> None:
        """Add download links and other computed fields to manifest."""
        is_global = manifest["Name"].endswith(f"(API{self.config.global_api_level})")
        plugin_name = manifest["InternalName"]
        is_from_repository = manifest.get("_repository_source", False)

        # Check if we have a stored repository asset URL (for private repos or non-standard assets)
        if "_repository_asset_url" in manifest:
            asset_api_url = manifest["_repository_asset_url"]
            repo_url = manifest.get("RepoUrl", "")
            
            # Extract asset ID from API URL and construct public download URL
            # API URL format: https://api.github.com/repos/{owner}/{repo}/releases/assets/{id}
            owner_repo = repo_url.replace("https://github.com/", "").rstrip("/")
            
            try:
                token_name = manifest.get("_repository_token_name")
                if token_name:
                    token = os.environ.get(token_name)
                    headers = {"Authorization": f"token {token}"}
                    response = requests.get(asset_api_url, headers=headers)
                    if response.status_code == 200:
                        asset_info = response.json()
                        asset_name = asset_info.get("name")
                        if asset_name:
                            manifest["DownloadLinkInstall"] = f"https://github.com/{owner_repo}/releases/latest/download/{asset_name}"
                            print(f"Using repository releases for {plugin_name}: {manifest['DownloadLinkInstall']}")
                        else:
                            print(f"WARNING: Could not determine asset name for {plugin_name}")
            except Exception as e:
                print(f"Error resolving asset URL for {plugin_name}: {e}")
        elif is_from_repository:
            repo_download_url = self._get_repo_download_url(manifest)
            if repo_download_url:
                manifest["DownloadLinkInstall"] = repo_download_url
                print(f"Using repository releases for {plugin_name}: {repo_download_url}")
            else:
                print(f"WARNING: Repository plugin {plugin_name} has no releases and no local files - skipping download links")
        elif not is_from_repository:
            url_key = "global" if is_global else "main"
            manifest["DownloadLinkInstall"] = self.config.download_urls[url_key].format(
                branch=self.config.branch, plugin_name=plugin_name
            )
            print(f"Using local files for {plugin_name}")

        if "TestingAssemblyVersion" in manifest and not is_global:
            if "_testing_download_url" in manifest:
                manifest["DownloadLinkTesting"] = manifest.pop("_testing_download_url")
            else:
                manifest["DownloadLinkTesting"] = self.config.download_urls["testing"].format(
                    branch=self.config.branch, plugin_name=plugin_name
                )

        for src, targets in self.config.field_duplicates.items():
            for target in targets:
                if src in manifest and target not in manifest:
                    manifest[target] = manifest[src]

        if "IconUrl" not in manifest or not manifest["IconUrl"]:
            icon_path = Path("icons") / f"{plugin_name}.png"
            if icon_path.exists():
                manifest["IconUrl"] = f"https://raw.githubusercontent.com/{self.config.repo}/main/icons/{plugin_name}.png"
                print(f"Added icon URL for {plugin_name}")

        manifest["DownloadCount"] = 0

    def _get_repo_download_url(self, manifest: Dict[str, Any]) -> Optional[str]:
        """Get download URL from repository releases if available."""
        try:
            repo_url = manifest.get("RepoUrl", "")
            if not repo_url or "github.com" not in repo_url:
                return None

            repo_path = repo_url.replace("https://github.com/", "").rstrip("/")
            if "/" not in repo_path:
                return None

            owner, repo = repo_path.split("/", 1)

            api_url = f"https://api.github.com/repos/{owner}/{repo}/releases/latest"
            response = requests.get(api_url)

            if response.status_code == 200:
                release_data = response.json()

                plugin_name = manifest["InternalName"]
                assets = release_data.get("assets", [])

                preferred_asset_name = None

                for asset in assets:
                    asset_name = asset.get("name", "")
                    if asset_name == "latest.zip":
                        preferred_asset_name = asset_name
                        break

                if not preferred_asset_name:
                    for asset in assets:
                        asset_name = asset.get("name", "")
                        if (asset_name.endswith(".zip") and 
                            asset_name.startswith(f"{plugin_name}-") and 
                            not asset_name.endswith("-latest.zip")):
                            preferred_asset_name = asset_name
                            break

                if not preferred_asset_name:
                    for asset in assets:
                        asset_name = asset.get("name", "")
                        if asset_name == f"{plugin_name}.zip":
                            preferred_asset_name = asset_name
                            break

                if not preferred_asset_name:
                    for asset in assets:
                        asset_name = asset.get("name", "")
                        if asset_name.endswith(".zip"):
                            preferred_asset_name = asset_name
                            break

                if preferred_asset_name:
                    stable_url = f"https://github.com/{owner}/{repo}/releases/latest/download/{preferred_asset_name}"
                    return stable_url

            return None

        except Exception as e:
            print(f"Error checking repository releases for {manifest.get('InternalName', 'unknown')}: {e}")
            return None


class RepositoryPluginProcessor:
    """Handles processing plugins directly from GitHub repositories."""
    
    def __init__(self, config: Config):
        self.config = config
        # Collect all token names referenced in plugin-sources.json and resolve from env
        token_names = {rc["token"] for rc in config.repository_list.values()}
        token_names.add("GITHUB_TOKEN")  # always available
        self.tokens = {name: os.environ.get(name) for name in token_names}

    def get_repository_plugins(self) -> List[Dict[str, Any]]:
        """Get plugin manifests from configured repositories."""
        manifests = []

        for plugin_name, repo_config in self.config.repository_list.items():
            repo_url = repo_config["url"]
            token_name = repo_config["token"]
            print(f"Processing repository plugin: {plugin_name} from {repo_url} (using {token_name})")

            repo_manifest = self._get_manifest_from_repository(plugin_name, repo_url, token_name)
            if repo_manifest:
                # Tag output routing now while we have the correct plugin-sources.json key
                repo_manifest["_output_name"] = self.config.plugin_outputs.get(plugin_name, "default")
                manifests.append(repo_manifest)

        return manifests

    def _get_manifest_from_repository(self, plugin_name: str, repo_url: str, token_name: str) -> Optional[Dict[str, Any]]:
        """Extract manifest from a GitHub repository's latest release."""
        try:
            repo_path = repo_url.replace("https://github.com/", "").rstrip("/")
            if "/" not in repo_path:
                print(f"Invalid repository URL format for {plugin_name}: {repo_url}")
                return None

            owner, repo = repo_path.split("/", 1)

            api_url = f"https://api.github.com/repos/{owner}/{repo}/releases/latest"
            
            # Get the specified token
            token = self.tokens.get(token_name)
            headers = {"Authorization": f"token {token}"} if token else {}
            
            response = requests.get(api_url, headers=headers)

            if response.status_code == 404:
                print(f"Repository {owner}/{repo} not found or private - skipping")
                return None
            elif response.status_code == 403:
                print(f"Access forbidden for {owner}/{repo} (rate limited or private) - skipping")
                return None
            elif response.status_code != 200:
                print(f"Error accessing repository {owner}/{repo}: HTTP {response.status_code}")
                return None

            release_data = response.json()

            release_date = release_data.get("published_at")
            if release_date:
                from datetime import datetime
                try:
                    dt = datetime.fromisoformat(release_date.replace('Z', '+00:00'))
                    release_timestamp = str(int(dt.timestamp()))
                except Exception:
                    release_timestamp = None
            else:
                release_timestamp = None

            plugin_zip_url = self._find_plugin_asset(release_data, plugin_name)
            if not plugin_zip_url:
                print(f"No suitable ZIP asset found for {plugin_name} in {owner}/{repo}")
                return None

            manifest = self._extract_manifest_from_url(plugin_zip_url, plugin_name, token)
            if manifest:
                manifest["RepoUrl"] = repo_url
                manifest["_repository_source"] = True
                manifest["_repository_asset_url"] = plugin_zip_url
                manifest["_repository_token_name"] = token_name
                if release_timestamp:
                    manifest["LastUpdate"] = release_timestamp

                # Check for testing pre-release
                testing_info = self._get_testing_release_info(owner, repo, plugin_name, token)
                if testing_info:
                    manifest["TestingAssemblyVersion"] = testing_info["version"]
                    manifest["_testing_download_url"] = testing_info["download_url"]
                    print(f"Found testing release for {plugin_name}: v{testing_info['version']}")

                print(f"Successfully extracted manifest for {plugin_name} v{manifest.get('AssemblyVersion', 'unknown')}")
                return manifest

        except Exception as e:
            print(f"Error processing repository plugin {plugin_name}: {e}")
            return None

        return None

    def _find_plugin_asset(self, release_data: Dict[str, Any], plugin_name: str) -> Optional[str]:
        """Find the best plugin ZIP asset from release assets."""
        assets = release_data.get("assets", [])
        
        print(f"Available assets for {plugin_name}: {[asset.get('name') for asset in assets]}")

        for asset in assets:
            if asset.get("name") == "latest.zip":
                return asset.get("url")

        for asset in assets:
            if asset.get("name") == f"{plugin_name}.zip":
                return asset.get("url")
        
        plugin_name_no_spaces = plugin_name.replace(" ", "")
        for asset in assets:
            if asset.get("name") == f"{plugin_name_no_spaces}.zip":
                return asset.get("url")

        plugin_name_dashes = plugin_name.replace(" ", "-")
        for asset in assets:
            if asset.get("name") == f"{plugin_name_dashes}.zip":
                return asset.get("url")

        for asset in assets:
            asset_name = asset.get("name", "")
            if asset_name.endswith(".zip") and asset_name.startswith(f"{plugin_name}-"):
                return asset.get("url")

        for asset in assets:
            if asset.get("name", "").endswith(".zip"):
                return asset.get("url")

        return None

    def _get_testing_release_info(self, owner: str, repo: str, plugin_name: str, token: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """Fetch testing pre-release info (version and download URL) from a repository."""
        try:
            api_url = f"https://api.github.com/repos/{owner}/{repo}/releases"
            headers = {"Authorization": f"token {token}"} if token else {}

            response = requests.get(api_url, headers=headers, params={"per_page": 30})
            if response.status_code != 200:
                return None

            releases = response.json()

            # Find the latest pre-release.
            # Supports two tag formats:
            #   - Legacy: testing-v1.2.3 (old workflow style)
            #   - Current: v1.2.3 with prerelease=true (new workflow style)
            for release in releases:
                if not release.get("prerelease"):
                    continue

                tag_name = release.get("tag_name", "")
                if tag_name.startswith("testing-v"):
                    testing_version = tag_name[len("testing-v"):]
                elif tag_name.startswith("v"):
                    testing_version = tag_name[len("v"):]
                else:
                    continue

                # Find a downloadable zip asset
                for asset in release.get("assets", []):
                    if asset.get("name") == "latest.zip":
                        download_url = f"https://github.com/{owner}/{repo}/releases/download/{tag_name}/latest.zip"
                        return {"version": testing_version, "download_url": download_url}

                for asset in release.get("assets", []):
                    if asset.get("name", "").endswith(".zip"):
                        download_url = f"https://github.com/{owner}/{repo}/releases/download/{tag_name}/{asset['name']}"
                        return {"version": testing_version, "download_url": download_url}

            return None

        except Exception as e:
            print(f"Error fetching testing release for {owner}/{repo}: {e}")
            return None

    def _extract_manifest_from_url(self, zip_url: str, plugin_name: str, token: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """Download ZIP file and extract plugin manifest."""
        try:
            headers = {}
            if token:
                headers["Authorization"] = f"token {token}"
                headers["Accept"] = "application/octet-stream"
            
            response = requests.get(zip_url, headers=headers, stream=True, allow_redirects=True)
            response.raise_for_status()

            temp_zip_path = Path(f"temp_{plugin_name}.zip")
            try:
                with open(temp_zip_path, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)

                actual_plugin_name = plugin_name
                for alias_name, alias_config in self.config.plugin_aliases.items():
                    if plugin_name == alias_name:
                        actual_plugin_name = alias_config["source"]
                        break

                with ZipFile(temp_zip_path) as z:
                    # Try to find the manifest file with various naming conventions
                    manifest_candidates = [
                        f"{actual_plugin_name}.json",
                        f"{actual_plugin_name.replace(' ', '')}.json",  # No spaces
                        f"{actual_plugin_name.replace(' ', '-')}.json",  # Dashes instead of spaces
                    ]
                    
                    # Also check all JSON files in the ZIP (excluding .deps.json files)
                    all_files = z.namelist()
                    json_files = [f for f in all_files if f.endswith('.json') and '/' not in f and not f.endswith('.deps.json')]
                    
                    manifest_file = None
                    for candidate in manifest_candidates:
                        if candidate in all_files:
                            manifest_file = candidate
                            break
                    
                    # If no match, try the first JSON file at root level (excluding .deps.json)
                    if not manifest_file and json_files:
                        manifest_file = json_files[0]
                        print(f"Using manifest file: {manifest_file}")
                    
                    if not manifest_file:
                        print(f"No manifest JSON found in {zip_url}. Available files: {all_files}")
                        return None
                    
                    manifest_data = z.read(manifest_file).decode("utf-8")
                    manifest = json.loads(manifest_data)
            
                    return manifest

            finally:
                if temp_zip_path.exists():
                    temp_zip_path.unlink()

        except Exception as e:
            print(f"Error extracting manifest from {zip_url}: {e}")
            return None


class ExternalPluginManager:
    """Handles downloading and caching of external plugins."""

    def __init__(self, config: Config):
        self.config = config

    def download_external_plugins(self) -> None:
        """Download all configured external plugins."""
        for plugin_name, urls in self.config.external_plugins.items():
            plugin_dir = self.config.plugins_dir / plugin_name
            plugin_dir.mkdir(parents=True, exist_ok=True)

            for variant, url in urls.items():
                if variant == "main":
                    dest_path = plugin_dir / "latest.zip"
                else:
                    variant_dir = plugin_dir / variant
                    variant_dir.mkdir(exist_ok=True)
                    dest_path = variant_dir / "latest.zip"

                self._download_if_needed(url, dest_path)

    def _download_if_needed(self, url: str, dest_path: Path) -> bool:
        """Download file only if it's newer than local copy."""
        try:
            if self._is_up_to_date(url, dest_path):
                print(f"Skipping {url} - already up to date")
                return True

            print(f"Downloading {url} to {dest_path}")
            response = requests.get(url, stream=True)
            response.raise_for_status()

            with open(dest_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)

            with ZipFile(dest_path) as z:
                pass

            self._save_metadata(response, dest_path)
            return True

        except Exception as e:
            print(f"Error downloading {url}: {e}")
            if dest_path.exists():
                dest_path.unlink()
            return False

    def _is_up_to_date(self, url: str, dest_path: Path) -> bool:
        """Check if local file is up to date based on HTTP headers."""
        if not dest_path.exists():
            return False

        try:
            head_response = requests.head(url)
            head_response.raise_for_status()

            metadata_file = dest_path.with_suffix('.meta')
            if not metadata_file.exists():
                return False

            with open(metadata_file, 'r') as f:
                metadata = json.load(f)

            etag = head_response.headers.get('ETag')
            last_modified = head_response.headers.get('Last-Modified')

            return ((etag and metadata.get('ETag') == etag) or
                    (last_modified and metadata.get('Last-Modified') == last_modified))

        except Exception:
            return False

    def _save_metadata(self, response: requests.Response, dest_path: Path) -> None:
        """Save HTTP metadata for future comparison."""
        metadata = {
            'ETag': response.headers.get('ETag'),
            'Last-Modified': response.headers.get('Last-Modified')
        }
        if any(metadata.values()):
            with open(dest_path.with_suffix('.meta'), 'w') as f:
                json.dump(metadata, f)


class DownloadCountUpdater:
    """Handles updating download counts from GitHub releases."""

    def __init__(self):
        self.github_token = os.environ.get("GITHUB_TOKEN")
        self.headers = {"Authorization": f"token {self.github_token}"} if self.github_token else {}
        self.repo_cache = {}

    def update_download_counts(self, manifests: List[Dict[str, Any]]) -> None:
        """Update download counts for all manifests."""
        for manifest in manifests:
            try:
                repo_url = manifest.get("RepoUrl", "")
                if not repo_url or "github.com" not in repo_url:
                    continue

                owner, repo = self._parse_github_url(repo_url)
                if not owner or not repo:
                    continue

                repo_key = f"{owner}/{repo}"

                if repo_key not in self.repo_cache:
                    self.repo_cache[repo_key] = self._fetch_download_count(owner, repo)

                manifest["DownloadCount"] = self.repo_cache[repo_key]
                print(f"Updated {manifest['InternalName']}: {manifest['DownloadCount']} downloads")

            except Exception as e:
                print(f"Error updating download count for {manifest.get('InternalName', 'unknown')}: {e}")

    def _parse_github_url(self, url: str) -> Tuple[Optional[str], Optional[str]]:
        """Parse GitHub URL to extract owner and repo."""
        try:
            repo_path = url.replace("https://github.com/", "").rstrip("/")
            if "/" in repo_path:
                return repo_path.split("/", 1)
        except Exception:
            pass
        return None, None

    def _fetch_download_count(self, owner: str, repo: str) -> int:
        """Fetch total download count for a GitHub repository (with pagination)."""
        try:
            api_url = f"https://api.github.com/repos/{owner}/{repo}/releases"
            print(f"Fetching download counts for {owner}/{repo}")

            total_downloads = 0
            page = 1

            while True:
                response = requests.get(
                    api_url,
                    headers=self.headers,
                    params={"per_page": 100, "page": page}
                )

                if response.status_code == 404:
                    print(f"Repository {owner}/{repo} not found or is private - skipping download count")
                    return 0
                elif response.status_code == 403:
                    print(f"Access forbidden for {owner}/{repo} (rate limited or private) - skipping download count")
                    return 0
                elif response.status_code == 401:
                    print(f"Authentication required for {owner}/{repo} - skipping download count")
                    return 0

                response.raise_for_status()
                releases = response.json()

                if not releases:
                    break

                for release in releases:
                    for asset in release.get("assets", []):
                        total_downloads += asset.get("download_count", 0)

                if len(releases) < 100:
                    break
                page += 1

            if total_downloads == 0:
                print(f"Repository {owner}/{repo} has no releases or no downloads")

            return total_downloads

        except requests.exceptions.RequestException as e:
            print(f"Network error fetching download count for {owner}/{repo}: {e}")
            return 0
        except Exception as e:
            print(f"Unexpected error fetching download count for {owner}/{repo}: {e}")
            return 0


class PluginMasterGenerator:
    """Main class that orchestrates the plugin master generation process."""

    def __init__(self, config: Config):
        self.config = config
        self.processor = PluginProcessor(config)
        self.repo_processor = RepositoryPluginProcessor(config)
        self.external_manager = ExternalPluginManager(config)
        self.download_updater = DownloadCountUpdater()
        self.existing_download_counts = {}

    def generate(self) -> None:
        """Generate the plugin master file."""
        print("Starting plugin master generation...")

        self._load_existing_download_counts()

        if self.config.external_plugins:
            print("Downloading external plugins...")
            self.external_manager.download_external_plugins()

        print("Collecting plugin manifests...")
        manifests = self._collect_manifests_with_priority()

        for manifest in manifests:
            self.processor.add_download_links(manifest)

        print("Updating download counts...")
        self.download_updater.update_download_counts(manifests)
    
        for manifest in manifests:
            plugin_name = manifest.get("InternalName")
            if manifest.get("DownloadCount", 0) == 0 and plugin_name in self.existing_download_counts:
                manifest["DownloadCount"] = self.existing_download_counts[plugin_name]
                print(f"Using cached download count for {plugin_name}: {manifest['DownloadCount']}")

        self._update_last_modified(manifests)

        # Preserve output routing through trim
        output_routing = {}
        for m in manifests:
            output_routing[m.get("InternalName")] = m.get("_output_name", "default")

        manifests = [self.processor.trim_manifest(m) for m in manifests]

        # Re-attach output routing after trim
        for m in manifests:
            m["_output_name"] = output_routing.get(m.get("InternalName"), "default")

        print("Writing plugin master file(s)...")
        self._write_plugin_master(manifests)

        print("Generating alias plugin master files...")
        self._generate_alias_files()

        print(f"Generated plugin master with {len(manifests)} plugins")

    def _generate_alias_files(self) -> None:
        """Generate separate pluginmaster files for aliases."""
        for alias_name, alias_config in self.config.plugin_aliases.items():
            print(f"\nGenerating alias file for {alias_name}...")
    
            source_plugin = alias_config["source"]
            source_repo = alias_config["source_repo"]
            output_file = Path(alias_config["output_file"])
            name_suffix = alias_config.get("name_suffix", " (Alternative)")
    
            repo_processor = RepositoryPluginProcessor(self.config)
            # For aliases, default to GITHUB_TOKEN
            manifest = repo_processor._get_manifest_from_repository(source_plugin, source_repo, "GITHUB_TOKEN")
    
            if not manifest:
                print(f"Could not fetch manifest for {source_plugin}, skipping alias {alias_name}")
                continue
    
            manifest["InternalName"] = alias_name
            manifest["Name"] = f"{manifest.get('Name', source_plugin)}{name_suffix}"
    
            manifest = self.processor.trim_manifest(manifest)
            self.processor.add_download_links(manifest)
    
            self.download_updater.update_download_counts([manifest])
    
            if manifest.get("DownloadCount", 0) == 0 and alias_name in self.existing_download_counts:
                manifest["DownloadCount"] = self.existing_download_counts[alias_name]
    
            import time
            manifest["LastUpdate"] = str(int(time.time()))
            print(f"Set current timestamp for {alias_name}: {manifest['LastUpdate']}")
    
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump([manifest], f, indent=4, ensure_ascii=False, sort_keys=True)
    
            print(f"Successfully generated {output_file} for {alias_name}")

    def _load_existing_download_counts(self) -> None:
        """Load download counts from existing output files if they exist."""
        for output_name, output_path in self.config.output_files.items():
            if output_path.exists():
                try:
                    with open(output_path, 'r', encoding='utf-8') as f:
                        existing_data = json.load(f)
                        for plugin in existing_data:
                            plugin_name = plugin.get("InternalName")
                            download_count = plugin.get("DownloadCount", 0)
                            if plugin_name:
                                self.existing_download_counts[plugin_name] = download_count
                    print(f"Loaded existing download counts from {output_path} ({output_name})")
                except Exception as e:
                    print(f"Could not load existing download counts from {output_path}: {e}")

    def _collect_manifests_with_priority(self) -> List[Dict[str, Any]]:
        """Collect plugin manifests with repository-first priority system."""
        manifests = []
        processed_plugins = set()

        print("Processing repository-configured plugins...")
        repo_manifests = self.repo_processor.get_repository_plugins()

        for manifest in repo_manifests:
            plugin_name = manifest.get("InternalName")
            if plugin_name:
                local_manifest = self._get_local_manifest(plugin_name)

                if local_manifest:
                    chosen_manifest = self._choose_better_manifest(repo_manifest=manifest, local_manifest=local_manifest, plugin_name=plugin_name)
                    manifests.append(chosen_manifest)
                else:
                    print(f"Using repository version for {plugin_name} (no local version found)")
                    manifests.append(manifest)

                processed_plugins.add(plugin_name)

        print("Processing remaining local plugins...")
        local_manifests = self._collect_local_manifests()

        for manifest in local_manifests:
            plugin_name = manifest.get("InternalName")
            if plugin_name and plugin_name not in processed_plugins:
                print(f"Using local version for {plugin_name} (not in repository list)")
                manifests.append(manifest)
                processed_plugins.add(plugin_name)

        # Attach output routing metadata (skip if already tagged by get_repository_plugins)
        for manifest in manifests:
            if "_output_name" not in manifest:
                plugin_name = manifest.get("Name", "")
                internal_name = manifest.get("InternalName", "")
                # Check by Name first (matches plugin-sources.json keys), then InternalName
                output_name = self.config.plugin_outputs.get(plugin_name) or self.config.plugin_outputs.get(internal_name, "default")
                manifest["_output_name"] = output_name

        return manifests

    def _get_local_manifest(self, plugin_name: str) -> Optional[Dict[str, Any]]:
        """Get manifest from local plugins directory."""
        plugin_dir = self.config.plugins_dir / plugin_name
        if plugin_dir.exists() and plugin_dir.is_dir():
            local_manifests = self.processor.process_plugin_directory(plugin_dir)
            return local_manifests[0] if local_manifests else None
        return None

    def _collect_local_manifests(self) -> List[Dict[str, Any]]:
        """Collect all plugin manifests from the local plugins directory."""
        manifests = []

        if not self.config.plugins_dir.exists():
            print(f"Plugins directory {self.config.plugins_dir} does not exist")
            return manifests

        for plugin_dir in self.config.plugins_dir.iterdir():
            if plugin_dir.is_dir():
                plugin_manifests = self.processor.process_plugin_directory(plugin_dir)
                manifests.extend(plugin_manifests)

        return manifests

    def _choose_better_manifest(self, repo_manifest: Dict[str, Any], local_manifest: Dict[str, Any], plugin_name: str) -> Dict[str, Any]:
        """Choose between repository and local manifest based on version comparison."""
        repo_version = repo_manifest.get("AssemblyVersion", "0.0")
        local_version = local_manifest.get("AssemblyVersion", "0.0")

        print(f"Comparing versions for {plugin_name}: repo={repo_version}, local={local_version}")

        if repo_version == local_version:
            print(f"Versions are equal for {plugin_name}, prioritising repository version")
            return repo_manifest

        try:
            repo_parts = [int(x) for x in repo_version.split('.')]
            local_parts = [int(x) for x in local_version.split('.')]

            max_len = max(len(repo_parts), len(local_parts))
            repo_parts.extend([0] * (max_len - len(repo_parts)))
            local_parts.extend([0] * (max_len - len(local_parts)))

            if repo_parts >= local_parts:
                print(f"Repository version is newer or equal for {plugin_name}, using repository")
                return repo_manifest
            else:
                print(f"Local version is newer for {plugin_name}, using local")
                return local_manifest

        except ValueError:
            print(f"Could not parse versions for {plugin_name}, prioritising repository")
            return repo_manifest

    def _write_plugin_master(self, manifests: List[Dict[str, Any]], final: bool = True) -> None:
        """Write plugin master JSON file(s), grouped by output routing."""
        # Group manifests by output name
        grouped: Dict[str, List[Dict[str, Any]]] = {name: [] for name in self.config.output_files}
        for m in manifests:
            output_name = m.get("_output_name", "default")
            if output_name not in grouped:
                output_name = "default"
            # Only pop _output_name on final write
            clean = {k: v for k, v in m.items() if k != "_output_name"} if final else dict(m)
            grouped.setdefault(output_name, []).append(clean)

        current_output_paths = set()
        for output_name, output_path in self.config.output_files.items():
            output_manifests = grouped.get(output_name, [])
            if not output_manifests and output_name != "default":
                if output_path.exists():
                    output_path.unlink()
                    print(f"Removed empty output file: {output_path} ({output_name})")
                continue
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(output_manifests, f, indent=4, ensure_ascii=False, sort_keys=True)
            current_output_paths.add(output_path.resolve())
            print(f"Wrote {len(output_manifests)} plugins to {output_path} ({output_name})")

        if final:
            self._cleanup_stale_outputs(current_output_paths)

    def _cleanup_stale_outputs(self, current_output_paths: set) -> None:
        """Remove output JSON files that are no longer in the config."""
        protected = {Path("./plugin-sources.json").resolve()}
        for json_file in Path(".").glob("*.json"):
            resolved = json_file.resolve()
            if resolved not in current_output_paths and resolved not in protected:
                json_file.unlink()
                print(f"Removed stale output file: {json_file}")

    def _update_last_modified(self, manifests: List[Dict[str, Any]]) -> None:
        """Update LastUpdate timestamps based on file modification times or repository release dates."""
        for manifest in manifests:
            try:
                if manifest.get("_repository_source"):
                    if "LastUpdate" in manifest:
                        print(f"Preserving GitHub release timestamp for {manifest['InternalName']}: {manifest['LastUpdate']}")
                        del manifest["_repository_source"]
                    else:
                        del manifest["_repository_source"]
                        self._set_local_timestamp(manifest, manifest["InternalName"])
                else:
                    self._set_local_timestamp(manifest, manifest["InternalName"])

            except Exception as e:
                print(f"Error updating last modified time for {manifest.get('InternalName', 'unknown')}: {e}")

        self._write_plugin_master(manifests, final=False)

    def _set_local_timestamp(self, manifest: Dict[str, Any], plugin_name: str) -> None:
        """Set timestamp from local file modification time."""
        is_global = manifest["Name"].endswith(f"(API{self.config.global_api_level})")
        
        if is_global:
            zip_path = self.config.plugins_dir / plugin_name / "global" / "latest.zip"
        else:
            zip_path = self.config.plugins_dir / plugin_name / "latest.zip"

        if zip_path.exists():
            modified_time = str(int(zip_path.stat().st_mtime))
            manifest["LastUpdate"] = modified_time
        else:
            import time
            manifest["LastUpdate"] = str(int(time.time()))

def main():
    """Main entry point."""
    config = Config.load_default()
    generator = PluginMasterGenerator(config)
    generator.generate()


if __name__ == "__main__":
    main()