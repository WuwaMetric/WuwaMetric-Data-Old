import asyncio
import argparse
import json
import logging
import sys
import re
from pathlib import Path
from typing import List, Dict, Set, Any, Optional
from urllib.parse import urljoin

import httpx
from tqdm.asyncio import tqdm

BASE_API_URL = "https://api.hakush.in/ww/data/"
BASE_ASSET_URL = "https://api.hakush.in/ww/" 

DEFAULT_LANGS = ["zh", "en", "ja", "ko"]
TIMEOUT = 30
MAX_CONCURRENCY = 15

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - [%(levelname)s] - %(message)s",
    handlers=[logging.FileHandler("spider.log", encoding='utf-8')]
)
logger = logging.getLogger("WWSpider")

def parse_game_asset_path(game_path: str) -> Optional[str]:
    """
    Converts Unreal Engine internal paths to Web URLs.
    Input:  /Game/Aki/UI/UIResources/Common/Image/IconA/T_IconA_hsb_UI.T_IconA_hsb_UI
    Output: https://api.hakush.in/ww/UI/UIResources/Common/Image/IconA/T_IconA_hsb_UI.webp
    """
    if not isinstance(game_path, str) or not game_path:
        return None

    clean_path = game_path.replace("/Game/Aki/", "/")
    
    if "/" not in clean_path:
        return None

    clean_path = re.sub(r'\.[^/]+$', '', clean_path)

    if not clean_path.lower().endswith(('.png', '.jpg', '.webp')):
        clean_path += ".webp"

    return urljoin(BASE_ASSET_URL, clean_path.lstrip("/"))

class HakushinSpider:
    def __init__(self, output_dir: str, langs: List[str], force: bool = False):
        self.output_dir = Path(output_dir)
        self.langs = langs
        self.force = force
        self.client = httpx.AsyncClient(
            timeout=TIMEOUT,
            follow_redirects=True,
            limits=httpx.Limits(max_keepalive_connections=20, max_connections=30),
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Referer": "https://hakush.in/"
            }
        )
        self.semaphore = asyncio.Semaphore(MAX_CONCURRENCY)
        self.global_image_urls: Set[str] = set()

    async def close(self):
        await self.client.aclose()

    async def fetch_json(self, url: str, ignore_404: bool = False) -> Optional[Any]:
        for attempt in range(3):
            try:
                async with self.semaphore:
                    resp = await self.client.get(url)
                    if resp.status_code == 404:
                        if not ignore_404:
                            logger.warning(f"404 Not Found: {url}")
                        return None
                    resp.raise_for_status()
                    return resp.json()
            except Exception as e:
                if attempt == 2:
                    logger.error(f"Failed to fetch JSON {url}: {e}")
                    return None
                await asyncio.sleep(0.5)
        return None

    async def download_file(self, url: str, save_path: Path) -> bool:
        if save_path.exists() and not self.force:
            if save_path.stat().st_size > 0:
                return True

        save_path.parent.mkdir(parents=True, exist_ok=True)

        for attempt in range(3):
            try:
                async with self.semaphore:
                    async with self.client.stream("GET", url) as resp:
                        if resp.status_code != 200:
                            return False
                        temp_path = save_path.with_suffix(".tmp")
                        with open(temp_path, "wb") as f:
                            async for chunk in resp.aiter_bytes():
                                f.write(chunk)
                        if temp_path.exists():
                            temp_path.replace(save_path)
                        return True
            except Exception as e:
                if attempt == 2:
                    logger.error(f"Failed download asset {url}: {e}")
                    return False
                await asyncio.sleep(1)
        return False

    def extract_images_from_data(self, data: Any) -> None:
        """Recursively find image paths in JSON"""
        if isinstance(data, dict):
            for v in data.values():
                self.extract_images_from_data(v)
        elif isinstance(data, list):
            for item in data:
                self.extract_images_from_data(item)
        elif isinstance(data, str):
            if "/Game/Aki/" in data or ("/UI/" in data and "." in data):
                real_url = parse_game_asset_path(data)
                if real_url:
                    self.global_image_urls.add(real_url)

    async def process_category(self, category_name: str, index_file: str, detail_prefix: str, pbar_main: tqdm):
        """Process a specific category (e.g., character, weapon)"""
        
        index_url = urljoin(BASE_API_URL, f"en/{index_file}") 
        index_data = await self.fetch_json(index_url)
        
        if not index_data:
            index_url = urljoin(BASE_API_URL, index_file)
            index_data = await self.fetch_json(index_url)

        if not index_data:
            logger.error(f"Index not found: {category_name}")
            return

        self.extract_images_from_data(index_data)

        (self.output_dir / category_name).mkdir(parents=True, exist_ok=True)
        with open(self.output_dir / category_name / "index.json", "w", encoding="utf-8") as f:
            json.dump(index_data, f, ensure_ascii=False, indent=2)

        id_list = []
        if isinstance(index_data, dict):
            id_list = list(index_data.keys())
        elif isinstance(index_data, list):
            id_list = [str(x.get('id', x.get('Id', ''))) for x in index_data]

        id_list = [i for i in id_list if i]
        
        pbar_main.total += len(id_list) * len(self.langs)
        pbar_main.refresh()

        tasks = []
        for item_id in id_list:
            for lang in self.langs:
                tasks.append(self.process_single_item(category_name, detail_prefix, item_id, lang, pbar_main))

        await asyncio.gather(*tasks)

    async def process_single_item(self, category: str, prefix: str, item_id: str, lang: str, pbar: tqdm):
        try:
            url = f"{BASE_API_URL}{lang}/{prefix}/{item_id}.json"
            save_path = self.output_dir / category / lang / f"{item_id}.json"

            data = None
            if save_path.exists() and not self.force:
                try:
                    with open(save_path, "r", encoding="utf-8") as f:
                        data = json.load(f)
                except:
                    pass
            
            if not data:
                # Items often lack detail pages, ignore 404s
                ignore_404 = (category == "item")
                data = await self.fetch_json(url, ignore_404=ignore_404)
                
                if data:
                    save_path.parent.mkdir(parents=True, exist_ok=True)
                    with open(save_path, "w", encoding="utf-8") as f:
                        json.dump(data, f, ensure_ascii=False, indent=2)
            
            if data:
                self.extract_images_from_data(data)
                
        finally:
            pbar.update(1)

    async def download_all_images(self):
        if not self.global_image_urls:
            return

        logger.info(f"Downloading {len(self.global_image_urls)} unique images...")
        assets_dir = self.output_dir / "assets"
        assets_dir.mkdir(exist_ok=True)

        tasks = []
        pbar = tqdm(total=len(self.global_image_urls), desc="Downloading Assets", unit="img")

        for url in self.global_image_urls:
            filename = url.split("/")[-1]
            save_path = assets_dir / filename
            tasks.append(self._download_wrapper(url, save_path, pbar))
        
        await asyncio.gather(*tasks)
        pbar.close()

    async def _download_wrapper(self, url, path, pbar):
        await self.download_file(url, path)
        pbar.update(1)

async def main():
    parser = argparse.ArgumentParser(description="Hakushin Wuthering Waves Data Spider")
    parser.add_argument("--out", type=str, default="./ww_data", help="Output directory")
    parser.add_argument("--langs", type=str, default=",".join(DEFAULT_LANGS), help="Languages")
    parser.add_argument("--force", action="store_true", help="Force re-download")
    
    args = parser.parse_args()
    langs = [l.strip() for l in args.langs.split(",")]

    spider = HakushinSpider(args.out, langs, args.force)

    print(f"🚀 Starting Spider...")
    print(f"📂 Output: {args.out}")
    print(f"🌐 Languages: {langs}")
    
    categories = [
        ("character", "character.json", "character"),
        ("weapon", "weapon.json", "weapon"),
        ("echo", "echo.json", "echo"),
        ("item", "item.json", "item"), 
    ]

    try:
        pbar_json = tqdm(total=0, desc="Fetching JSON Data", unit="file")
        
        for cat_name, idx_file, prefix in categories:
            pbar_json.set_description(f"Processing {cat_name}")
            await spider.process_category(cat_name, idx_file, prefix, pbar_json)
        
        pbar_json.close()
        
        print(f"\n🖼️  Analyzing collected assets...")
        await spider.download_all_images()

        print(f"\n✅ Done!")

    finally:
        await spider.close()

if __name__ == "__main__":
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())