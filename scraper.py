import aiohttp
import asyncio
from bs4 import BeautifulSoup
import re
from typing import List, Dict, Optional
import logging
from datetime import datetime, timedelta
import json
import statistics

from google_sheets import GoogleSheetsClient

logger = logging.getLogger(__name__)

class VintageGearScraper:
    def __init__(self, config):
        self.config = config
        self.session = None
        self.google_client = GoogleSheetsClient(config)
        
        # Headers to appear more like a real browser
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        }

    async def __aenter__(self):
        self.session = aiohttp.ClientSession(headers=self.headers)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()

    async def scrape_all(self) -> List[Dict]:
        """Main scraping method that orchestrates all scraping"""
        async with self:
            all_results = []
            
            # Get search configuration
            config = self.config.get_current_config()
            search_terms = config.get("search_terms", ["vintage guitar", "vintage amplifier"])
            
            for term in search_terms:
                logger.info(f"Scraping for term: {term}")
                
                # Scrape eBay
                try:
                    ebay_results = await self.scrape_ebay(term)
                    all_results.extend(ebay_results)
                    logger.info(f"Found {len(ebay_results)} eBay results for '{term}'")
                except Exception as e:
                    logger.error(f"eBay scraping failed for '{term}': {str(e)}")
                
                # Scrape Reverb
                try:
                    reverb_results = await self.scrape_reverb(term)
                    all_results.extend(reverb_results)
                    logger.info(f"Found {len(reverb_results)} Reverb results for '{term}'")
                except Exception as e:
                    logger.error(f"Reverb scraping failed for '{term}': {str(e)}")
                
                # Rate limiting
                await asyncio.sleep(2)
            
            # Filter results
            filtered_results = self.filter_results(all_results)
            
            # Write to Google Sheets if configured
            if filtered_results and self.config.get_current_config().get("google_sheets_id"):
                try:
                    await self.google_client.write_results(filtered_results)
                    logger.info(f"Wrote {len(filtered_results)} results to Google Sheets")
                except Exception as e:
                    logger.error(f"Failed to write to Google Sheets: {str(e)}")
            
            return filtered_results

    async def scrape_ebay(self, search_term: str) -> List[Dict]:
        """Scrape eBay for vintage gear"""
        results = []
        
        # Use eBay API if available, otherwise scrape HTML
        ebay_api_key = self.config.get_current_config().get("ebay_api_key")
        
        if ebay_api_key:
            results = await self.scrape_ebay_api(search_term, ebay_api_key)
        else:
            results = await self.scrape_ebay_html(search_term)
        
        return results

    async def scrape_ebay_api(self, search_term: str, api_key: str) -> List[Dict]:
        """Scrape eBay using official API"""
        results = []
        
        # eBay Finding API endpoint
        url = "https://svcs.ebay.com/services/search/FindingService/v1"
        
        params = {
            "OPERATION-NAME": "findItemsAdvanced",
            "SERVICE-VERSION": "1.0.0",
            "SECURITY-APPNAME": api_key,
            "RESPONSE-DATA-FORMAT": "JSON",
            "keywords": search_term,
            "itemFilter(0).name": "Condition",
            "itemFilter(0).value": "Used",
            "itemFilter(1).name": "Country",
            "itemFilter(1).value": "US",
            "itemFilter(2).name": "ListingType",
            "itemFilter(2).value": "FixedPrice",
            "paginationInput.entriesPerPage": "100",
            "sortOrder": "EndTimeSoonest"
        }
        
        try:
            async with self.session.get(url, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    
                    if "findItemsAdvancedResponse" in data:
                        items = data["findItemsAdvancedResponse"][0].get("searchResult", [{}])[0].get("item", [])
                        
                        for item in items:
                            try:
                                result = {
                                    "marketplace": "eBay",
                                    "title": item.get("title", [""])[0],
                                    "price": float(item.get("sellingStatus", [{}])[0].get("currentPrice", [{"@currencyId": "USD", "__value__": "0"}])[0]["__value__"]),
                                    "currency": item.get("sellingStatus", [{}])[0].get("currentPrice", [{"@currencyId": "USD"}])[0]["@currencyId"],
                                    "condition": item.get("condition", [{"conditionDisplayName": "Unknown"}])[0].get("conditionDisplayName", "Unknown"),
                                    "url": item.get("viewItemURL", [""])[0],
                                    "image_url": item.get("galleryURL", [""])[0],
                                    "location": item.get("location", [""])[0],
                                    "year": self.extract_year_from_title(item.get("title", [""])[0]),
                                    "scraped_at": datetime.now().isoformat()
                                }
                                results.append(result)
                            except Exception as e:
                                logger.warning(f"Failed to parse eBay item: {str(e)}")
                                continue
                        
        except Exception as e:
            logger.error(f"eBay API request failed: {str(e)}")
            # Fallback to HTML scraping
            results = await self.scrape_ebay_html(search_term)
        
        return results

    async def scrape_ebay_html(self, search_term: str) -> List[Dict]:
        """Scrape eBay HTML as fallback"""
        results = []
        
        # eBay search URL
        search_url = f"https://www.ebay.com/sch/i.html?_nkw={search_term.replace(' ', '+')}&_in_kw=1&_ex_kw=&_sacat=0&LH_Sold=1&_udlo=&_udhi=&_samilow=&_samihi=&_sadis=15&_stpos=&_sargn=-1%26saslc%3D1&_salic=1&_sop=12&_dmd=1&_ipg=50"
        
        try:
            async with self.session.get(search_url) as response:
                if response.status == 200:
                    html = await response.text()
                    soup = BeautifulSoup(html, 'html.parser')
                    
                    # Find listing items
                    items = soup.find_all('div', class_='s-item')
                    
                    for item in items[:50]:  # Limit to first 50 results
                        try:
                            title_elem = item.find('h3', class_='s-item__title')
                            price_elem = item.find('span', class_='s-item__price')
                            link_elem = item.find('a', class_='s-item__link')
                            condition_elem = item.find('span', class_='SECONDARY_INFO')
                            
                            if title_elem and price_elem and link_elem:
                                title = title_elem.get_text(strip=True)
                                price_text = price_elem.get_text(strip=True)
                                url = link_elem.get('href', '')
                                condition = condition_elem.get_text(strip=True) if condition_elem else "Unknown"
                                
                                # Extract price
                                price_match = re.search(r'\$?(\d+(?:,\d{3})*(?:\.\d{2})?)', price_text.replace(',', ''))
                                price = float(price_match.group(1)) if price_match else 0
                                
                                result = {
                                    "marketplace": "eBay",
                                    "title": title,
                                    "price": price,
                                    "currency": "USD",
                                    "condition": condition,
                                    "url": url,
                                    "image_url": "",
                                    "location": "US",
                                    "year": self.extract_year_from_title(title),
                                    "scraped_at": datetime.now().isoformat()
                                }
                                results.append(result)
                        
                        except Exception as e:
                            logger.warning(f"Failed to parse eBay HTML item: {str(e)}")
                            continue
                            
        except Exception as e:
            logger.error(f"eBay HTML scraping failed: {str(e)}")
        
        return results

    async def scrape_reverb(self, search_term: str) -> List[Dict]:
        """Scrape Reverb for vintage gear"""
        results = []
        
        # Use Reverb API if available, otherwise scrape HTML
        reverb_api_key = self.config.get_current_config().get("reverb_api_key")
        
        if reverb_api_key:
            results = await self.scrape_reverb_api(search_term, reverb_api_key)
        else:
            results = await self.scrape_reverb_html(search_term)
        
        return results

    async def scrape_reverb_api(self, search_term: str, api_token: str) -> List[Dict]:
        """Scrape Reverb using official API"""
        results = []
        
        headers = {
            **self.headers,
            'Authorization': f'Bearer {api_token}',
            'Accept': 'application/hal+json'
        }
        
        url = "https://reverb.com/api/listings"
        params = {
            "query": search_term,
            "condition": "used,b_stock,fair,good,very_good,excellent,mint",
            "shipping_region": "US",
            "per_page": 50,
            "page": 1
        }
        
        try:
            async with self.session.get(url, headers=headers, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    
                    for item in data.get("listings", []):
                        try:
                            result = {
                                "marketplace": "Reverb",
                                "title": item.get("title", ""),
                                "price": float(item.get("price", {}).get("amount", 0)) / 100,  # Reverb prices are in cents
                                "currency": item.get("price", {}).get("currency", "USD"),
                                "condition": item.get("condition", {}).get("display_name", "Unknown"),
                                "url": f"https://reverb.com{item.get('_links', {}).get('web', {}).get('href', '')}",
                                "image_url": item.get("photos", [{}])[0].get("_links", {}).get("large", {}).get("href", ""),
                                "location": item.get("shipping", {}).get("origin_country_code", "US"),
                                "year": self.extract_year_from_title(item.get("title", "")),
                                "scraped_at": datetime.now().isoformat()
                            }
                            results.append(result)
                        except Exception as e:
                            logger.warning(f"Failed to parse Reverb API item: {str(e)}")
                            continue
                            
        except Exception as e:
            logger.error(f"Reverb API request failed: {str(e)}")
            # Fallback to HTML scraping
            results = await self.scrape_reverb_html(search_term)
        
        return results

    async def scrape_reverb_html(self, search_term: str) -> List[Dict]:
        """Scrape Reverb HTML as fallback"""
        results = []
        
        search_url = f"https://reverb.com/marketplace?query={search_term.replace(' ', '+')}&condition=used&condition=b_stock&condition=fair&condition=good&condition=very_good&condition=excellent&condition=mint"
        
        try:
            async with self.session.get(search_url) as response:
                if response.status == 200:
                    html = await response.text()
                    soup = BeautifulSoup(html, 'html.parser')
                    
                    # Find listing items (Reverb structure may vary)
                    items = soup.find_all('div', class_='tiles-item')
                    
                    for item in items[:50]:  # Limit to first 50 results
                        try:
                            title_elem = item.find('a', class_='listing-item__title')
                            price_elem = item.find('span', class_='listing-item__price')
                            condition_elem = item.find('span', class_='listing-item__condition')
                            
                            if title_elem and price_elem:
                                title = title_elem.get_text(strip=True)
                                price_text = price_elem.get_text(strip=True)
                                url = "https://reverb.com" + title_elem.get('href', '')
                                condition = condition_elem.get_text(strip=True) if condition_elem else "Unknown"
                                
                                # Extract price
                                price_match = re.search(r'\$?(\d+(?:,\d{3})*(?:\.\d{2})?)', price_text.replace(',', ''))
                                price = float(price_match.group(1)) if price_match else 0
                                
                                result = {
                                    "marketplace": "Reverb",
                                    "title": title,
                                    "price": price,
                                    "currency": "USD",
                                    "condition": condition,
                                    "url": url,
                                    "image_url": "",
                                    "location": "US",
                                    "year": self.extract_year_from_title(title),
                                    "scraped_at": datetime.now().isoformat()
                                }
                                results.append(result)
                        
                        except Exception as e:
                            logger.warning(f"Failed to parse Reverb HTML item: {str(e)}")
                            continue
                            
        except Exception as e:
            logger.error(f"Reverb HTML scraping failed: {str(e)}")
        
        return results

    def extract_year_from_title(self, title: str) -> Optional[int]:
        """Extract year from listing title"""
        # Look for 4-digit years in the title
        year_match = re.search(r'\b(19[0-9]{2}|20[0-2][0-9])\b', title)
        if year_match:
            year = int(year_match.group(1))
            # Only return years that make sense for vintage gear
            if 1920 <= year <= 1979:
                return year
        return None

    def filter_results(self, results: List[Dict]) -> List[Dict]:
        """Filter results based on configuration criteria"""
        config = self.config.get_current_config()
        max_year = config.get("max_year", 1979)
        max_price_percentage = config.get("max_price_percentage", 0.60)
        min_condition = config.get("min_condition", "Good")
        
        # Define condition hierarchy
        condition_rank = {
            "Poor": 1, "Fair": 2, "Good": 3, "Very Good": 4, 
            "Excellent": 5, "Mint": 6, "New": 7, "Unknown": 0
        }
        min_condition_rank = condition_rank.get(min_condition, 3)
        
        filtered = []
        
        for item in results:
            # Year filter
            if item.get("year") and item["year"] > max_year:
                continue
            
            # Condition filter
            item_condition_rank = condition_rank.get(item.get("condition", "Unknown"), 0)
            if item_condition_rank < min_condition_rank:
                continue
            
            # Price filter (simplified - in production, you'd get historical data)
            # For now, we'll assume items under $500 are potentially good deals
            if item.get("price", 0) > 500:
                continue
            
            # Location filter (only US)
            location = item.get("location", "").upper()
            if location and "US" not in location and "UNITED STATES" not in location:
                continue
            
            filtered.append(item)
        
        return filtered
