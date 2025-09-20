import json
import asyncio
from typing import List, Dict
import logging
from datetime import datetime
import gspread
from google.oauth2.service_account import Credentials

logger = logging.getLogger(__name__)

class GoogleSheetsClient:
    """Client for writing scraper results to Google Sheets"""
    
    def __init__(self, config):
        self.config = config
        self.client = None
        self.worksheet = None
        
        # Define the scope
        self.scope = [
            'https://spreadsheets.google.com/feeds',
            'https://www.googleapis.com/auth/drive'
        ]
        
        self.headers = [
            'Date',
            'Marketplace',
            'Title',
            'Price',
            'Currency',
            'Year',
            'Condition',
            'Location',
            'URL',
            'Image URL'
        ]
    
    def _initialize_client(self):
        """Initialize Google Sheets client"""
        try:
            config = self.config.get_current_config()
            credentials_json = config.get("google_credentials_json")
            sheets_id = config.get("google_sheets_id")
            
            if not credentials_json or not sheets_id:
                logger.warning("Google Sheets credentials or ID not configured")
                return False
            
            # Parse credentials JSON
            if isinstance(credentials_json, str):
                creds_dict = json.loads(credentials_json)
            else:
                creds_dict = credentials_json
            
            # Create credentials
            credentials = Credentials.from_service_account_info(
                creds_dict, scopes=self.scope
            )
            
            # Initialize client
            self.client = gspread.authorize(credentials)
            
            # Open spreadsheet
            spreadsheet = self.client.open_by_key(sheets_id)
            
            # Get or create worksheet
            try:
                self.worksheet = spreadsheet.worksheet("Vintage Gear Results")
            except gspread.WorksheetNotFound:
                self.worksheet = spreadsheet.add_worksheet(
                    title="Vintage Gear Results", 
                    rows=1000, 
                    cols=len(self.headers)
                )
                # Add headers
                self.worksheet.append_row(self.headers)
            
            logger.info("Google Sheets client initialized successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to initialize Google Sheets client: {str(e)}")
            return False
    
    async def write_results(self, results: List[Dict]):
        """Write scraper results to Google Sheets"""
        try:
            # Initialize client if not done already
            if not self.client and not self._initialize_client():
                return False
            
            if not results:
                logger.info("No results to write to Google Sheets")
                return True
            
            # Prepare rows for insertion
            rows = []
            current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            for result in results:
                row = [
                    current_time,
                    result.get('marketplace', ''),
                    result.get('title', ''),
                    result.get('price', 0),
                    result.get('currency', 'USD'),
                    result.get('year', ''),
                    result.get('condition', ''),
                    result.get('location', ''),
                    result.get('url', ''),
                    result.get('image_url', '')
                ]
                rows.append(row)
            
            # Write to sheet in batches to avoid API limits
            batch_size = 100
            for i in range(0, len(rows), batch_size):
                batch = rows[i:i + batch_size]
                self.worksheet.append_rows(batch)
                logger.info(f"Wrote batch of {len(batch)} rows to Google Sheets")
                
                # Rate limiting
                await asyncio.sleep(1)
            
            logger.info(f"Successfully wrote {len(results)} results to Google Sheets")
            return True
            
        except Exception as e:
            logger.error(f"Failed to write to Google Sheets: {str(e)}")
            return False
    
    def test_connection(self) -> Dict[str, any]:
        """Test Google Sheets connection"""
        try:
            if self._initialize_client():
                # Try to read the first row
                first_row = self.worksheet.row_values(1)
                return {
                    "success": True,
                    "message": f"Connection successful. Sheet has {len(first_row)} columns.",
                    "headers": first_row
                }
            else:
                return {
                    "success": False,
                    "message": "Failed to initialize Google Sheets client"
                }
        except Exception as e:
            return {
                "success": False,
                "message": f"Connection test failed: {str(e)}"
            }
    
    def clear_sheet(self) -> bool:
        """Clear all data from the sheet (except headers)"""
        try:
            if not self.client and not self._initialize_client():
                return False
            
            # Get all values
            all_values = self.worksheet.get_all_values()
            
            if len(all_values) > 1:  # More than just headers
                # Clear everything except the first row (headers)
                range_to_clear = f"A2:J{len(all_values)}"
                self.worksheet.batch_clear([range_to_clear])
                logger.info("Sheet cleared successfully")
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to clear sheet: {str(e)}")
            return False
    
    def get_recent_results(self, limit: int = 100) -> List[Dict]:
        """Get recent results from the sheet"""
        try:
            if not self.client and not self._initialize_client():
                return []
            
            # Get all records
            records = self.worksheet.get_all_records()
            
            # Sort by date (most recent first) and limit
            sorted_records = sorted(
                records, 
                key=lambda x: x.get('Date', ''), 
                reverse=True
            )
            
            return sorted_records[:limit]
            
        except Exception as e:
            logger.error(f"Failed to get recent results: {str(e)}")
            return []
