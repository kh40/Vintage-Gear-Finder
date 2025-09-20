from fastapi import FastAPI, Request, Form, BackgroundTasks
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
import asyncio
import uvicorn
from datetime import datetime
import json
import os
from typing import List, Dict, Optional
from pydantic import BaseModel
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
import logging

from scraper import VintageGearScraper
from config import Config

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Vintage Gear Finder", description="Scrape vintage guitars and amplifiers from eBay and Reverb")

# Mount static files and templates
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

# Global configuration
config = Config()
scraper = VintageGearScraper(config)

# Scheduler for automated daily scraping
scheduler = BackgroundScheduler()

class SearchConfig(BaseModel):
    max_year: int = 1979
    max_price_percentage: float = 0.60
    min_condition: str = "Good"
    location: str = "US"
    search_terms: List[str] = ["vintage guitar", "vintage amplifier", "tube amp"]

# In-memory storage for last results (in production, use Redis or similar)
last_scrape_results = []
scrape_status = {"running": False, "last_run": None, "message": "Ready to scrape"}

@app.on_event("startup")
async def startup_event():
    """Initialize scheduler on startup"""
    # Schedule daily scraping at 6 AM
    scheduler.add_job(
        run_daily_scrape,
        CronTrigger(hour=6, minute=0),
        id="daily_scrape",
        name="Daily Vintage Gear Scrape"
    )
    scheduler.start()
    logger.info("Scheduler started - daily scraping at 6 AM")

@app.on_event("shutdown")
async def shutdown_event():
    """Clean shutdown"""
    scheduler.shutdown()

def run_daily_scrape():
    """Background job for daily scraping"""
    asyncio.create_task(perform_scrape_job())

async def perform_scrape_job():
    """Perform the actual scraping job"""
    global last_scrape_results, scrape_status
    
    try:
        scrape_status["running"] = True
        scrape_status["message"] = "Scraping in progress..."
        logger.info("Starting daily scrape job")
        
        # Run the scraper
        results = await scraper.scrape_all()
        
        # Update results
        last_scrape_results = results
        scrape_status["running"] = False
        scrape_status["last_run"] = datetime.now().isoformat()
        scrape_status["message"] = f"Completed - Found {len(results)} items"
        
        logger.info(f"Scrape completed - {len(results)} items found")
        
    except Exception as e:
        scrape_status["running"] = False
        scrape_status["message"] = f"Error: {str(e)}"
        logger.error(f"Scrape failed: {str(e)}")

@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    """Main dashboard page"""
    return templates.TemplateResponse("index.html", {
        "request": request,
        "config": config.get_current_config(),
        "results": last_scrape_results[:20],  # Show last 20 results
        "status": scrape_status
    })

@app.get("/config", response_class=HTMLResponse)
async def config_page(request: Request):
    """Configuration page"""
    return templates.TemplateResponse("config.html", {
        "request": request,
        "config": config.get_current_config()
    })

@app.post("/config")
async def update_config(
    max_year: int = Form(1979),
    max_price_percentage: float = Form(0.60),
    min_condition: str = Form("Good"),
    search_terms: str = Form("vintage guitar, vintage amplifier, tube amp"),
    ebay_api_key: str = Form(""),
    reverb_api_key: str = Form(""),
    google_sheets_id: str = Form(""),
    google_credentials_json: str = Form("")
):
    """Update configuration settings"""
    try:
        # Parse search terms
        terms = [term.strip() for term in search_terms.split(",")]
        
        # Update configuration
        new_config = {
            "max_year": max_year,
            "max_price_percentage": max_price_percentage,
            "min_condition": min_condition,
            "search_terms": terms,
            "ebay_api_key": ebay_api_key,
            "reverb_api_key": reverb_api_key,
            "google_sheets_id": google_sheets_id,
            "google_credentials_json": google_credentials_json
        }
        
        config.update_config(new_config)
        return JSONResponse({"status": "success", "message": "Configuration updated"})
        
    except Exception as e:
        return JSONResponse({"status": "error", "message": str(e)}, status_code=400)

@app.post("/scrape")
async def manual_scrape(background_tasks: BackgroundTasks):
    """Trigger manual scrape"""
    global scrape_status
    
    if scrape_status["running"]:
        return JSONResponse({"status": "error", "message": "Scrape already in progress"})
    
    background_tasks.add_task(perform_scrape_job)
    return JSONResponse({"status": "success", "message": "Scrape started"})

@app.get("/api/results")
async def get_results():
    """API endpoint to get latest results"""
    return JSONResponse({
        "results": last_scrape_results,
        "status": scrape_status,
        "total": len(last_scrape_results)
    })

@app.get("/api/status")
async def get_status():
    """Get current scraping status"""
    return JSONResponse(scrape_status)

@app.get("/results", response_class=HTMLResponse)
async def results_page(request: Request):
    """Full results page"""
    return templates.TemplateResponse("results.html", {
        "request": request,
        "results": last_scrape_results,
        "status": scrape_status
    })

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy", "timestamp": datetime.now().isoformat()}

if __name__ == "__main__":
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
