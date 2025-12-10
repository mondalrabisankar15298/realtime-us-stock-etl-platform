import asyncio
import os
import sys
import logging
from src.bronze import run_bronze
from src.silver import run_silver
from src.gold import run_gold

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("PolarsETL")

async def main():
    logger.info("Starting Polars ETL Service")
    
    # Run all layers concurrently in the same event loop
    # In a real production scenario, these might be separate services or processes
    # But for "Application Lite", we run them as async tasks in one container
    
    try:
        await asyncio.gather(
            run_bronze(),
            run_silver(),
            run_gold()
        )
    except Exception as e:
        logger.error(f"Fatal error in ETL service: {e}")
        sys.exit(1)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Stopping ETL service...")
