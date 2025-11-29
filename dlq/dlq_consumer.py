#!/usr/bin/env python3
"""
DLQ Consumer - Dead Letter Queue Message Handler
Monitors DLQ topic for failed messages and sends notifications
Logs failures for analysis and manual intervention
"""

import json
import logging
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict

from kafka import KafkaConsumer

# Configuration
REDPANDA_BROKER = os.getenv("REDPANDA_BROKER", "redpanda:9092")
KAFKA_TOPIC_DLQ = os.getenv("KAFKA_TOPIC_DLQ", "stock-dlq")
DLQ_LOG_FILE = Path("/app/logs/dlq_failures.log")

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(DLQ_LOG_FILE.parent / "dlq_consumer.log")
    ]
)
logger = logging.getLogger(__name__)


class DLQConsumer:
    """
    Consumes messages from DLQ topic and handles failures
    """
    
    def __init__(self):
        self.consumer = self._create_consumer()
        self.failure_log = DLQ_LOG_FILE
        self._ensure_log_dir()
    
    def _ensure_log_dir(self):
        """Create log directory if it doesn't exist"""
        self.failure_log.parent.mkdir(parents=True, exist_ok=True)
    
    def _create_consumer(self) -> KafkaConsumer:
        """Create Kafka consumer for DLQ topic"""
        max_retries = 10
        
        for attempt in range(max_retries):
            try:
                consumer = KafkaConsumer(
                    KAFKA_TOPIC_DLQ,
                    bootstrap_servers=REDPANDA_BROKER,
                    auto_offset_reset='earliest',
                    enable_auto_commit=True,
                    group_id='dlq-consumer-group',
                    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                )
                logger.info(f"DLQ consumer connected to {REDPANDA_BROKER}")
                return consumer
            except Exception as e:
                logger.warning(f"Connection attempt {attempt + 1}/{max_retries} failed: {e}")
                if attempt < max_retries - 1:
                    import time
                    time.sleep(5)
                else:
                    logger.error("Failed to connect to Kafka after max retries")
                    raise
    
    def log_failure(self, message: Dict):
        """
        Log failure details to file for analysis
        """
        try:
            with open(self.failure_log, 'a') as f:
                log_entry = {
                    'logged_at': datetime.now().isoformat(),
                    'ticker': message.get('ticker', 'UNKNOWN'),
                    'error': message.get('error', 'No error message'),
                    'timestamp': message.get('timestamp', ''),
                    'original_data': message.get('original_data', None),
                }
                f.write(json.dumps(log_entry) + '\n')
            
            logger.info(f"Logged failure: {message.get('ticker')} - {message.get('error')}")
            
        except Exception as e:
            logger.error(f"Error logging failure: {e}")
    
    def send_notification(self, message: Dict):
        """
        Send notification about DLQ message
        In production, this would integrate with:
        - Slack webhook
        - Email (SMTP)
        - PagerDuty
        - SMS alerts
        """
        ticker = message.get('ticker', 'UNKNOWN')
        error = message.get('error', 'Unknown error')
        timestamp = message.get('timestamp', datetime.now().isoformat())
        
        # Slack notification (placeholder)
        notification_message = f"""
🚨 **DLQ Alert** 🚨

**Ticker**: {ticker}
**Error**: {error}
**Timestamp**: {timestamp}
**Action Required**: Manual investigation needed

Check DLQ logs for details: {self.failure_log}
        """.strip()
        
        logger.warning(f"NOTIFICATION: {notification_message}")
        
        # In production, send to Slack:
        # requests.post(SLACK_WEBHOOK_URL, json={'text': notification_message})
        
        # In production, send email:
        # send_email(to='ops@company.com', subject='DLQ Alert', body=notification_message)
    
    def analyze_failure(self, message: Dict) -> str:
        """
        Analyze failure and suggest remediation
        """
        error = message.get('error', '').lower()
        
        if 'kafka' in error or 'connection' in error:
            return "Network/Kafka issue - check broker health"
        elif 'timeout' in error:
            return "Timeout issue - check API rate limits or network latency"
        elif 'invalid' in error or 'validation' in error:
            return "Data validation issue - check data source quality"
        elif 'market' in error or 'closed' in error:
            return "Market hours issue - expected behavior"
        else:
            return "Unknown error - manual investigation required"
    
    def process_dlq_message(self, message: Dict):
        """
        Process a single DLQ message
        """
        ticker = message.get('ticker', 'UNKNOWN')
        error = message.get('error', 'Unknown error')
        
        logger.error(f"DLQ Message - Ticker: {ticker}, Error: {error}")
        
        # Log the failure
        self.log_failure(message)
        
        # Analyze failure
        analysis = self.analyze_failure(message)
        logger.info(f"Analysis: {analysis}")
        
        # Send notification (only for critical errors)
        if 'manual' in analysis.lower() or 'kafka' in error.lower():
            self.send_notification(message)
    
    def run(self):
        """
        Main consumer loop
        """
        logger.info("=" * 80)
        logger.info("DLQ CONSUMER - Starting")
        logger.info("=" * 80)
        logger.info(f"Monitoring topic: {KAFKA_TOPIC_DLQ}")
        logger.info(f"Failure log: {self.failure_log}")
        logger.info("=" * 80)
        
        try:
            message_count = 0
            
            for kafka_message in self.consumer:
                try:
                    dlq_message = kafka_message.value
                    message_count += 1
                    
                    logger.info(f"\n--- DLQ Message #{message_count} ---")
                    self.process_dlq_message(dlq_message)
                    logger.info(f"--- End Message #{message_count} ---\n")
                    
                except Exception as e:
                    logger.error(f"Error processing DLQ message: {e}", exc_info=True)
        
        except KeyboardInterrupt:
            logger.info("\nDLQ consumer stopped by user")
        
        except Exception as e:
            logger.error(f"Unexpected error in DLQ consumer: {e}", exc_info=True)
        
        finally:
            self.consumer.close()
            logger.info("DLQ consumer shut down gracefully")


if __name__ == "__main__":
    try:
        consumer = DLQConsumer()
        consumer.run()
    except Exception as e:
        logger.critical(f"DLQ consumer failed to start: {e}", exc_info=True)
        sys.exit(1)

