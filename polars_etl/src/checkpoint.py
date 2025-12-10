import json
import os
from datetime import datetime
from pathlib import Path
import logging

logger = logging.getLogger("Checkpoint")

CHECKPOINT_DIR = "/opt/spark/checkpoints"

def ensure_checkpoint_dir():
    """Ensure checkpoint directory exists"""
    Path(CHECKPOINT_DIR).mkdir(parents=True, exist_ok=True)

def read_checkpoint(checkpoint_name: str) -> float:
    """
    Read last processed timestamp from checkpoint file
    
    Args:
        checkpoint_name: Name of checkpoint file (e.g., 'silver', 'gold')
    
    Returns:
        Last processed timestamp as float (Unix epoch), or 0.0 if no checkpoint exists
    """
    ensure_checkpoint_dir()
    checkpoint_path = Path(CHECKPOINT_DIR) / f"{checkpoint_name}_checkpoint.json"
    
    try:
        if checkpoint_path.exists():
            with open(checkpoint_path, 'r') as f:
                data = json.load(f)
                timestamp = data.get('last_processed_timestamp', 0.0)
                logger.info(f"Read checkpoint '{checkpoint_name}': last_processed_timestamp={timestamp}")
                return float(timestamp)
        else:
            logger.info(f"No checkpoint found for '{checkpoint_name}', starting from beginning")
            return 0.0
    except Exception as e:
        logger.error(f"Error reading checkpoint '{checkpoint_name}': {e}")
        return 0.0

def write_checkpoint(checkpoint_name: str, timestamp: float):
    """
    Write checkpoint timestamp to file
    
    Args:
        checkpoint_name: Name of checkpoint file (e.g., 'silver', 'gold')
        timestamp: Timestamp to save (Unix epoch float)
    """
    ensure_checkpoint_dir()
    checkpoint_path = Path(CHECKPOINT_DIR) / f"{checkpoint_name}_checkpoint.json"
    
    try:
        data = {
            'last_processed_timestamp': timestamp,
            'updated_at': datetime.now().isoformat(),
            'checkpoint_name': checkpoint_name
        }
        
        with open(checkpoint_path, 'w') as f:
            json.dump(data, f, indent=2)
        
        logger.info(f"Updated checkpoint '{checkpoint_name}': last_processed_timestamp={timestamp}")
    except Exception as e:
        logger.error(f"Error writing checkpoint '{checkpoint_name}': {e}")

def reset_checkpoint(checkpoint_name: str):
    """Reset checkpoint to start from beginning"""
    ensure_checkpoint_dir()
    checkpoint_path = Path(CHECKPOINT_DIR) / f"{checkpoint_name}_checkpoint.json"
    
    try:
        if checkpoint_path.exists():
            checkpoint_path.unlink()
            logger.info(f"Reset checkpoint '{checkpoint_name}'")
    except Exception as e:
        logger.error(f"Error resetting checkpoint '{checkpoint_name}': {e}")
