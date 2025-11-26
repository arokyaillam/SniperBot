from datetime import datetime
import pytz

def convert_unix_to_ist(unix_timestamp: int) -> str:
    """
    Convert Unix timestamp (in milliseconds or seconds) to IST timezone string.
    """
    # Check if timestamp is in milliseconds (13 digits) or seconds (10 digits)
    if len(str(unix_timestamp)) == 13:
        unix_timestamp = unix_timestamp / 1000
    
    utc_time = datetime.fromtimestamp(unix_timestamp, pytz.utc)
    ist_timezone = pytz.timezone('Asia/Kolkata')
    ist_time = utc_time.astimezone(ist_timezone)
    return ist_time.strftime('%Y-%m-%d %H:%M:%S %Z')
