import logging
import sqlite3
import time
from flask import Flask, request, g

# ------------------------------------------------
# LOGGING CONFIGURATION
# ------------------------------------------------
# Configure logging to display timestamps and log levels.
# In a real production environment, you might log to a file or an external aggregator.
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = Flask(__name__)
DB_FILE = 'attendance.db'

# ------------------------------------------------
# DATABASE HANDLING
# ------------------------------------------------
def get_db():
    """
    Opens a new database connection if there is none yet for the current application context.
    Using Flask's 'g' object to store the connection per request.
    """
    db = getattr(g, '_database', None)
    if db is None:
        db = g._database = sqlite3.connect(DB_FILE)
        # Return rows as dictionaries/accessible by name for better readability
        db.row_factory = sqlite3.Row
    return db

@app.teardown_appcontext
def close_connection(exception):
    """
    Closes the database connection at the end of the request.
    This is automatically called by Flask.
    """
    db = getattr(g, '_database', None)
    if db is not None:
        db.close()

def init_db():
    """
    Initializes the database with the required table(s).
    This should be run once when the application starts.
    """
    with app.app_context():
        db = get_db()
        cursor = db.cursor()
        # Create attendance_logs table if it doesn't exist
        # Columns:
        #   id: Primary key
        #   user_id: The ID of the user from the device
        #   punch_time: The timestamp of the punch
        #   device_serial: Serial number of the device
        #   is_synced: Sync status (default 0)
        #   received_at: Server timestamp when the record was received
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS attendance_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id TEXT NOT NULL,
                punch_time TEXT NOT NULL,
                device_serial TEXT,
                is_synced INTEGER DEFAULT 0,
                received_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        db.commit()
        logger.info(f"Database initialized and connected to {DB_FILE}")

# Initialize the DB structure immediately
init_db()

# ------------------------------------------------
# 1) HANDSHAKE & DATA ENDPOINT: /iclock/cdata
# ------------------------------------------------
@app.route("/iclock/cdata", methods=["GET", "POST"])
def cdata():
    """
    Handles both the initial handshake (GET) and data uploads (POST) from the device.
    """
    args = request.args.to_dict()
    raw_body = request.get_data(as_text=True)
    device_sn = args.get("SN", "UNKNOWN")

    # Logging received request details for debugging
    # logger.debug(f"/iclock/cdata request from {device_sn} | Method: {request.method}")

    # --- Handshake Request (GET) ---
    # The device sends a GET request with options='all' to sync usage parameters.
    if request.method == "GET" and args.get("options") == "all":
        logger.info(f"Handshake received from Device: {device_sn}")
        now = int(time.time())

        # Response parameters:
        # Stamp: Last synchronization stamp
        # OpStamp: Operation stamp
        # ErrorDelay: Retry delay in minutes if error
        # Delay: Polling interval in seconds
        # TransTimes: Transmission times
        # TransInterval: Transmission interval
        # TransFlag: Binary flags for data transmission
        # Realtime: Enable realtime transmission
        resp_lines = [
            f"GET OPTION FROM: {device_sn}",
            "Stamp=9999",
            f"OpStamp={now}",
            "ErrorDelay=30",
            "Delay=10",
            "TransTimes=00:00;23:59",
            "TransInterval=1",
            "TransFlag=1111000000",
            "Realtime=1",
            "Encrypt=None",
        ]
        return "\n".join(resp_lines)

    # --- Data Upload (POST) ---
    table = (args.get("table") or "").upper()
    
    # Case: Attendance Logs (ATTLOG)
    if request.method == "POST" and table == "ATTLOG":
        # Extract non-empty lines from the body
        lines = [ln.strip() for ln in raw_body.splitlines() if ln.strip()]
        
        if not lines:
            logger.warning("ATTLOG received but contains no records.")
            return "OK: 0"

        logger.info(f"Processing ATTLOG batch: {len(lines)} records from Device: {device_sn}")
        
        count = 0
        db = get_db()
        cursor = db.cursor()
        
        try:
            for line in lines:
                parts = line.split("\t")
                # Expected format: UserID \t Time \t ...
                if len(parts) >= 2:
                    user_id = parts[0]
                    punch_time = parts[1]
                    
                    # Insert record into SQLite database
                    cursor.execute('''
                        INSERT INTO attendance_logs (user_id, punch_time, device_serial, is_synced)
                        VALUES (?, ?, ?, ?)
                    ''', (user_id, punch_time, device_sn, 0))
                    
                    count += 1
                else:
                    logger.warning(f"Skipping malformed line: {line}")
            
            db.commit()
            logger.info(f"Successfully saved {count} attendance records.")
            
        except sqlite3.Error as e:
            db.rollback()
            logger.error(f"Database error while saving punches: {e}")
            # In case of DB error, we might want to return 'OK: 0' so device resends later? 
            # Or just 'OK: 0' to indicate 0 accepted.
            return "OK: 0"
        except Exception as e:
            db.rollback()
            logger.error(f"Unexpected error: {e}")
            return "OK: 0"

        # Return the count of successfully processed records to the device
        return f"OK: {count}"

    # Case: Other tables (OPERLOG, BIODATA, etc.)
    # We acknowledge them but don't process the data currently.
    if request.method == "POST":
        logger.info(f"Received data for table '{table}' - Acknowledging without processing.")
        return "OK"

    return "OK"


# ------------------------------------------------
# 2) DEVICE HEARTBEAT / COMMAND POLLING: /iclock/getrequest
# ------------------------------------------------
@app.route("/iclock/getrequest", methods=["GET"])
def getrequest():
    """
    Endpoint polled by the device to receive commands from the server.
    """
    # args = request.args.to_dict()
    # device_sn = args.get("SN", "UNKNOWN")
    
    # If we had commands (e.g. reboot, clear log, user updates), we would return them here.
    # returning "OK" means "No commands".
    
    # Logging is disabled here to prevent spamming logs every few seconds.
    # logger.debug(f"/iclock/getrequest from {device_sn}")
    
    return "OK"


# ------------------------------------------------
# 3) ROOT FALLBACK
# ------------------------------------------------
@app.route("/", methods=["GET", "POST"])
def root():
    """
    Fallback route for basic reachability check.
    """
    return "OK"


if __name__ == "__main__":
    PORT = 8081
    logger.info(f"🚀 ZKTeco Push Server running on 0.0.0.0:{PORT}")
    logger.info("Waiting for attendance punches...")
    app.run(host="0.0.0.0", port=PORT)
