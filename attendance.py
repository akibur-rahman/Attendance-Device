from flask import Flask, request
import time

app = Flask(__name__)

# ------------------------------------------------
# 1) HANDSHAKE & DATA: /iclock/cdata
# ------------------------------------------------
@app.route("/iclock/cdata", methods=["GET", "POST"])
def cdata():
    args = request.args.to_dict()
    raw_body = request.get_data(as_text=True)

    print("\n==============================")
    print("📥 /iclock/cdata RECEIVED")
    print("==============================")
    print("GET:", args)
    print("RAW BODY:")
    print(repr(raw_body))
    print("------------------------------")

    # --- Handshake request: options=all ---
    if request.method == "GET" and args.get("options") == "all":
        sn = args.get("SN", "UNKNOWN")
        now = int(time.time())

        # Send basic config – this also controls how often it polls
        resp_lines = [
            f"GET OPTION FROM: {sn}",
            "Stamp=9999",
            f"OpStamp={now}",
            "ErrorDelay=30",
            "Delay=10",            # seconds between polls
            "TransTimes=00:00;23:59",
            "TransInterval=1",
            "TransFlag=1111000000",  # send AttLog, OpLog, User, etc.
            "Realtime=1",
            "Encrypt=None",
        ]
        resp = "\n".join(resp_lines)
        print("🔧 Sending handshake response:\n", resp)
        return resp

    # --- Data upload (POST) ---
    table = (args.get("table") or "").upper()

    # ATTLOG = attendance punches
    if request.method == "POST" and table == "ATTLOG":
        lines = [ln.strip() for ln in raw_body.splitlines() if ln.strip()]
        if not lines:
            print("⚠ ATTLOG POST but body is empty (no records in this batch).")
            return "OK: 0"

        print(f"🎉 ATTLOG batch with {len(lines)} record(s):")
        count = 0
        for line in lines:
            print("RAW LINE:", repr(line))
            parts = line.split("\t")
            if len(parts) >= 2:
                user_id = parts[0]
                timestamp = parts[1]
                print(f"👤 User: {user_id}   ⏰ Time: {timestamp}")
                count += 1
            else:
                print("⚠ Could not parse line (expected tab-separated fields).")

        # Tell device how many we processed (optional but recommended)
        return f"OK: {count}"

    # Other tables (OPERLOG, BIODATA, options, etc.)
    if request.method == "POST":
        print(f"ℹ Received table={table} data (not parsed in detail).")
        print("Body:\n", raw_body)
        return "OK"

    # Fallback
    return "OK"


# ------------------------------------------------
# 2) DEVICE HEARTBEAT / COMMAND POLLING: /iclock/getrequest
# ------------------------------------------------
last_print_time = 0

@app.route("/iclock/getrequest", methods=["GET"])
def getrequest():
    global last_print_time
    now = time.time()

    # Only print once every few seconds to avoid console spam
    if now - last_print_time > 5:
        print("\n📡 /iclock/getrequest FROM DEVICE:", request.args.to_dict())
        last_print_time = now

    # No commands for device
    return "OK"


# ------------------------------------------------
# 3) Root fallback
# ------------------------------------------------
@app.route("/", methods=["GET", "POST"])
def root():
    return "OK"


# ------------------------------------------------
# 4) Start server
# ------------------------------------------------
if __name__ == "__main__":
    PORT = 8081  # must match the port set in the device
    print(f"\n🚀 ZKTeco Push Server Running on 0.0.0.0:{PORT}")
    print("Waiting for attendance punches...\n")
    app.run(host="0.0.0.0", port=PORT)
