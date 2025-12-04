import os
import logging
from datetime import datetime, timedelta
from flask import Flask, request, jsonify
from dotenv import load_dotenv
import mysql.connector

load_dotenv()
app = Flask(__name__)

logging.basicConfig(
    filename="app.log",
    level=logging.ERROR,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def get_connection():
    try:
        return mysql.connector.connect(
            host=os.getenv("DB_HOST"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASS"),
            database=os.getenv("DB_NAME")
        )
    except Exception as e:
        logging.error(f"DB Connection Error: {str(e)}")
        raise

def get_value(sql, params=None):
    conn = cursor = None
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(sql, params or ())
        row = cursor.fetchone()
        return row[0] if row and row[0] is not None else 0
    except Exception as e:
        logging.error(f"Query Error: {str(e)} | SQL: {sql}")
        raise
    finally:
        if cursor: cursor.close()
        if conn: conn.close()

def get_date_range(filter_type, start_date, end_date):
    today = datetime.now().date()

    if filter_type == "today":
        return today, today
    if filter_type == "yesterday":
        y = today - timedelta(days=1)
        return y, y
    if filter_type == "last_week":
        return today - timedelta(days=7), today
    if filter_type == "last_month":
        return today - timedelta(days=30), today

    if start_date and end_date:
        s = datetime.strptime(start_date, "%Y-%m-%d").date()
        e = datetime.strptime(end_date, "%Y-%m-%d").date()
        return s, e

    if start_date and not end_date:
        raise ValueError("End date (to) is required when 'from' is provided.")
    if end_date and not start_date:
        raise ValueError("Start date (from) is required when 'to' is provided.")

    return today, today

@app.route("/kpis", methods=["GET"])
def kpis():
    try:
        filter_type = request.args.get("filter")
        start_date = request.args.get("from")
        end_date = request.args.get("to")

        try:
            start, end = get_date_range(filter_type, start_date, end_date)
        except ValueError as e:
            return jsonify({"status": "error", "message": str(e)}), 400

        if start > end:
            return jsonify({
                "status": "error",
                "message": "Start date cannot be greater than end date."
            }), 400

        files = get_value("""
            SELECT SUM(is_doc_processed='y')
            FROM file_metadata
            WHERE DATE(time_stamp) BETWEEN %s AND %s;
        """, (start, end))

        pages = get_value("""
            SELECT SUM(texteract_pages)
            FROM file_metadata
            WHERE DATE(time_stamp) BETWEEN %s AND %s
              AND texteract_pages IS NOT NULL;
        """, (start, end))

        avg_seconds = get_value("""
            SELECT AVG(TIMESTAMPDIFF(SECOND, process_start_time, process_end_time))
            FROM file_metadata
            WHERE DATE(process_end_time) BETWEEN %s AND %s;
        """, (start, end))

        if (files == 0) and (pages == 0) and (avg_seconds == 0):
            return jsonify({
                "status": "error",
                "message": "No records found for the selected date range.",
                "data": {}
            }), 404

        if avg_seconds is None or avg_seconds == 0:
            avg_text = "0 seconds"
        else:
            avg_seconds = int(avg_seconds)
            if avg_seconds < 60:
                avg_text = f"{avg_seconds} seconds"
            else:
                avg_text = f"{avg_seconds // 60} mins"

        return jsonify({
            "status": "success",
            "message": "KPIs fetched successfully",
            "data": {
                "files_processed": int(files or 0),
                "texteract_pages": int(pages or 0),
                "avg_processing_time": avg_text,
                "date_range": f"{start} to {end}"
            }
        }), 200

    except Exception as e:
        logging.error(f"Unhandled Error: {str(e)}")
        return jsonify({
            "status": "error",
            "message": "Something went wrong. Contact support."
        }), 500

if __name__ == "__main__":
    app.run(debug=True)
