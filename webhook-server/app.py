import os
import json
import logging
from datetime import datetime, timezone
from flask import Flask, request, jsonify
import psycopg2
from psycopg2.extras import Json

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DATABASE_URL = os.environ.get("DATABASE_URL")


def get_db():
    return psycopg2.connect(DATABASE_URL)


def init_db():
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS survey_submissions (
                    id SERIAL PRIMARY KEY,
                    object_id INTEGER,
                    global_id TEXT,
                    event_type TEXT,
                    agent_name TEXT,
                    agent_id TEXT,
                    poi_name_ar TEXT,
                    poi_name_en TEXT,
                    category TEXT,
                    subcategory TEXT,
                    latitude DOUBLE PRECISION,
                    longitude DOUBLE PRECISION,
                    submitted_at TIMESTAMPTZ,
                    received_at TIMESTAMPTZ DEFAULT NOW(),
                    raw_payload JSONB,
                    attributes JSONB
                );

                CREATE INDEX IF NOT EXISTS idx_submissions_global_id
                    ON survey_submissions(global_id);
                CREATE INDEX IF NOT EXISTS idx_submissions_agent
                    ON survey_submissions(agent_name);
                CREATE INDEX IF NOT EXISTS idx_submissions_submitted
                    ON survey_submissions(submitted_at);
            """)
        conn.commit()
    logger.info("Database initialized")


@app.route("/", methods=["GET"])
def health():
    return jsonify({
        "status": "ok",
        "service": "Survey123 Webhook Server",
        "form": "POI Field Survey"
    })


@app.route("/webhook", methods=["POST"])
def webhook():
    try:
        payload = request.get_json(force=True)
        logger.info("Webhook received: %s", json.dumps(payload, default=str)[:500])

        event_type = payload.get("eventType", "unknown")

        # Extract feature data
        feature = payload.get("feature", {})
        attrs = feature.get("attributes", {})
        geometry = feature.get("geometry", {})

        # Extract server response
        server_resp = payload.get("serverResponse", {})
        object_id = server_resp.get("objectId")
        global_id = server_resp.get("globalId")

        # Extract key fields from attributes
        agent_name = attrs.get("agent_name", "")
        agent_id = attrs.get("agent_id", "")
        poi_name_ar = attrs.get("name_ar", "")
        poi_name_en = attrs.get("name_en", "")
        category = attrs.get("category", "")
        subcategory = attrs.get("secondary_category", "")
        latitude = geometry.get("y") or attrs.get("latitude")
        longitude = geometry.get("x") or attrs.get("longitude")

        # Parse submission time
        survey_datetime = attrs.get("survey_datetime")
        submitted_at = None
        if survey_datetime:
            try:
                if isinstance(survey_datetime, (int, float)):
                    submitted_at = datetime.fromtimestamp(
                        survey_datetime / 1000, tz=timezone.utc
                    )
                else:
                    submitted_at = datetime.fromisoformat(str(survey_datetime))
            except (ValueError, OSError):
                submitted_at = datetime.now(timezone.utc)
        else:
            submitted_at = datetime.now(timezone.utc)

        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO survey_submissions
                        (object_id, global_id, event_type, agent_name, agent_id,
                         poi_name_ar, poi_name_en, category, subcategory,
                         latitude, longitude, submitted_at,
                         raw_payload, attributes)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING id
                """, (
                    object_id, global_id, event_type,
                    agent_name, agent_id,
                    poi_name_ar, poi_name_en,
                    category, subcategory,
                    latitude, longitude, submitted_at,
                    Json(payload), Json(attrs)
                ))
                row_id = cur.fetchone()[0]
            conn.commit()

        logger.info("Saved submission #%d (agent=%s, poi=%s)",
                     row_id, agent_name, poi_name_ar)

        return jsonify({"status": "success", "id": row_id}), 200

    except Exception as e:
        logger.error("Webhook error: %s", str(e), exc_info=True)
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/submissions", methods=["GET"])
def list_submissions():
    try:
        limit = request.args.get("limit", 50, type=int)
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT id, object_id, global_id, event_type,
                           agent_name, poi_name_ar, poi_name_en,
                           category, subcategory,
                           latitude, longitude,
                           submitted_at, received_at
                    FROM survey_submissions
                    ORDER BY received_at DESC
                    LIMIT %s
                """, (limit,))
                columns = [desc[0] for desc in cur.description]
                rows = [dict(zip(columns, row)) for row in cur.fetchall()]

        return jsonify({"count": len(rows), "submissions": rows}), 200

    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/submissions/<int:sub_id>", methods=["GET"])
def get_submission(sub_id):
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT * FROM survey_submissions WHERE id = %s
                """, (sub_id,))
                columns = [desc[0] for desc in cur.description]
                row = cur.fetchone()
                if not row:
                    return jsonify({"error": "Not found"}), 404
                data = dict(zip(columns, row))

        return jsonify(data), 200

    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


with app.app_context():
    init_db()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
