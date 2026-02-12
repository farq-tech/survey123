import os
import json
import logging
from datetime import datetime, timezone
from flask import Flask, request, jsonify
from flask_cors import CORS
import psycopg2
from psycopg2.extras import Json
import requests as http_requests

app = Flask(__name__)
CORS(app)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DATABASE_URL = os.environ.get("DATABASE_URL")
ARCGIS_USERNAME = os.environ.get("ARCGIS_USERNAME")
ARCGIS_PASSWORD = os.environ.get("ARCGIS_PASSWORD")
ARCGIS_SERVICE_URL = os.environ.get(
    "ARCGIS_SERVICE_URL",
    "https://services5.arcgis.com/pYlVm2T6SvR7ytZv/arcgis/rest/services"
    "/service_36f94509389d4a85a311cc6aa9c7398e_form/FeatureServer/0"
)


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


@app.route("/webhook", methods=["POST", "OPTIONS"])
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


def _get_arcgis_token():
    """Get an ArcGIS Online token using stored credentials."""
    if not ARCGIS_USERNAME or not ARCGIS_PASSWORD:
        return None
    try:
        r = http_requests.post(
            "https://www.arcgis.com/sharing/rest/generateToken",
            data={
                "username": ARCGIS_USERNAME,
                "password": ARCGIS_PASSWORD,
                "referer": "https://www.arcgis.com",
                "f": "json",
            },
            timeout=15,
        )
        data = r.json()
        return data.get("token")
    except Exception as e:
        logger.warning("Failed to get ArcGIS token: %s", e)
        return None


def _get_attachment_counts(submission_oids):
    """Query ArcGIS feature service for photo and video attachment counts.

    Only queries attachments for the given object IDs (from our database),
    not the entire feature service which may have thousands of POI records.
    """
    token = _get_arcgis_token()
    if not token:
        return None, None

    # Filter to valid object IDs only
    valid_oids = [oid for oid in submission_oids if oid is not None]
    if not valid_oids:
        return 0, 0

    try:
        total_photos = 0
        total_videos = 0
        batch_size = 100

        for i in range(0, len(valid_oids), batch_size):
            batch_ids = valid_oids[i : i + batch_size]
            ids_str = ",".join(str(oid) for oid in batch_ids)

            r = http_requests.get(
                f"{ARCGIS_SERVICE_URL}/queryAttachments",
                params={
                    "objectIds": ids_str,
                    "f": "json",
                    "token": token,
                },
                timeout=30,
            )
            att_data = r.json()

            if "error" in att_data:
                logger.warning("ArcGIS attachment query error: %s",
                               att_data["error"])
                return None, None

            for group in att_data.get("attachmentGroups", []):
                for att in group.get("attachmentInfos", []):
                    content_type = (att.get("contentType") or "").lower()
                    if content_type.startswith("image/"):
                        total_photos += 1
                    elif content_type.startswith("video/"):
                        total_videos += 1

        return total_photos, total_videos

    except Exception as e:
        logger.warning("Failed to query ArcGIS attachments: %s", e)
        return None, None


def _count_attr(rows, key):
    """Count non-empty values for a given attribute key across all rows."""
    count = 0
    for attrs in rows:
        val = attrs.get(key)
        if val is not None and str(val).strip() != "":
            count += 1
    return count


def _distribution(rows, key, label_map=None):
    """Get value distribution for a given attribute key."""
    dist = {}
    for attrs in rows:
        val = attrs.get(key)
        if val is not None and str(val).strip() != "":
            label = val
            if label_map and val in label_map:
                label = label_map[val]
            dist[label] = dist.get(label, 0) + 1
    return dict(sorted(dist.items(), key=lambda x: -x[1]))


PHOTO_FIELDS = [
    "entrance_photo", "license_photo",
    "business_exterior", "exterior_photo_2",
    "business_interior", "interior_photo_2",
    "menu_photo_1", "menu_photo_2", "menu_photo_3",
    "additional_photo",
]

VIDEO_FIELDS = [
    "interior_walkthrough_video",
]

# Fields to SKIP entirely from the report (business/legal name)
SKIP_FIELDS = ["legal_name"]

CATEGORY_LABELS = {
    "health_medical": "Health & Medical",
    "finance_insurance": "Finance & Insurance",
    "culture_art": "Culture & Art",
    "life_convenience": "Life & Convenience",
    "services_industry": "Services & Industry",
    "shopping_distribution": "Shopping & Distribution",
    "accommodation": "Accommodation",
    "restaurants": "Restaurants",
}

STATUS_LABELS = {
    "open": "Open",
    "closed": "Permanently Closed",
    "temporary_closed": "Temporarily Closed",
    "under_construction": "Under Construction",
    "coming_soon": "Coming Soon",
    "relocated": "Relocated",
}


@app.route("/report", methods=["GET"])
def report():
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT attributes, agent_name, category, subcategory,
                           submitted_at, received_at, object_id
                    FROM survey_submissions
                    ORDER BY submitted_at
                """)
                columns = [desc[0] for desc in cur.description]
                raw_rows = [dict(zip(columns, row)) for row in cur.fetchall()]

        all_attrs = [r.get("attributes") or {} for r in raw_rows]
        submission_oids = [r.get("object_id") for r in raw_rows]
        total_pois = len(raw_rows)

        # --- Photos & Videos from ArcGIS attachments ---
        arcgis_photos, arcgis_videos = _get_attachment_counts(submission_oids)
        total_photos = arcgis_photos if arcgis_photos is not None else 0
        total_videos = arcgis_videos if arcgis_videos is not None else 0

        # --- Agent breakdown ---
        agent_dist = _distribution(all_attrs, "agent_name")

        # --- Category breakdown ---
        category_dist = _distribution(all_attrs, "category", CATEGORY_LABELS)

        # --- Subcategory breakdown ---
        subcategory_dist = _distribution(all_attrs, "secondary_category")

        # --- Status breakdown ---
        status_dist = _distribution(all_attrs, "company_status", STATUS_LABELS)

        # --- Coordinates: only include if there are answers ---
        coords_answered = 0
        for attrs in all_attrs:
            lat = attrs.get("latitude") or attrs.get("corrected_lat")
            lon = attrs.get("longitude") or attrs.get("corrected_lon")
            if lat and lon:
                coords_answered += 1
        location_correct_dist = _distribution(all_attrs, "location_correct")

        # --- Building & Floor ---
        building_dist = _distribution(all_attrs, "building_number")
        floor_dist = _distribution(all_attrs, "floor_number")

        # --- Contact ---
        phone_count = _count_attr(all_attrs, "phone_number")
        website_count = _count_attr(all_attrs, "website")
        social_count = _count_attr(all_attrs, "social_media")

        # --- License ---
        license_count = _count_attr(all_attrs, "commercial_license_number")
        license_photo_count = _count_attr(all_attrs, "license_photo")

        # --- Hours ---
        working_days_dist = _distribution(all_attrs, "working_days")
        working_hours_dist = _distribution(all_attrs, "working_hours_each_day")
        break_time_dist = _distribution(all_attrs, "break_time_each_day")

        # --- Identity (name corrections, EXCLUDING legal_name) ---
        identity_correct_dist = _distribution(all_attrs, "identity_correct")
        name_ar_count = _count_attr(all_attrs, "name_ar")
        name_en_count = _count_attr(all_attrs, "name_en")

        # --- Language ---
        language_dist = {}
        for attrs in all_attrs:
            lang_val = attrs.get("language", "")
            if lang_val:
                for lang in str(lang_val).split(","):
                    lang = lang.strip()
                    if lang:
                        language_dist[lang] = language_dist.get(lang, 0) + 1
        language_dist = dict(sorted(language_dist.items(), key=lambda x: -x[1]))

        # --- Landmark & Pickup ---
        landmark_dist = _distribution(all_attrs, "is_landmark")
        pickup_dist = _distribution(all_attrs, "pickup_point_exists")

        # --- Entrance ---
        entrance_photo_count = _count_attr(all_attrs, "entrance_photo")
        entrance_desc_count = _count_attr(all_attrs, "entrance_description")

        # --- Menu ---
        physical_menu_dist = _distribution(all_attrs, "has_physical_menu")
        digital_menu_dist = _distribution(all_attrs, "has_digital_menu")

        # --- Cuisine ---
        cuisine_dist = {}
        for attrs in all_attrs:
            c_val = attrs.get("cuisine", "")
            if c_val:
                for c in str(c_val).split(","):
                    c = c.strip()
                    if c:
                        cuisine_dist[c] = cuisine_dist.get(c, 0) + 1
        cuisine_dist = dict(sorted(cuisine_dist.items(), key=lambda x: -x[1]))

        # --- Payment ---
        payment_dist = {}
        for attrs in all_attrs:
            p_val = attrs.get("accepted_payment_methods", "")
            if p_val:
                for p in str(p_val).split(","):
                    p = p.strip()
                    if p:
                        payment_dist[p] = payment_dist.get(p, 0) + 1
        payment_dist = dict(sorted(payment_dist.items(), key=lambda x: -x[1]))

        # --- Parking ---
        parking_dist = _distribution(all_attrs, "has_parking_lot")
        valet_dist = _distribution(all_attrs, "valet_parking")
        drive_thru_dist = _distribution(all_attrs, "drive_thru")

        # --- Accessibility ---
        wheelchair_dist = _distribution(all_attrs, "is_wheelchair_accessible")
        wifi_dist = _distribution(all_attrs, "wifi")

        # --- Seating ---
        dine_in_dist = _distribution(all_attrs, "dine_in")
        delivery_dist = _distribution(all_attrs, "only_delivery")
        family_dist = _distribution(all_attrs, "has_family_seating")
        separate_rooms_dist = _distribution(all_attrs, "has_separate_rooms_for_dining")
        large_groups_dist = _distribution(all_attrs, "large_groups_can_be_seated")
        order_car_dist = _distribution(all_attrs, "order_from_car")

        # --- Entertainment ---
        music_dist = _distribution(all_attrs, "music")
        sports_dist = _distribution(all_attrs, "live_sport_broadcasting")
        shisha_dist = _distribution(all_attrs, "shisha")
        children_dist = _distribution(all_attrs, "children_area")

        # --- Smoking & Waiting ---
        smoking_dist = _distribution(all_attrs, "has_smoking_area")
        waiting_dist = _distribution(all_attrs, "has_a_waiting_area")
        reservation_dist = _distribution(all_attrs, "reservation")

        # --- Prayer ---
        prayer_dist = _distribution(all_attrs, "has_women_only_prayer_room")

        # --- Ramadan ---
        iftar_dist = _distribution(all_attrs, "offers_iftar_menu")
        suhoor_dist = _distribution(all_attrs, "is_open_during_suhoor")
        iftar_tent_dist = _distribution(all_attrs, "provides_iftar_tent")

        # --- Special ---
        ticket_dist = _distribution(all_attrs, "require_ticket")
        free_entry_dist = _distribution(all_attrs, "is_free_entry")

        # --- Notes ---
        notes_count = _count_attr(all_attrs, "general_notes")

        # Build report - only include sections that have data
        report_data = {
            "report_title": "POI Field Survey Report",
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "total_pois_gathered": total_pois,
            "total_photos_taken": total_photos,
            "total_videos_taken": total_videos,
            "sections": []
        }

        def add_section(title, data_dict):
            """Only add section if it has non-empty data."""
            # Filter out empty distributions
            filtered = {}
            for k, v in data_dict.items():
                if isinstance(v, dict) and len(v) == 0:
                    continue
                if isinstance(v, int) and v == 0:
                    continue
                if v is None:
                    continue
                filtered[k] = v
            if filtered:
                report_data["sections"].append({
                    "title": title,
                    **filtered
                })

        add_section("Agents", {
            "agent_distribution": agent_dist,
        })

        add_section("1. Identity & Names", {
            "names_arabic_collected": name_ar_count,
            "names_english_collected": name_en_count,
            "identity_correct": identity_correct_dist,
        })

        add_section("2. Category", {
            "category_distribution": category_dist,
            "subcategory_distribution": subcategory_dist,
        })

        add_section("3. Company Status", {
            "status_distribution": status_dist,
        })

        add_section("4. Commercial License", {
            "licenses_collected": license_count,
            "license_photos_taken": license_photo_count,
        })

        add_section("5. Coordinates", {
            "pois_with_coordinates": coords_answered,
            "location_correct": location_correct_dist,
        })

        add_section("6. Building & Floor", {
            "building_distribution": building_dist,
            "floor_distribution": floor_dist,
        })

        add_section("7. Entrance", {
            "entrance_photos_taken": entrance_photo_count,
            "entrance_descriptions": entrance_desc_count,
        })

        add_section("8. Contact Info", {
            "phone_numbers_collected": phone_count,
            "websites_collected": website_count,
            "social_media_collected": social_count,
        })

        add_section("9. Language, Landmark & Pickup", {
            "languages_distribution": language_dist,
            "is_landmark": landmark_dist,
            "pickup_point_exists": pickup_dist,
        })

        add_section("10. Working Hours", {
            "working_days_distribution": working_days_dist,
            "working_hours_distribution": working_hours_dist,
            "break_time_distribution": break_time_dist,
        })

        add_section("11. Business Exterior Photos", {
            "exterior_photos_taken": _count_attr(all_attrs, "business_exterior")
                                    + _count_attr(all_attrs, "exterior_photo_2"),
        })

        add_section("12. Business Interior Photos", {
            "interior_photos_taken": _count_attr(all_attrs, "business_interior")
                                    + _count_attr(all_attrs, "interior_photo_2"),
        })

        add_section("13. Interior Walkthrough Video", {
            "videos_taken": total_videos,
        })

        add_section("14. Physical Menu Photos", {
            "has_physical_menu": physical_menu_dist,
            "menu_photos_taken": _count_attr(all_attrs, "menu_photo_1")
                                + _count_attr(all_attrs, "menu_photo_2")
                                + _count_attr(all_attrs, "menu_photo_3"),
        })

        add_section("15. Digital Menu / QR", {
            "has_digital_menu": digital_menu_dist,
        })

        add_section("16. Cuisine", {
            "cuisine_distribution": cuisine_dist,
        })

        add_section("17. Payment Methods", {
            "payment_distribution": payment_dist,
        })

        add_section("18. Parking & Valet", {
            "has_parking": parking_dist,
            "valet_parking": valet_dist,
            "drive_thru": drive_thru_dist,
        })

        add_section("19. Accessibility & WiFi", {
            "wheelchair_accessible": wheelchair_dist,
            "wifi_available": wifi_dist,
        })

        add_section("20. Seating", {
            "dine_in": dine_in_dist,
            "only_delivery": delivery_dist,
            "family_seating": family_dist,
            "separate_dining_rooms": separate_rooms_dist,
            "large_groups": large_groups_dist,
            "order_from_car": order_car_dist,
        })

        add_section("21. Entertainment", {
            "music": music_dist,
            "live_sports": sports_dist,
            "shisha": shisha_dist,
            "children_area": children_dist,
        })

        add_section("22. Smoking & Waiting", {
            "smoking_area": smoking_dist,
            "waiting_area": waiting_dist,
            "reservation": reservation_dist,
        })

        add_section("23. Prayer Rooms", {
            "women_prayer_room": prayer_dist,
        })

        add_section("24. Iftar & Suhoor", {
            "offers_iftar": iftar_dist,
            "open_during_suhoor": suhoor_dist,
            "iftar_tent": iftar_tent_dist,
        })

        add_section("25. Attraction / Special", {
            "require_ticket": ticket_dist,
            "free_entry": free_entry_dist,
        })

        add_section("Notes", {
            "submissions_with_notes": notes_count,
        })

        return jsonify(report_data), 200

    except Exception as e:
        logger.error("Report error: %s", str(e), exc_info=True)
        return jsonify({"status": "error", "message": str(e)}), 500


with app.app_context():
    init_db()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
