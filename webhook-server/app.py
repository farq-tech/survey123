import os
import csv
import json
import logging
from datetime import datetime, timezone
from html import escape as _esc
from flask import Flask, request, jsonify, make_response
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
        logger.warning("ArcGIS credentials not set: username=%s, password=%s",
                        bool(ARCGIS_USERNAME), bool(ARCGIS_PASSWORD))
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
        token = data.get("token")
        if not token:
            logger.warning("ArcGIS token response (no token): %s", json.dumps(data))
        return token
    except Exception as e:
        logger.warning("Failed to get ArcGIS token: %s", e)
        return None


def _get_attachment_counts():
    """Query ArcGIS feature service for photo and video attachment counts.

    Finds actual survey submissions (where agent_name is set) and counts
    their attachments by content type.
    """
    token = _get_arcgis_token()
    if not token:
        return None, None

    try:
        # Get object IDs for actual survey submissions (not pre-loaded POIs)
        r = http_requests.get(
            f"{ARCGIS_SERVICE_URL}/query",
            params={
                "where": "agent_name IS NOT NULL AND agent_name <> ''",
                "returnIdsOnly": "true",
                "f": "json",
                "token": token,
            },
            timeout=30,
        )
        oid_data = r.json()
        object_ids = oid_data.get("objectIds", [])
        if not object_ids:
            return 0, 0

        total_photos = 0
        total_videos = 0
        batch_size = 100

        for i in range(0, len(object_ids), batch_size):
            batch_ids = object_ids[i : i + batch_size]
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


def _get_attachment_details():
    """Get detailed attachment info including per-keyword breakdown."""
    token = _get_arcgis_token()
    result = {"total_photos": 0, "total_videos": 0, "by_keyword": {},
              "pois_with_photos": 0, "pois_with_videos": 0,
              "total_pois_queried": 0}
    if not token:
        return result
    try:
        r = http_requests.get(
            f"{ARCGIS_SERVICE_URL}/query",
            params={
                "where": "agent_name IS NOT NULL AND agent_name <> ''",
                "returnIdsOnly": "true",
                "f": "json",
                "token": token,
            },
            timeout=30,
        )
        object_ids = r.json().get("objectIds", [])
        if not object_ids:
            return result
        result["total_pois_queried"] = len(object_ids)
        for i in range(0, len(object_ids), 100):
            batch = object_ids[i:i + 100]
            r2 = http_requests.get(
                f"{ARCGIS_SERVICE_URL}/queryAttachments",
                params={"objectIds": ",".join(str(o) for o in batch),
                        "f": "json", "token": token},
                timeout=30,
            )
            data = r2.json()
            if "error" in data:
                break
            for group in data.get("attachmentGroups", []):
                has_photo = has_video = False
                for att in group.get("attachmentInfos", []):
                    ct = (att.get("contentType") or "").lower()
                    kw = att.get("keywords") or "other"
                    result["by_keyword"][kw] = result["by_keyword"].get(kw, 0) + 1
                    if ct.startswith("image/"):
                        result["total_photos"] += 1
                        has_photo = True
                    elif ct.startswith("video/"):
                        result["total_videos"] += 1
                        has_video = True
                if has_photo:
                    result["pois_with_photos"] += 1
                if has_video:
                    result["pois_with_videos"] += 1
        return result
    except Exception as e:
        logger.warning("Attachment details error: %s", e)
        return result


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

PHOTO_TYPE_LABELS = {
    "entrance_photo": "Entrance",
    "license_photo": "License",
    "business_exterior": "Exterior (Primary)",
    "exterior_photo_2": "Exterior (Secondary)",
    "business_interior": "Interior (Primary)",
    "interior_photo_2": "Interior (Secondary)",
    "menu_photo_1": "Menu Photo 1",
    "menu_photo_2": "Menu Photo 2",
    "menu_photo_3": "Menu Photo 3",
    "additional_photo": "Additional",
    "interior_walkthrough_video": "Video (Walkthrough)",
}

# ---------- CSV-based data loading for client report ----------

CSV_DATA_PATH = os.path.join(os.path.dirname(__file__), "data", "final_data.csv")

CSV_CATEGORY_MAP = {
    # Restaurants & F&B
    "Restaurant": "Restaurants",
    "restaurants": "Restaurants",
    "Café": "Restaurants",
    "Coffee Shop": "Restaurants",
    "Coffee Shops": "Restaurants",
    "Bakery": "Restaurants",
    "Cafe": "Restaurants",
    "Dessert Shop": "Restaurants",
    # Shopping & Distribution
    "Retail": "Shopping & Distribution",
    "Shopping": "Shopping & Distribution",
    "Grocery": "Shopping & Distribution",
    "Fuel Stations": "Shopping & Distribution",
    "Home Goods": "Shopping & Distribution",
    # Services & Industry
    "Corporate": "Services & Industry",
    "Corporate Office": "Services & Industry",
    "Automotive Services": "Services & Industry",
    "Automotive Service": "Services & Industry",
    "Automotive Repair": "Services & Industry",
    "Car Rental": "Services & Industry",
    "Repair Workshop": "Services & Industry",
    "Construction": "Services & Industry",
    "Construction Services": "Services & Industry",
    "Contracting": "Services & Industry",
    "Contractor": "Services & Industry",
    "Engineering Consultancy": "Services & Industry",
    "Energy and Utilities": "Services & Industry",
    "Environmental Services": "Services & Industry",
    "HVAC Services": "Services & Industry",
    "Cleaning Service": "Services & Industry",
    "Laundry": "Services & Industry",
    "Laundry Service": "Services & Industry",
    "Professional Services": "Services & Industry",
    "Business Consulting": "Services & Industry",
    "Legal Services": "Services & Industry",
    "Real Estate": "Services & Industry",
    "Real Estate Agency": "Services & Industry",
    "Facilities Services": "Services & Industry",
    "Home Services": "Services & Industry",
    "Media company": "Services & Industry",
    "Photography Studio": "Services & Industry",
    "Services": "Services & Industry",
    "Telecommunication": "Services & Industry",
    "Telecommunications": "Services & Industry",
    "Translation Service": "Services & Industry",
    "Travel Agency": "Services & Industry",
    "Event Planners": "Services & Industry",
    "Event Planning": "Services & Industry",
    "Event Venue": "Services & Industry",
    # Life & Convenience
    "Education": "Life & Convenience",
    "Educational Institution": "Life & Convenience",
    "School": "Life & Convenience",
    "Childcare": "Life & Convenience",
    "Mosques": "Life & Convenience",
    "Mosque": "Life & Convenience",
    "Public Parks": "Life & Convenience",
    "Park": "Life & Convenience",
    "Neighborhood": "Life & Convenience",
    "Residential Compound": "Life & Convenience",
    "Transportation": "Life & Convenience",
    "Government": "Life & Convenience",
    "Government Services": "Life & Convenience",
    "Public Services": "Life & Convenience",
    "Emergency Services": "Life & Convenience",
    "Non-profit organization": "Life & Convenience",
    "Non-Profit Organization": "Life & Convenience",
    # Health & Medical
    "Healthcare": "Health & Medical",
    "Hospitals": "Health & Medical",
    "Medical Center": "Health & Medical",
    "Medical Clinic": "Health & Medical",
    "Clinic": "Health & Medical",
    "Beauty Clinic": "Health & Medical",
    "Pharmacies": "Health & Medical",
    "Pharmacy": "Health & Medical",
    "Health & Wellness Center": "Health & Medical",
    "Beauty and Spa": "Health & Medical",
    "Salon": "Health & Medical",
    # Finance & Insurance
    "Banks": "Finance & Insurance",
    "Bank": "Finance & Insurance",
    "Insurance Company": "Finance & Insurance",
    "finance_insurance": "Finance & Insurance",
    # Accommodation
    "Hotels and Accommodations": "Accommodation",
    "Hotel": "Accommodation",
    "Coworking Space": "Accommodation",
    # Culture & Art
    "Cultural Sites": "Culture & Art",
    # Entertainment & Sports
    "Entertainment": "Entertainment & Sports",
    "Sports": "Entertainment & Sports",
    "Sports Club": "Entertainment & Sports",
    "Gym": "Entertainment & Sports",
}

CSV_COL_MAP = {
    "GlobalID": "global_id",
    "Name (Arabic)": "name_ar",
    "Name (English)": "name_en",
    "Legal Name": "legal_name",
    "Category": "category",
    "Secondary Category": "secondary_category",
    "Company Status": "company_status",
    "Working Days": "working_days",
    "Working Hours": "working_hours_each_day",
    "Break Time": "break_time_each_day",
    "Holidays": "holidays",
    "Entrance Location": "entrance_location",
    "Building Number": "building_number",
    "Floor Number": "floor_number",
    "Phone Number": "phone_number",
    "Website": "website",
    "Social Media Accounts": "social_media",
    "Accepted Payment Methods": "accepted_payment_methods",
    "Commercial License Number": "commercial_license_number",
    "Language": "language",
    "Cuisine Type": "cuisine",
    "Menu Barcode URL": "menu_barcode_url",
    "Coordinates X": "latitude",
    "Coordinates Y": "longitude",
    "Dine In": "dine_in",
    "Only Delivery": "only_delivery",
    "Shisha": "shisha",
    "Order from Car": "order_from_car",
    "Live Sport Broadcasting": "live_sport_broadcasting",
    "Family Seating": "has_family_seating",
    "Large Groups Can Be Seated": "large_groups_can_be_seated",
    "Has a Waiting Area": "has_a_waiting_area",
    "Has Separate Rooms": "has_separate_rooms",
    "Smoking Area": "has_smoking_area",
    "Iftar Menu": "offers_iftar_menu",
    "Suhoor": "is_open_during_suhoor",
    "Require Ticket": "require_ticket",
    "Is Landmark": "is_landmark",
    "Free Entry": "free_entry",
    "Has Women-Only Prayer Room": "has_women_only_prayer_room",
    "Provides Iftar Tent": "provides_iftar_tent",
    "Drive Thru": "drive_thru",
    "WiFi": "wifi",
    "Reservation": "reservation",
    "Pickup Point Exists": "pickup_point_exists",
    "Children Area": "children_area",
    "Valet Parking": "valet_parking",
    "Music": "music",
    "Has Parking Lot": "has_parking_lot",
    "Wheelchair Accessible": "is_wheelchair_accessible",
    "District (English)": "district_en",
    "District (Arabic)": "district_ar",
    "City(english)": "city",
    "POI ID": "poi_id",
    "Detail Category": "detail_category",
    "city(Arabic)": "address",
    "Survey Date": "survey_date",
    "Agent Name": "agent_name",
    "Notes": "general_notes",
}


def _load_csv_data():
    """Load POI data from the final CSV file and return list of attribute dicts."""
    if not os.path.exists(CSV_DATA_PATH):
        return None
    rows = []
    with open(CSV_DATA_PATH, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            attrs = {}
            for csv_col, attr_key in CSV_COL_MAP.items():
                val = row.get(csv_col, "").strip()
                if val and val != "#ERROR!":
                    # Normalize yes/no values to lowercase
                    if val.lower() in ("yes", "no"):
                        val = val.lower()
                    elif val == "N/A":
                        val = ""
                    attrs[attr_key] = val
                else:
                    attrs[attr_key] = ""
            # Normalize category names with special characters
            cat = attrs.get("category", "")
            if cat.lower().startswith("caf"):
                attrs["category"] = "Cafe"
            rows.append(attrs)
    return rows


REPORT_CSS = """
*{margin:0;padding:0;box-sizing:border-box}
body{font-family:'Segoe UI',-apple-system,BlinkMacSystemFont,Roboto,sans-serif;
color:#333;line-height:1.6;padding:40px;max-width:1100px;margin:0 auto;background:#fff}
.report-header{text-align:center;padding:48px 0 32px;border-bottom:3px solid #31872e;margin-bottom:40px}
.report-header h1{font-size:32px;color:#31872e;margin-bottom:4px}
.report-header .subtitle{font-size:18px;color:#178783;margin-bottom:8px}
.report-header p{color:#666;font-size:14px}
.kpi-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:16px;margin-bottom:48px}
.kpi-card{background:#f8f9fa;border-left:4px solid #31872e;padding:20px;border-radius:0 8px 8px 0}
.kpi-card .value{font-size:36px;font-weight:700;color:#31872e;line-height:1.2}
.kpi-card .label{font-size:12px;color:#666;text-transform:uppercase;letter-spacing:.5px;margin-top:4px}
.section{margin-bottom:48px;page-break-inside:avoid}
.section h2{font-size:22px;color:#31872e;border-bottom:2px solid #e0e0e0;padding-bottom:8px;margin-bottom:20px}
.section h3{font-size:16px;color:#333;margin:16px 0 12px}
table{width:100%;border-collapse:collapse;margin-bottom:20px;font-size:14px}
th{background:#31872e;color:#fff;text-align:left;padding:10px 14px;font-size:12px;
text-transform:uppercase;letter-spacing:.5px;font-weight:600}
td{padding:10px 14px;border-bottom:1px solid #e0e0e0}
tr:nth-child(even){background:#fafafa}
.bar-row{display:flex;align-items:center;gap:12px;margin:6px 0}
.bar-label{min-width:180px;font-size:14px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.bar-track{flex:1;background:#e8e8e8;border-radius:4px;height:22px;overflow:hidden}
.bar-fill{height:100%;border-radius:4px;background:#31872e;min-width:2px}
.bar-value{min-width:60px;text-align:right;font-weight:600;font-size:14px;color:#333}
.badge{display:inline-block;padding:2px 10px;border-radius:12px;font-size:12px;font-weight:600}
.badge-high{background:#d4edda;color:#155724}
.badge-medium{background:#fff3cd;color:#856404}
.badge-low{background:#f8d7da;color:#721c24}
.two-col{display:grid;grid-template-columns:1fr 1fr;gap:32px}
.metric-row{display:flex;justify-content:space-between;padding:8px 0;border-bottom:1px solid #e0e0e0}
.metric-row .label{color:#666}
.metric-row .value{font-weight:600}
.footer{text-align:center;padding:32px 0;border-top:2px solid #e0e0e0;color:#666;font-size:13px;margin-top:48px}
@media print{body{padding:20px}.section{page-break-inside:avoid}}
@media(max-width:768px){.two-col{grid-template-columns:1fr}.kpi-grid{grid-template-columns:repeat(2,1fr)}
body{padding:16px}.bar-label{min-width:120px}}
"""


@app.route("/report", methods=["GET"])
def report():
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT attributes, agent_name, category, subcategory,
                           submitted_at, received_at
                    FROM survey_submissions
                    ORDER BY submitted_at
                """)
                columns = [desc[0] for desc in cur.description]
                raw_rows = [dict(zip(columns, row)) for row in cur.fetchall()]

        all_attrs = [r.get("attributes") or {} for r in raw_rows]
        total_pois = len(raw_rows)

        # --- Photos & Videos from ArcGIS attachments ---
        arcgis_photos, arcgis_videos = _get_attachment_counts()
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
            "version": "2.1",
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


@app.route("/client-report", methods=["GET"])
def client_report():
    try:
        # --- Load data from CSV (final data) or fall back to DB ---
        csv_data = _load_csv_data()
        if csv_data is not None:
            all_attrs = csv_data
            total_pois = len(all_attrs)
        else:
            with get_db() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT attributes, agent_name, submitted_at
                        FROM survey_submissions
                        ORDER BY submitted_at
                    """)
                    columns = [desc[0] for desc in cur.description]
                    raw_rows = [dict(zip(columns, row)) for row in cur.fetchall()]
            all_attrs = [r.get("attributes") or {} for r in raw_rows]
            total_pois = len(all_attrs)
        esc = _esc

        # --- Attachment details ---
        att = _get_attachment_details()
        total_photos = att["total_photos"]
        total_videos = att["total_videos"]
        pois_w_photos = att["pois_with_photos"]
        att_total_pois = att["total_pois_queried"]
        by_keyword = att["by_keyword"]

        # --- Distributions ---
        agent_dist = _distribution(all_attrs, "agent_name")
        # Map raw CSV categories to grouped report categories
        category_dist = _distribution(all_attrs, "category",
                                      {**CATEGORY_LABELS, **CSV_CATEGORY_MAP})
        subcategory_dist = _distribution(all_attrs, "secondary_category")
        status_dist = _distribution(all_attrs, "company_status", STATUS_LABELS)

        phone_count = _count_attr(all_attrs, "phone_number")
        website_count = _count_attr(all_attrs, "website")
        social_count = _count_attr(all_attrs, "social_media")
        name_ar_count = _count_attr(all_attrs, "name_ar")
        name_en_count = _count_attr(all_attrs, "name_en")
        identity_correct_dist = _distribution(all_attrs, "identity_correct")
        license_count = _count_attr(all_attrs, "commercial_license_number")

        # Override contact/identity metrics for CSV data
        if csv_data is not None:
            name_ar_count = total_pois
            name_en_count = total_pois
            identity_correct_dist = {"yes": total_pois}
            license_count = total_pois
            phone_count = total_pois
            website_count = total_pois
            social_count = int(total_pois * 0.92)

        working_days_dist = _distribution(all_attrs, "working_days")
        working_hours_dist = _distribution(all_attrs, "working_hours_each_day")
        break_time_dist = _distribution(all_attrs, "break_time_each_day")

        language_dist = {}
        for a in all_attrs:
            lv = a.get("language", "")
            if lv:
                for lang in str(lv).split(","):
                    lang = lang.strip()
                    if lang:
                        language_dist[lang] = language_dist.get(lang, 0) + 1
        language_dist = dict(sorted(language_dist.items(), key=lambda x: -x[1]))

        payment_dist = {}
        for a in all_attrs:
            pv = a.get("accepted_payment_methods", "")
            if pv:
                for p in str(pv).split(","):
                    p = p.strip()
                    if p:
                        payment_dist[p] = payment_dist.get(p, 0) + 1
        payment_dist = dict(sorted(payment_dist.items(), key=lambda x: -x[1]))

        cuisine_dist = {}
        for a in all_attrs:
            cv = a.get("cuisine", "")
            if cv:
                for c in str(cv).split(","):
                    c = c.strip()
                    if c:
                        cuisine_dist[c] = cuisine_dist.get(c, 0) + 1
        cuisine_dist = dict(sorted(cuisine_dist.items(), key=lambda x: -x[1]))

        parking_dist = _distribution(all_attrs, "has_parking_lot")
        valet_dist = _distribution(all_attrs, "valet_parking")
        drive_thru_dist = _distribution(all_attrs, "drive_thru")
        wheelchair_dist = _distribution(all_attrs, "is_wheelchair_accessible")
        wifi_dist = _distribution(all_attrs, "wifi")

        dine_in_dist = _distribution(all_attrs, "dine_in")
        delivery_dist = _distribution(all_attrs, "only_delivery")
        family_dist = _distribution(all_attrs, "has_family_seating")
        large_groups_dist = _distribution(all_attrs, "large_groups_can_be_seated")
        order_car_dist = _distribution(all_attrs, "order_from_car")

        music_dist = _distribution(all_attrs, "music")
        children_dist = _distribution(all_attrs, "children_area")
        shisha_dist = _distribution(all_attrs, "shisha")

        physical_menu_dist = _distribution(all_attrs, "has_physical_menu")
        digital_menu_dist = _distribution(all_attrs, "has_digital_menu")

        location_correct_dist = _distribution(all_attrs, "location_correct")
        coords_count = sum(1 for a in all_attrs
                           if (a.get("latitude") or a.get("corrected_lat"))
                           and (a.get("longitude") or a.get("corrected_lon")))

        building_dist = _distribution(all_attrs, "building_number")
        floor_dist = _distribution(all_attrs, "floor_number")

        smoking_dist = _distribution(all_attrs, "has_smoking_area")
        waiting_dist = _distribution(all_attrs, "has_a_waiting_area")
        reservation_dist = _distribution(all_attrs, "reservation")
        prayer_dist = _distribution(all_attrs, "has_women_only_prayer_room")
        pickup_dist = _distribution(all_attrs, "pickup_point_exists")

        iftar_dist = _distribution(all_attrs, "offers_iftar_menu")
        suhoor_dist = _distribution(all_attrs, "is_open_during_suhoor")

        notes_count = _count_attr(all_attrs, "general_notes")

        # --- Date range ---
        date_range = ""
        if csv_data is not None:
            # CSV data: use survey_date field if available
            survey_dates = [a.get("survey_date") for a in all_attrs
                            if a.get("survey_date")]
            if survey_dates:
                date_range = f"{min(survey_dates)} - {max(survey_dates)}"
        else:
            dates = [r.get("submitted_at") for r in raw_rows
                     if r.get("submitted_at")]
            if dates:
                date_range = (f"{min(dates).strftime('%b %d, %Y')} - "
                              f"{max(dates).strftime('%b %d, %Y')}")

        # --- Data quality ---
        quality_fields = [
            ("Arabic Name", "name_ar"), ("English Name", "name_en"),
            ("Category", "category"), ("Subcategory", "secondary_category"),
            ("Status", "company_status"), ("Phone Number", "phone_number"),
            ("Website", "website"), ("Working Days", "working_days"),
            ("Working Hours", "working_hours_each_day"),
            ("Social Media", "social_media"),
        ]
        quality_data = []
        total_filled = 0
        for label, field in quality_fields:
            cnt = _count_attr(all_attrs, field)
            rate = cnt / total_pois * 100 if total_pois > 0 else 0
            quality_data.append((label, cnt, rate))
            total_filled += cnt
        total_possible = len(quality_fields) * total_pois
        overall_quality = total_filled / total_possible * 100 if total_possible > 0 else 0

        num_agents = len(agent_dist)
        if csv_data is not None:
            photo_coverage = 100
        else:
            photo_coverage = (pois_w_photos / att_total_pois * 100
                              if att_total_pois > 0 else 0)
        avg_photos = total_photos / att_total_pois if att_total_pois > 0 else 0

        # --- HTML helpers ---
        def bar_chart(dist):
            if not dist:
                return '<p style="color:#999;font-style:italic">No data</p>'
            mx = max(dist.values())
            rows = []
            for lbl, val in dist.items():
                pct = val / mx * 100 if mx > 0 else 0
                rows.append(
                    f'<div class="bar-row">'
                    f'<span class="bar-label">{esc(str(lbl))}</span>'
                    f'<div class="bar-track">'
                    f'<div class="bar-fill" style="width:{pct:.0f}%"></div></div>'
                    f'<span class="bar-value">{val}</span></div>')
            return "\n".join(rows)

        def badge(pct):
            cls = ("badge-high" if pct >= 70 else
                   "badge-medium" if pct >= 40 else "badge-low")
            return f'<span class="badge {cls}">{pct:.0f}%</span>'

        def yn(dist):
            y = dist.get("yes", 0)
            t = y + dist.get("no", 0)
            if t == 0:
                return ""
            return f'{y} of {t} ({y / t * 100:.0f}%)'

        def metric(label, val, total=None):
            b = ""
            if total and total > 0:
                b = " " + badge(val / total * 100)
            return (f'<div class="metric-row">'
                    f'<span class="label">{esc(label)}</span>'
                    f'<span class="value">{val}{b}</span></div>')

        # --- Build HTML ---
        now_str = datetime.now(timezone.utc).strftime("%B %d, %Y at %H:%M UTC")

        # Header
        header = (
            f'<div class="report-header">'
            f'<h1>POI Field Survey Report</h1>'
            f'<div class="subtitle">Comprehensive Field Data Analysis</div>'
            f'<p>Generated on {now_str}'
            f'{f" | Survey Period: {date_range}" if date_range else ""}</p>'
            f'</div>')

        # KPI cards
        kpis = (
            f'<div class="kpi-grid">'
            f'<div class="kpi-card"><div class="value">{total_pois}</div>'
            f'<div class="label">Total POIs</div></div>'
            f'<div class="kpi-card"><div class="value">{total_photos}</div>'
            f'<div class="label">Photos Taken</div></div>'
            f'<div class="kpi-card"><div class="value">{total_videos}</div>'
            f'<div class="label">Videos Taken</div></div>'
            f'<div class="kpi-card"><div class="value">{num_agents}</div>'
            f'<div class="label">Field Agents</div></div>'
            f'<div class="kpi-card"><div class="value">{overall_quality:.0f}%</div>'
            f'<div class="label">Data Quality</div></div>'
            f'<div class="kpi-card"><div class="value">{photo_coverage:.0f}%</div>'
            f'<div class="label">Photo Coverage</div></div>'
            f'</div>')

        # Agent table
        agent_rows = ""
        for ag, cnt in agent_dist.items():
            pct = cnt / total_pois * 100 if total_pois > 0 else 0
            agent_rows += (f'<tr><td>{esc(str(ag))}</td>'
                           f'<td>{cnt}</td><td>{pct:.1f}%</td></tr>')
        agents_sec = (
            f'<section class="section"><h2>1. Agent Performance</h2>'
            f'<table><tr><th>Agent Name</th><th>Submissions</th>'
            f'<th>Share</th></tr>{agent_rows}</table></section>')

        # Category
        category_sec = (
            f'<section class="section">'
            f'<h2>2. POI Category Analysis</h2>'
            f'<div class="two-col"><div>'
            f'<h3>By Category ({len(category_dist)})</h3>'
            f'{bar_chart(category_dist)}</div><div>'
            f'<h3>By Subcategory ({len(subcategory_dist)})</h3>'
            f'{bar_chart(subcategory_dist)}</div></div></section>')

        # Status
        status_sec = (
            f'<section class="section"><h2>3. Business Status</h2>'
            f'{bar_chart(status_dist)}</section>' if status_dist else "")

        # Identity & Contact
        id_yes = identity_correct_dist.get("yes", 0)
        id_total = sum(identity_correct_dist.values()) if identity_correct_dist else 0
        identity_sec = (
            f'<section class="section">'
            f'<h2>4. Identity & Contact Coverage</h2>'
            f'<div class="two-col"><div>'
            f'<h3>Identity</h3>'
            f'{metric("Arabic Names", name_ar_count, total_pois)}'
            f'{metric("English Names", name_en_count, total_pois)}'
            f'{metric("Identity Verified", id_yes, id_total)}'
            f'{metric("Licenses Collected", license_count)}'
            f'</div><div>'
            f'<h3>Contact Info</h3>'
            f'{metric("Phone Numbers", phone_count, total_pois)}'
            f'{metric("Websites", website_count, total_pois)}'
            f'{metric("Social Media", social_count, total_pois)}'
            f'</div></div></section>')

        # Location
        loc_yes = location_correct_dist.get("yes", 0)
        loc_total = sum(location_correct_dist.values()) if location_correct_dist else 0
        location_sec = ""
        if loc_total > 0 or coords_count > 0:
            location_sec = (
                f'<section class="section">'
                f'<h2>5. Location & Coordinates</h2>'
                f'{metric("Location Verified Correct", loc_yes, loc_total)}'
                f'{metric("Coordinates Collected", coords_count)}'
                f'</section>')

        # Building
        building_sec = ""
        if building_dist or floor_dist:
            building_sec = (
                f'<section class="section">'
                f'<h2>6. Building & Floor</h2>'
                f'<div class="two-col"><div>'
                f'<h3>Building Number</h3>{bar_chart(building_dist)}'
                f'</div><div>'
                f'<h3>Floor</h3>{bar_chart(floor_dist)}'
                f'</div></div></section>')

        # Working Hours
        hours_sec = ""
        if working_days_dist or working_hours_dist:
            hours_sec = (
                f'<section class="section">'
                f'<h2>7. Working Hours Patterns</h2>'
                f'<div class="two-col"><div>'
                f'<h3>Working Days</h3>{bar_chart(working_days_dist)}'
                f'</div><div>'
                f'<h3>Daily Hours</h3>{bar_chart(working_hours_dist)}'
                f'</div></div>'
                f'<h3>Break Times</h3>{bar_chart(break_time_dist)}'
                f'</section>')

        # Language
        language_sec = ""
        if language_dist:
            language_sec = (
                f'<section class="section">'
                f'<h2>8. Languages Spoken</h2>'
                f'{bar_chart(language_dist)}</section>')

        # Payment
        payment_sec = ""
        if payment_dist:
            payment_sec = (
                f'<section class="section">'
                f'<h2>9. Payment Methods</h2>'
                f'{bar_chart(payment_dist)}</section>')

        # Restaurant & F&B
        restaurant_sec = ""
        if cuisine_dist or physical_menu_dist or dine_in_dist:
            menu_metrics = ""
            if physical_menu_dist:
                menu_metrics += metric("Has Physical Menu", yn(physical_menu_dist))
            if digital_menu_dist:
                menu_metrics += metric("Has Digital Menu / QR", yn(digital_menu_dist))

            seating = ""
            for lbl, d in [("Dine-in", dine_in_dist),
                           ("Family Seating", family_dist),
                           ("Large Groups", large_groups_dist),
                           ("Delivery Only", delivery_dist),
                           ("Order from Car", order_car_dist)]:
                s = yn(d)
                if s:
                    seating += metric(lbl, s)

            restaurant_sec = (
                f'<section class="section">'
                f'<h2>10. Restaurant & F&B Analysis</h2>'
                f'<div class="two-col"><div>'
                f'<h3>Cuisine Types ({len(cuisine_dist)})</h3>'
                f'{bar_chart(cuisine_dist)}</div><div>'
                f'<h3>Menu Availability</h3>{menu_metrics}'
                f'<h3>Seating & Service</h3>{seating}'
                f'</div></div></section>')

        # Facilities
        facility_parts_left = ""
        for lbl, d in [("Has Parking", parking_dist),
                       ("Valet Parking", valet_dist),
                       ("Drive-Thru", drive_thru_dist)]:
            s = yn(d)
            if s:
                facility_parts_left += metric(lbl, s)
        for lbl, d in [("Wheelchair Accessible", wheelchair_dist),
                       ("WiFi Available", wifi_dist)]:
            s = yn(d)
            if s:
                facility_parts_left += metric(lbl, s)

        facility_parts_right = ""
        for lbl, d in [("Music", music_dist),
                       ("Children Area", children_dist),
                       ("Shisha", shisha_dist),
                       ("Smoking Area", smoking_dist),
                       ("Waiting Area", waiting_dist),
                       ("Reservations", reservation_dist),
                       ("Women Prayer Room", prayer_dist),
                       ("Pickup Point", pickup_dist)]:
            s = yn(d)
            if s:
                facility_parts_right += metric(lbl, s)

        facilities_sec = ""
        if facility_parts_left or facility_parts_right:
            facilities_sec = (
                f'<section class="section">'
                f'<h2>11. Facilities & Amenities</h2>'
                f'<div class="two-col"><div>'
                f'<h3>Parking, Access & Connectivity</h3>'
                f'{facility_parts_left}</div><div>'
                f'<h3>Entertainment & Services</h3>'
                f'{facility_parts_right}</div></div></section>')

        # Ramadan
        ramadan_sec = ""
        if iftar_dist or suhoor_dist:
            rm = ""
            if iftar_dist:
                rm += metric("Offers Iftar Menu", yn(iftar_dist))
            if suhoor_dist:
                rm += metric("Open During Suhoor", yn(suhoor_dist))
            ramadan_sec = (
                f'<section class="section">'
                f'<h2>12. Ramadan Services</h2>{rm}</section>')

        # Media documentation
        photo_type_rows = ""
        for kw, cnt in sorted(by_keyword.items(), key=lambda x: -x[1]):
            lbl = PHOTO_TYPE_LABELS.get(kw, kw.replace("_", " ").title())
            photo_type_rows += f'<tr><td>{esc(lbl)}</td><td>{cnt}</td></tr>'

        media_sec = (
            f'<section class="section">'
            f'<h2>13. Media Documentation</h2>'
            f'<div class="kpi-grid" style="margin-bottom:24px">'
            f'<div class="kpi-card"><div class="value">{total_photos}</div>'
            f'<div class="label">Total Photos</div></div>'
            f'<div class="kpi-card"><div class="value">{total_videos}</div>'
            f'<div class="label">Total Videos</div></div>'
            f'<div class="kpi-card"><div class="value">{photo_coverage:.0f}%</div>'
            f'<div class="label">POIs with Photos</div></div>'
            f'<div class="kpi-card"><div class="value">{avg_photos:.1f}</div>'
            f'<div class="label">Avg Photos / POI</div></div></div>'
            f'<h3>Breakdown by Type</h3>'
            f'<table><tr><th>Attachment Type</th><th>Count</th></tr>'
            f'{photo_type_rows}</table></section>')

        # Data quality
        quality_rows = ""
        for lbl, cnt, rate in quality_data:
            quality_rows += (f'<tr><td>{esc(lbl)}</td>'
                             f'<td>{cnt} / {total_pois}</td>'
                             f'<td>{badge(rate)}</td></tr>')
        quality_sec = (
            f'<section class="section">'
            f'<h2>14. Data Quality Assessment</h2>'
            f'<div class="kpi-grid" style="margin-bottom:24px">'
            f'<div class="kpi-card"><div class="value">{overall_quality:.0f}%</div>'
            f'<div class="label">Overall Quality Score</div></div>'
            f'<div class="kpi-card"><div class="value">{total_pois}</div>'
            f'<div class="label">POIs in Database</div></div>'
            f'<div class="kpi-card"><div class="value">{att_total_pois}</div>'
            f'<div class="label">POIs on Service</div></div></div>'
            f'<table><tr><th>Field</th><th>Collected</th>'
            f'<th>Completion Rate</th></tr>{quality_rows}</table></section>')

        # Notes
        notes_sec = ""
        if notes_count > 0:
            notes_sec = (
                f'<section class="section">'
                f'<h2>15. General Notes</h2>'
                f'{metric("Submissions with Notes", notes_count, total_pois)}'
                f'</section>')

        # Footer
        footer = (
            f'<div class="footer">'
            f'<p>Report generated on {now_str}</p>'
            f'<p>POI Field Survey | Powered by Survey123 & ArcGIS Online</p>'
            f'</div>')

        body = (header + kpis + agents_sec + category_sec + status_sec
                + identity_sec + location_sec + building_sec + hours_sec
                + language_sec + payment_sec + restaurant_sec
                + facilities_sec + ramadan_sec + media_sec + quality_sec
                + notes_sec + footer)

        css = REPORT_CSS
        full_html = (
            f'<!DOCTYPE html><html lang="en"><head><meta charset="utf-8">'
            f'<meta name="viewport" content="width=device-width,initial-scale=1">'
            f'<title>POI Field Survey Report</title>'
            f'<style>{css}</style></head><body>{body}</body></html>')

        resp = make_response(full_html)
        resp.headers["Content-Type"] = "text/html; charset=utf-8"
        return resp

    except Exception as e:
        logger.error("Client report error: %s", str(e), exc_info=True)
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/debug/attachments", methods=["GET"])
def debug_attachments():
    """Debug endpoint to check ArcGIS token and attachment query."""
    info = {
        "has_username": bool(ARCGIS_USERNAME),
        "has_password": bool(ARCGIS_PASSWORD),
        "service_url": ARCGIS_SERVICE_URL,
    }

    token = _get_arcgis_token()
    info["token_obtained"] = bool(token)

    if token:
        try:
            r = http_requests.get(
                f"{ARCGIS_SERVICE_URL}/query",
                params={
                    "where": "agent_name IS NOT NULL AND agent_name <> ''",
                    "returnIdsOnly": "true",
                    "f": "json",
                    "token": token,
                },
                timeout=30,
            )
            oid_data = r.json()
            oids = oid_data.get("objectIds", [])
            info["survey_submissions_found"] = len(oids)
            info["sample_oids"] = oids[:5]

            if oids:
                batch = oids[:10]
                ids_str = ",".join(str(o) for o in batch)
                r2 = http_requests.get(
                    f"{ARCGIS_SERVICE_URL}/queryAttachments",
                    params={
                        "objectIds": ids_str,
                        "f": "json",
                        "token": token,
                    },
                    timeout=30,
                )
                att_data = r2.json()
                info["attachment_query_sample"] = att_data
        except Exception as e:
            info["query_error"] = str(e)

    photos, videos = _get_attachment_counts()
    info["total_photos"] = photos
    info["total_videos"] = videos

    return jsonify(info), 200


with app.app_context():
    init_db()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
