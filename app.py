import mimetypes
import os
import logging
import json
import threading
import time
import uuid
import zipfile
from pathlib import Path

from flask import Flask, Response, flash, g, has_request_context, jsonify, redirect, render_template, request, session, url_for
from markupsafe import Markup, escape
from werkzeug.security import check_password_hash, generate_password_hash
from werkzeug.utils import secure_filename

try:
    import markdown as markdown_lib
except ImportError:  # pragma: no cover
    markdown_lib = None

from tracker_core import (
    DATA_FILE,
    DOCUMENT_PREFIX,
    GENDER_OPTIONS,
    HASH_FILE,
    LEGACY_VALUE,
    LOCATION_PREFIX,
    MONTH_OPTIONS,
    ORGAN_OPTIONS,
    RELATIONSHIP_STYLE_OPTIONS,
    STATUS_OPTIONS,
    SITE_SETTINGS_FILE,
    StorageError,
    STORAGE_SETTINGS_FILE,
    USER_FILE,
    WHEEL_PREFIX,
    add_gallery_item,
    bind_location,
    build_alter_view,
    build_affiliation_view,
    build_dashboard_context,
    build_document_view,
    build_location_view,
    build_wheel_view,
    build_wheels_context,
    can_view_document_for_entry,
    can_view_gallery_for_entry,
    can_view_profile_for_entry,
    can_view_relations_for_entry,
    create_entry_with_level,
    create_affiliation_prefix,
    create_alter_prefix,
    create_relation_tag,
    create_special_relation_tag,
    delete_entry,
    delete_affiliation_prefix,
    delete_alter_prefix,
    entry_is_accessible,
    ensure_storage_files,
    generate_unique_hash,
    get_affiliation_prefixes,
    get_alter_prefixes,
    get_storage,
    import_gallery_media_from_url,
    is_managed_media_url,
    load_data,
    load_site_settings,
    load_storage_settings,
    load_users,
    LocalStorage,
    media_name_from_url,
    media_storage_name,
    managed_media_url,
    migrate_storage_data,
    migrate_gallery_media,
    migrate_legacy_local_files,
    remove_affiliation_membership,
    remove_affiliation_timeline_entry,
    remove_gallery_item,
    remove_memory_entry,
    remove_note_entry,
    remove_occupation_entry,
    remove_relation,
    resolve_entry_reference,
    rename_entry,
    save_alter_profile,
    save_affiliation_summary,
    save_document_record,
    save_site_settings,
    save_storage_settings,
    save_uploaded_json,
    save_users,
    search_entries,
    set_document_locked,
    set_alter_section_lock,
    set_gallery_locked,
    set_relation_tag,
    update_memory_tree,
    update_notes,
    update_affiliation_membership,
    update_affiliation_timeline,
    update_occupation_entry,
    user_can_create,
    user_can_view_gallery,
    user_can_view_documents,
    user_can_view_locked_gallery,
    user_can_view_memory,
    user_can_view_profile,
    user_can_view_relations,
    user_can_view_wheel,
    user_can_edit_wheel,
    save_wheel_settings,
    save_wheel_permissions,
    add_wheel_text_entries,
    add_wheel_image_entry,
    remove_wheel_entry,
    clear_wheel_used_entries,
    spin_wheel,
)


APP_DIR = Path(__file__).resolve().parent
DATA_DIR = Path(os.getenv("DATA_DIR", str(APP_DIR / "data"))).resolve()
DATA_DIR.mkdir(parents=True, exist_ok=True)
IMPORT_JOB_DIR = DATA_DIR / "import_jobs"
IMPORT_JOB_DIR.mkdir(parents=True, exist_ok=True)
migrate_legacy_local_files(APP_DIR, DATA_DIR)
app = Flask(__name__)
app.config["SECRET_KEY"] = os.getenv("SECRET_KEY", "change-me-for-production")
logger = logging.getLogger(__name__)


def render_document_content(document_format, content):
    content = str(content or "")
    if document_format == "html":
        return Markup(content)
    if markdown_lib is not None:
        return Markup(markdown_lib.markdown(content, extensions=["extra", "sane_lists"]))
    return Markup(f"<pre>{escape(content)}</pre>")


def initialize_storage():
    try:
        current_storage = get_storage(DATA_DIR)
        ensure_storage_files(current_storage)
        migrate_gallery_media(current_storage, DATA_DIR)
        return current_storage
    except Exception as error:
        logger.exception("Storage initialization failed")
        raise RuntimeError(f"Storage initialization failed: {error}") from error


storage = initialize_storage()


@app.errorhandler(StorageError)
def handle_storage_error(error):
    clear_request_caches()
    message = str(error) or "Storage is unavailable right now."
    if is_async_request():
        return jsonify({"ok": False, "message": message, "category": "error"}), 503
    flash(message, "error")
    return redirect(request.referrer or url_for("dashboard"))


def refresh_storage():
    global storage
    storage = initialize_storage()
    return storage


def save_gallery_upload(kind, entry_id, file_storage):
    if not file_storage or not file_storage.filename:
        return ""
    suffix = Path(secure_filename(file_storage.filename)).suffix.lower()
    if suffix not in {".jpg", ".jpeg", ".png", ".gif", ".webp"}:
        raise ValueError("Uploads must be JPG, PNG, GIF, or WEBP.")
    filename = f"{os.urandom(12).hex()}{suffix}"
    storage.write_bytes(media_storage_name(kind, entry_id, filename), file_storage.read())
    return managed_media_url(kind, entry_id, filename)


def delete_gallery_upload(image_url):
    media_name = media_name_from_url(image_url)
    if not media_name:
        return
    storage.delete_bytes(media_name)


def delete_wheel_media(entry):
    media_name = media_name_from_url(entry.get("media_url", ""))
    if media_name:
        storage.delete_bytes(media_name)


def import_job_status_path(job_id):
    return IMPORT_JOB_DIR / f"{job_id}.json"


def import_job_upload_path(job_id, suffix):
    return IMPORT_JOB_DIR / f"{job_id}{suffix}"


def write_import_job_status(job_id, payload):
    path = import_job_status_path(job_id)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def read_import_job_status(job_id):
    path = import_job_status_path(job_id)
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def delete_import_job_files(job_id, upload_path=None):
    status_path = import_job_status_path(job_id)
    try:
        if upload_path and Path(upload_path).exists():
            Path(upload_path).unlink()
    except OSError:
        pass
    try:
        if status_path.exists():
            status_path.unlink()
    except OSError:
        pass


def import_wheel_upload(wheel_id, file_storage):
    if not file_storage or not file_storage.filename:
        return False, "Choose a .zip or .txt file.", 0
    suffix = Path(secure_filename(file_storage.filename)).suffix.lower()
    if suffix == ".txt":
        file_storage.stream.seek(0)
        lines = file_storage.stream.read().decode("utf-8", errors="replace").splitlines()
        return (*add_wheel_text_entries(storage, wheel_id, lines), len([line for line in lines if line.strip() and not line.strip().startswith("#")]))
    if suffix != ".zip":
        return False, "Imports must be .zip or .txt.", 0
    added = 0
    try:
        file_storage.stream.seek(0)
        with zipfile.ZipFile(file_storage.stream) as archive:
            for member in archive.infolist():
                if member.is_dir():
                    continue
                member_name = Path(member.filename).name
                lower = member_name.lower()
                if lower.endswith((".png", ".jpg", ".jpeg", ".gif", ".webp", ".bmp", ".tif", ".tiff")):
                    content = archive.read(member)
                    success, _ = add_wheel_image_entry(storage, wheel_id, member_name, content)
                    if success:
                        added += 1
                elif lower.endswith(".txt"):
                    with archive.open(member) as text_file:
                        lines = text_file.read().decode("utf-8", errors="replace").splitlines()
                    success, message = add_wheel_text_entries(storage, wheel_id, lines)
                    if success:
                        added += len([line for line in lines if line.strip() and not line.strip().startswith("#")])
        if added:
            return True, f"Imported {added} wheel entr{'y' if added == 1 else 'ies'}.", added
        return False, "No usable image or text entries were found in that zip.", 0
    except zipfile.BadZipFile:
        return False, "That zip file could not be read.", 0


def process_wheel_import_job(job_id, wheel_id, upload_path):
    status = read_import_job_status(job_id)
    if not status:
        return
    suffix = Path(upload_path).suffix.lower()
    try:
        status["state"] = "processing"
        status["message"] = "Importing entries..."
        write_import_job_status(job_id, status)
        added = 0
        if suffix == ".txt":
            lines = Path(upload_path).read_text(encoding="utf-8", errors="replace").splitlines()
            valid_lines = [line for line in lines if line.strip() and not line.strip().startswith("#")]
            status["total"] = len(valid_lines)
            status["processed"] = 0
            write_import_job_status(job_id, status)
            success, message = add_wheel_text_entries(storage, wheel_id, lines)
            status["processed"] = len(valid_lines)
            status["added"] = len(valid_lines) if success else 0
            status["state"] = "complete" if success else "error"
            status["message"] = message
            write_import_job_status(job_id, status)
            return

        with zipfile.ZipFile(upload_path) as archive:
            members = [member for member in archive.infolist() if not member.is_dir()]
            usable = []
            for member in members:
                lower = Path(member.filename).name.lower()
                if lower.endswith((".png", ".jpg", ".jpeg", ".gif", ".webp", ".bmp", ".tif", ".tiff", ".txt")):
                    usable.append(member)
            status["total"] = len(usable)
            status["processed"] = 0
            status["added"] = 0
            write_import_job_status(job_id, status)
            for member in usable:
                member_name = Path(member.filename).name
                lower = member_name.lower()
                if lower.endswith((".png", ".jpg", ".jpeg", ".gif", ".webp", ".bmp", ".tif", ".tiff")):
                    with archive.open(member) as image_file:
                        content = image_file.read()
                    success, _ = add_wheel_image_entry(storage, wheel_id, member_name, content)
                    if success:
                        added += 1
                elif lower.endswith(".txt"):
                    with archive.open(member) as text_file:
                        lines = text_file.read().decode("utf-8", errors="replace").splitlines()
                    success, _ = add_wheel_text_entries(storage, wheel_id, lines)
                    if success:
                        added += len([line for line in lines if line.strip() and not line.strip().startswith("#")])
                status["processed"] += 1
                status["added"] = added
                status["message"] = f"Processed {status['processed']} of {status['total']} files."
                write_import_job_status(job_id, status)

        if added:
            status["state"] = "complete"
            status["message"] = f"Imported {added} wheel entr{'y' if added == 1 else 'ies'}."
            status["added"] = added
        else:
            status["state"] = "error"
            status["message"] = "No usable image or text entries were found in that zip."
        write_import_job_status(job_id, status)
    except zipfile.BadZipFile:
        status["state"] = "error"
        status["message"] = "That zip file could not be read."
        write_import_job_status(job_id, status)
    except StorageError as error:
        status["state"] = "error"
        status["message"] = str(error) or "Storage is unavailable right now."
        write_import_job_status(job_id, status)
    except Exception as error:  # pragma: no cover
        logger.exception("Wheel import job failed")
        status["state"] = "error"
        status["message"] = f"Import failed: {error}"
        write_import_job_status(job_id, status)
    finally:
        clear_request_caches()
        try:
            Path(upload_path).unlink(missing_ok=True)
        except OSError:
            pass


def start_wheel_import_job(wheel_id, file_storage, user):
    if not file_storage or not file_storage.filename:
        return False, {"message": "Choose a .zip or .txt file."}
    suffix = Path(secure_filename(file_storage.filename)).suffix.lower()
    if suffix not in {".zip", ".txt"}:
        return False, {"message": "Imports must be .zip or .txt."}
    job_id = uuid.uuid4().hex
    upload_path = import_job_upload_path(job_id, suffix)
    file_storage.save(upload_path)
    payload = {
        "job_id": job_id,
        "wheel_id": wheel_id,
        "filename": secure_filename(file_storage.filename),
        "state": "queued",
        "message": "Upload received. Preparing import...",
        "processed": 0,
        "total": 0,
        "added": 0,
        "created_at": int(time.time()),
        "created_by": user["id"] if user else "",
    }
    write_import_job_status(job_id, payload)
    thread = threading.Thread(target=process_wheel_import_job, args=(job_id, wheel_id, str(upload_path)), daemon=True)
    thread.start()
    return True, payload


def get_users_data():
    if has_request_context():
        if not hasattr(g, "users_data_cache"):
            g.users_data_cache = load_users(storage)
        return g.users_data_cache
    return load_users(storage)


def get_tracker_data():
    if has_request_context():
        if not hasattr(g, "tracker_data_cache"):
            g.tracker_data_cache = load_data(storage)
        return g.tracker_data_cache
    return load_data(storage)


def get_storage_settings_data():
    if has_request_context():
        if not hasattr(g, "storage_settings_cache"):
            g.storage_settings_cache = load_storage_settings(DATA_DIR)
        return g.storage_settings_cache
    return load_storage_settings(DATA_DIR)


def get_site_settings_data():
    if has_request_context():
        if not hasattr(g, "site_settings_cache"):
            g.site_settings_cache = load_site_settings(DATA_DIR)
        return g.site_settings_cache
    return load_site_settings(DATA_DIR)


def clear_request_caches():
    if has_request_context():
        for attr in ("users_data_cache", "tracker_data_cache", "storage_settings_cache", "site_settings_cache", "current_user_cache"):
            if hasattr(g, attr):
                delattr(g, attr)


def is_async_request():
    return request.headers.get("X-Requested-With") == "XMLHttpRequest"


def current_user():
    if has_request_context() and hasattr(g, "current_user_cache"):
        return g.current_user_cache
    user_id = session.get("user_id")
    if not user_id:
        if has_request_context():
            g.current_user_cache = None
        return None
    for user in get_users_data()["users"]:
        if user["id"] == user_id and user.get("active", True):
            if has_request_context():
                g.current_user_cache = user
            return user
    if has_request_context():
        g.current_user_cache = None
    return None


def current_user_level():
    return 0


def can_manage_tracker():
    return user_can_create(current_user())


def login_required(view):
    def wrapped(*args, **kwargs):
        if not current_user():
            flash("Please log in first.", "error")
            return redirect(url_for("login"))
        return view(*args, **kwargs)

    wrapped.__name__ = view.__name__
    return wrapped


def roles_required(*roles):
    def decorator(view):
        def wrapped(*args, **kwargs):
            user = current_user()
            if not user or user["role"] not in roles:
                flash("You do not have access to that page.", "error")
                return redirect(url_for("dashboard"))
            return view(*args, **kwargs)

        wrapped.__name__ = view.__name__
        return wrapped

    return decorator


def tracker_write_required(view):
    def wrapped(*args, **kwargs):
        if not can_manage_tracker():
            flash("You do not have creation permission.", "error")
            return redirect(url_for("dashboard"))
        return view(*args, **kwargs)

    wrapped.__name__ = view.__name__
    return wrapped


@app.context_processor
def inject_globals():
    user = current_user()
    storage_settings = get_storage_settings_data()
    site_settings = get_site_settings_data()
    favicon_name = site_settings.get("favicon_name", "").strip()
    return {
        "current_user": user,
        "current_role": user["role"] if user else None,
        "can_manage_tracker": can_manage_tracker(),
        "can_create_records": can_manage_tracker(),
        "can_view_memory": user_can_view_memory(user),
        "can_view_documents": user_can_view_documents(user),
        "can_view_profile": user_can_view_profile(user),
        "can_view_relations": user_can_view_relations(user),
        "can_view_gallery": user_can_view_gallery(user),
        "can_view_locked_gallery": user_can_view_locked_gallery(user),
        "status_options": STATUS_OPTIONS,
        "month_options": MONTH_OPTIONS,
        "gender_options": GENDER_OPTIONS,
        "organ_options": ORGAN_OPTIONS,
        "relationship_style_options": RELATIONSHIP_STYLE_OPTIONS,
        "legacy_value": LEGACY_VALUE,
        "storage_settings": storage_settings,
        "site_settings": site_settings,
        "site_name": site_settings.get("site_name", "United Front Technical Database"),
        "favicon_url": url_for("site_favicon", v=favicon_name) if favicon_name else "",
        "data_dir": str(DATA_DIR),
    }


@app.route("/")
def index():
    return redirect(url_for("dashboard" if current_user() else "login"))


@app.route("/media/<kind>/<entry_id>/<path:filename>")
@login_required
def media_file(kind, entry_id, filename):
    if kind not in {"alter", "location", "affiliation", "wheel"}:
        flash("Unknown media type.", "error")
        return redirect(url_for("dashboard"))
    data = get_tracker_data()
    if kind == "wheel":
        if not user_can_view_wheel(data.get("wheel_records", {}).get(entry_id), current_user()):
            flash("You do not have access to that wheel media.", "error")
            return redirect(url_for("wheels"))
    else:
        if not can_view_gallery_for_entry(data, current_user(), kind, entry_id):
            flash("You do not have permission to view that locked gallery.", "error")
            return redirect(url_for("dashboard"))
    if not entry_is_accessible(data, kind, entry_id, current_user_level()):
        flash("You do not have access to that media.", "error")
        return redirect(url_for("dashboard"))
    media_name = media_storage_name(kind, entry_id, filename)
    if os.getenv("MEDIA_REDIRECTS", "").strip().lower() in {"1", "true", "yes", "on"}:
        download_url = storage.get_download_url(media_name, expires_in=300)
        if download_url:
            return redirect(download_url)
    payload = storage.read_bytes(media_name)
    if payload is None:
        flash("Media file not found.", "error")
        return redirect(url_for("dashboard"))
    return Response(payload, mimetype=Path(filename).suffix and ({".jpg":"image/jpeg",".jpeg":"image/jpeg",".png":"image/png",".gif":"image/gif",".webp":"image/webp"}.get(Path(filename).suffix.lower(), "application/octet-stream")) or "application/octet-stream")


@app.route("/favicon.ico")
def site_favicon():
    settings = get_site_settings_data()
    favicon_name = settings.get("favicon_name", "").strip()
    if not favicon_name:
        return ("", 204)
    path = DATA_DIR / "branding" / favicon_name
    if not path.exists():
        return ("", 204)
    return Response(path.read_bytes(), mimetype=mimetypes.guess_type(path.name)[0] or "image/x-icon")


@app.route("/admin")
@login_required
@roles_required("admin")
def admin_options():
    return render_template("admin_options.html")


@app.route("/admin/branding", methods=["GET", "POST"])
@login_required
@roles_required("admin")
def admin_branding():
    settings = get_site_settings_data()
    if request.method == "POST":
        updated = {
            "site_name": request.form.get("site_name", "").strip() or settings.get("site_name", "United Front Technical Database"),
            "favicon_name": settings.get("favicon_name", ""),
        }
        favicon = request.files.get("favicon_file")
        if favicon and favicon.filename:
            suffix = Path(secure_filename(favicon.filename)).suffix.lower()
            if suffix not in {".ico", ".png", ".jpg", ".jpeg", ".gif", ".webp", ".svg"}:
                flash("Favicon must be ICO, PNG, JPG, GIF, WEBP, or SVG.", "error")
                return redirect(url_for("admin_branding"))
            branding_dir = DATA_DIR / "branding"
            branding_dir.mkdir(parents=True, exist_ok=True)
            if updated["favicon_name"]:
                old_path = branding_dir / updated["favicon_name"]
                if old_path.exists():
                    old_path.unlink()
            filename = f"favicon-{os.urandom(8).hex()}{suffix}"
            (branding_dir / filename).write_bytes(favicon.read())
            updated["favicon_name"] = filename
        save_site_settings(DATA_DIR, updated)
        clear_request_caches()
        flash("Branding updated.", "success")
        return redirect(url_for("admin_branding"))
    return render_template("admin_branding.html", settings=settings)


@app.route("/admin/entries", methods=["GET", "POST"])
@login_required
@roles_required("admin")
def admin_entries():
    if request.method == "POST":
        kind = request.form.get("kind", "")
        entry_id = request.form.get("entry_id", "").strip()
        query = request.form.get("q", "").strip()
        data = get_tracker_data()
        gallery_urls = []
        if kind == "alter":
            gallery_urls = list(data.get("alter_profiles", {}).get(entry_id, {}).get("gallery", []))
        elif kind == "location":
            gallery_urls = list(data.get("location_galleries", {}).get(entry_id, []))
        elif kind == "affiliation":
            gallery_urls = list(data.get("affiliation_records", {}).get(entry_id, {}).get("gallery", []))
        elif kind == "wheel":
            gallery_urls = [item.get("media_url", "") for item in data.get("wheel_records", {}).get(entry_id, {}).get("entries", []) if item.get("kind") == "image"]
        success, message = delete_entry(storage, kind, entry_id)
        if success:
            for image_url in gallery_urls:
                delete_gallery_upload(image_url)
        clear_request_caches()
        flash(message, "success" if success else "error")
        return redirect(url_for("admin_entries", kind=kind or "alter", q=query))

    kind = request.args.get("kind", "alter")
    query = request.args.get("q", "").strip()
    results = search_entries(get_tracker_data(), kind, query, 4)
    return render_template("admin_entries.html", kind=kind, query=query, results=results)


@app.route("/register", methods=["GET", "POST"])
def register():
    users_data = get_users_data()
    if request.method == "POST":
        username = request.form.get("username", "").strip()
        password = request.form.get("password", "")
        if not username or not password:
            flash("Username and password are required.", "error")
            return redirect(url_for("register"))
        if any(user["username"].lower() == username.lower() for user in users_data["users"]):
            flash("That username already exists.", "error")
            return redirect(url_for("register"))

        role = "admin" if not users_data["users"] else "user"
        user = {
            "id": os.urandom(12).hex(),
            "username": username,
            "password_hash": generate_password_hash(password),
            "role": role,
            "creation_permission": role == "admin",
            "memory_tree_permission": role == "admin",
            "profile_permission": role == "admin",
            "relation_permission": role == "admin",
            "document_permission": role == "admin",
            "locked_gallery_permission": role == "admin",
            "active": True,
        }
        users_data["users"].append(user)
        save_users(storage, users_data)
        clear_request_caches()
        session["user_id"] = user["id"]
        flash(f"Account created. Logged in as {role}.", "success")
        return redirect(url_for("dashboard"))
    return render_template("register.html")


@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        username = request.form.get("username", "").strip()
        password = request.form.get("password", "")
        for user in get_users_data()["users"]:
            if user["username"].lower() == username.lower() and user.get("active", True):
                if check_password_hash(user["password_hash"], password):
                    session["user_id"] = user["id"]
                    flash("Logged in successfully.", "success")
                    return redirect(url_for("dashboard"))
        flash("Invalid username or password.", "error")
        return redirect(url_for("login"))
    return render_template("login.html")


@app.route("/logout")
@login_required
def logout():
    session.clear()
    flash("Logged out.", "success")
    return redirect(url_for("login"))


@app.route("/dashboard")
@login_required
def dashboard():
    data = get_tracker_data()
    context = build_dashboard_context(data, current_user_level())
    return render_template(
        "dashboard.html",
        counts={
            "alters": len(context["alters"]),
            "locations": len(context["locations"]),
            "affiliations": len(context["affiliations"]),
            "documents": len(context["documents"]),
            "relations": len(context["relations"]),
            "tags": len(context["tags"]),
        },
        context=context,
        alter_prefixes=get_alter_prefixes(data),
        affiliation_prefixes=get_affiliation_prefixes(data),
    )


@app.route("/generate-id/<kind>", methods=["POST"])
@login_required
@tracker_write_required
def generate_id(kind):
    data = get_tracker_data()
    prefix = request.form.get("prefix", "")
    if kind == "alter":
        if prefix and prefix not in get_alter_prefixes(data):
            if is_async_request():
                return jsonify({"ok": False, "message": "Unknown alter prefix.", "category": "error"}), 400
            flash("Unknown alter prefix.", "error")
            return redirect(url_for("dashboard"))
    elif kind == "location":
        prefix = LOCATION_PREFIX if prefix != "" else ""
    elif kind == "affiliation":
        if prefix and prefix not in get_affiliation_prefixes(data):
            if is_async_request():
                return jsonify({"ok": False, "message": "Unknown affiliation prefix.", "category": "error"}), 400
            flash("Unknown affiliation prefix.", "error")
            return redirect(url_for("dashboard"))
    elif kind == "document":
        prefix = DOCUMENT_PREFIX if prefix != "" else ""
    elif kind == "wheel":
        prefix = WHEEL_PREFIX if prefix != "" else ""
    else:
        if is_async_request():
            return jsonify({"ok": False, "message": "Unknown ID type.", "category": "error"}), 400
        flash("Unknown ID type.", "error")
        return redirect(url_for("dashboard"))
    generated_id = generate_unique_hash(storage, prefix)
    if is_async_request():
        return jsonify({"ok": True, "message": f"Generated ID: {generated_id}", "category": "success", "generated_id": generated_id})
    flash(f"Generated ID: {generated_id}", "success")
    return redirect(url_for("dashboard"))


@app.route("/create/<kind>", methods=["POST"])
@login_required
@tracker_write_required
def create_record(kind):
    data = get_tracker_data()
    name = request.form.get("name", "")
    entry_id = request.form.get("entry_id", "").strip()
    if kind == "alter" and not entry_id:
        entry_id = generate_unique_hash(storage, data["alter_prefixes"][0])
    elif kind == "location" and not entry_id:
        entry_id = generate_unique_hash(storage, LOCATION_PREFIX)
    elif kind == "affiliation" and not entry_id:
        entry_id = generate_unique_hash(storage, data["affiliation_prefixes"][0] if data["affiliation_prefixes"] else "")
    elif kind == "document" and not entry_id:
        entry_id = generate_unique_hash(storage, DOCUMENT_PREFIX)
    elif kind == "wheel" and not entry_id:
        entry_id = generate_unique_hash(storage, WHEEL_PREFIX)
    bucket_map = {"alter": ("alters", "alter"), "location": ("locations", "location"), "affiliation": ("affiliations", "affiliation"), "document": ("documents", "document"), "wheel": ("wheels", "wheel")}
    success, message = create_entry_with_level(storage, bucket_map[kind][0], bucket_map[kind][1], name, entry_id, None)
    if success and kind == "document":
        save_document_record(storage, entry_id, request.form)
    flash(message, "success" if success else "error")
    return redirect(url_for("dashboard"))


@app.route("/prefix/<kind>", methods=["POST"])
@login_required
@tracker_write_required
def add_prefix(kind):
    if kind == "alter":
        success, message = create_alter_prefix(storage, request.form.get("prefix", ""))
    else:
        success, message = create_affiliation_prefix(storage, request.form.get("prefix", ""))
    flash(message, "success" if success else "error")
    return redirect(url_for("dashboard"))


@app.route("/prefix/<kind>/delete", methods=["POST"])
@login_required
@roles_required("admin")
def remove_prefix(kind):
    prefix = request.form.get("prefix", "")
    if kind == "alter":
        success, message = delete_alter_prefix(storage, prefix)
    else:
        success, message = delete_affiliation_prefix(storage, prefix)
    flash(message, "success" if success else "error")
    return redirect(url_for("dashboard"))


@app.route("/tags/standard", methods=["POST"])
@login_required
@tracker_write_required
def add_standard_tag():
    success, message = create_relation_tag(storage, request.form.get("tag_name", ""))
    flash(message, "success" if success else "error")
    return redirect(url_for("dashboard"))


@app.route("/tags/special", methods=["POST"])
@login_required
@tracker_write_required
def add_special_tag():
    success, message = create_special_relation_tag(storage, request.form.get("forward_tag", ""), request.form.get("reverse_tag", ""))
    flash(message, "success" if success else "error")
    return redirect(url_for("dashboard"))


@app.route("/search")
@login_required
def search():
    query = request.args.get("q", "")
    data = get_tracker_data()
    grouped_results = {
        "alters": search_entries(data, "alter", query, current_user_level()),
        "locations": search_entries(data, "location", query, current_user_level()),
        "affiliations": search_entries(data, "affiliation", query, current_user_level()),
        "documents": search_entries(data, "document", query, current_user_level()),
    }
    total_results = sum(len(items) for items in grouped_results.values())
    return render_template("search_results.html", query=query, grouped_results=grouped_results, total_results=total_results)


@app.route("/wheels")
@login_required
def wheels():
    data = get_tracker_data()
    return render_template("wheels.html", wheels=build_wheels_context(data, current_user()))


@app.route("/wheels/create", methods=["POST"])
@login_required
@roles_required("admin")
def create_wheel_route():
    name = request.form.get("name", "")
    entry_id = request.form.get("entry_id", "").strip() or generate_unique_hash(storage, WHEEL_PREFIX)
    success, message = create_entry_with_level(storage, "wheels", "wheel", name, entry_id, None)
    flash(message, "success" if success else "error")
    return redirect(url_for("wheels"))


@app.route("/wheel/<wheel_id>", methods=["GET", "POST"])
@login_required
def wheel_detail(wheel_id):
    data = get_tracker_data()
    view = build_wheel_view(data, wheel_id, current_user(), get_users_data())
    if not view:
        flash("Unknown or inaccessible wheel.", "error")
        return redirect(url_for("wheels"))
    return render_template("wheel_detail.html", view=view, spin_result=None)


@app.route("/wheel/<wheel_id>/spin", methods=["POST"])
@login_required
def spin_wheel_route(wheel_id):
    data = get_tracker_data()
    record = data.get("wheel_records", {}).get(wheel_id)
    if not user_can_view_wheel(record, current_user()):
        flash("You do not have access to that wheel.", "error")
        return redirect(url_for("wheels"))
    success, message, result = spin_wheel(storage, wheel_id)
    clear_request_caches()
    data = get_tracker_data()
    view = build_wheel_view(data, wheel_id, current_user(), get_users_data())
    if not success or not view:
        flash(message, "error")
        return redirect(url_for("wheels"))
    return render_template("wheel_detail.html", view=view, spin_result=result)


@app.route("/wheel/<wheel_id>/settings", methods=["POST"])
@login_required
@roles_required("admin")
def update_wheel_settings(wheel_id):
    if not entry_is_accessible(get_tracker_data(), "wheel", wheel_id, current_user_level()):
        flash("Unknown wheel.", "error")
        return redirect(url_for("wheels"))
    repeat_exceptions = [item.strip() for item in request.form.get("repeat_exceptions", "").split(",") if item.strip()]
    success, message = save_wheel_settings(
        storage,
        wheel_id,
        {
            "entry_deletion": request.form.get("entry_deletion", "1"),
            "stop_repeat_entry": "1" if request.form.get("stop_repeat_entry") == "on" else "0",
            "repeat_exceptions": repeat_exceptions,
        },
    )
    flash(message, "success" if success else "error")
    clear_request_caches()
    return redirect(url_for("wheel_detail", wheel_id=wheel_id))


@app.route("/wheel/<wheel_id>/permissions", methods=["POST"])
@login_required
@roles_required("admin")
def update_wheel_permissions_route(wheel_id):
    if not entry_is_accessible(get_tracker_data(), "wheel", wheel_id, current_user_level()):
        flash("Unknown wheel.", "error")
        return redirect(url_for("wheels"))
    success, message = save_wheel_permissions(storage, wheel_id, request.form.getlist("view_user_ids"), request.form.getlist("edit_user_ids"))
    flash(message, "success" if success else "error")
    clear_request_caches()
    return redirect(url_for("wheel_detail", wheel_id=wheel_id))


@app.route("/wheel/<wheel_id>/entries/text", methods=["POST"])
@login_required
def add_wheel_text_entries_route(wheel_id):
    data = get_tracker_data()
    record = data.get("wheel_records", {}).get(wheel_id)
    if not user_can_edit_wheel(record, current_user()):
        flash("You do not have edit access to that wheel.", "error")
        return redirect(url_for("wheels"))
    success, message = add_wheel_text_entries(storage, wheel_id, request.form.get("entries_text", "").splitlines())
    flash(message, "success" if success else "error")
    clear_request_caches()
    return redirect(url_for("wheel_detail", wheel_id=wheel_id))


@app.route("/wheel/<wheel_id>/entries/import", methods=["POST"])
@login_required
def import_wheel_entries_route(wheel_id):
    data = get_tracker_data()
    record = data.get("wheel_records", {}).get(wheel_id)
    if not user_can_edit_wheel(record, current_user()):
        if is_async_request():
            return jsonify({"ok": False, "message": "You do not have edit access to that wheel.", "category": "error"}), 403
        flash("You do not have edit access to that wheel.", "error")
        return redirect(url_for("wheels"))
    success, payload = start_wheel_import_job(wheel_id, request.files.get("bundle"), current_user())
    if is_async_request():
        if success:
            return jsonify(
                {
                    "ok": True,
                    "message": "Import started.",
                    "category": "success",
                    "job_id": payload["job_id"],
                    "status_url": url_for("wheel_import_status_route", wheel_id=wheel_id, job_id=payload["job_id"]),
                }
            )
        return jsonify({"ok": False, "message": payload["message"], "category": "error"}), 400
    flash(payload["message"] if not success else "Import started.", "success" if success else "error")
    return redirect(url_for("wheel_detail", wheel_id=wheel_id))


@app.route("/wheel/<wheel_id>/import-status/<job_id>")
@login_required
def wheel_import_status_route(wheel_id, job_id):
    data = get_tracker_data()
    record = data.get("wheel_records", {}).get(wheel_id)
    if not user_can_edit_wheel(record, current_user()):
        return jsonify({"ok": False, "message": "You do not have edit access to that wheel.", "category": "error"}), 403
    payload = read_import_job_status(job_id)
    if not payload or payload.get("wheel_id") != wheel_id:
        return jsonify({"ok": False, "message": "That import job was not found.", "category": "error"}), 404
    if payload.get("created_by") and payload.get("created_by") != current_user()["id"] and current_user()["role"] != "admin":
        return jsonify({"ok": False, "message": "You do not have access to that import job.", "category": "error"}), 403
    if payload.get("state") in {"complete", "error"}:
        clear_request_caches()
    return jsonify({"ok": True, "job": payload})


@app.route("/wheel/<wheel_id>/entries/<entry_id>/delete", methods=["POST"])
@login_required
def delete_wheel_entry_route(wheel_id, entry_id):
    data = get_tracker_data()
    record = data.get("wheel_records", {}).get(wheel_id)
    if not user_can_edit_wheel(record, current_user()):
        flash("You do not have edit access to that wheel.", "error")
        return redirect(url_for("wheels"))
    success, payload = remove_wheel_entry(storage, wheel_id, entry_id)
    if success:
        if payload.get("kind") == "image":
            delete_wheel_media(payload)
        flash("Removed wheel entry.", "success")
    else:
        flash(payload, "error")
    clear_request_caches()
    return redirect(url_for("wheel_detail", wheel_id=wheel_id))


@app.route("/wheel/<wheel_id>/cache-clear", methods=["POST"])
@login_required
def clear_wheel_cache_route(wheel_id):
    data = get_tracker_data()
    record = data.get("wheel_records", {}).get(wheel_id)
    if not user_can_edit_wheel(record, current_user()):
        flash("You do not have edit access to that wheel.", "error")
        return redirect(url_for("wheels"))
    success, message = clear_wheel_used_entries(storage, wheel_id)
    flash(message, "success" if success else "error")
    clear_request_caches()
    return redirect(url_for("wheel_detail", wheel_id=wheel_id))


@app.route("/alter/<alter_id>")
@login_required
def alter_detail(alter_id):
    data = get_tracker_data()
    view = build_alter_view(data, alter_id, current_user_level())
    if not view:
        flash("Unknown or inaccessible alter.", "error")
        return redirect(url_for("dashboard"))
    return render_template(
        "alter_detail.html",
        view=view,
        can_view_memory=user_can_view_memory(current_user()),
        can_view_profile=can_view_profile_for_entry(data, current_user(), alter_id),
        can_view_relations=can_view_relations_for_entry(data, current_user(), alter_id),
        can_view_gallery=can_view_gallery_for_entry(data, current_user(), "alter", alter_id),
    )


@app.route("/alter/<alter_id>/profile", methods=["POST"])
@login_required
@tracker_write_required
def save_alter(alter_id):
    data = get_tracker_data()
    user_level = current_user_level()
    if not entry_is_accessible(data, "alter", alter_id, user_level):
        flash("You do not have access to that alter.", "error")
        return redirect(url_for("dashboard"))
    if not can_view_profile_for_entry(data, current_user(), alter_id):
        flash("You do not have permission to view that locked profile.", "error")
        return redirect(url_for("alter_detail", alter_id=alter_id))
    success, message = rename_entry(storage, "alters", "alter", alter_id, request.form.get("name", ""))
    if success:
        success, message = save_alter_profile(storage, alter_id, request.form)
    location_id = resolve_entry_reference(data, "location", request.form.get("location_id", ""), user_level)
    if success and location_id and entry_is_accessible(data, "location", location_id, user_level):
        success, message = bind_location(storage, alter_id, location_id)
    elif success and location_id:
        success, message = False, "You do not have access to that location."
    flash(message, "success" if success else "error")
    clear_request_caches()
    return redirect(url_for("alter_detail", alter_id=alter_id))


@app.route("/alter/<alter_id>/affiliations", methods=["POST"])
@login_required
@tracker_write_required
def update_alter_affiliations(alter_id):
    data = get_tracker_data()
    user_level = current_user_level()
    if not entry_is_accessible(data, "alter", alter_id, user_level):
        flash("You do not have access to that alter.", "error")
        return redirect(url_for("dashboard"))
    affiliation_id = resolve_entry_reference(data, "affiliation", request.form.get("affiliation_id", ""), user_level)
    status = request.form.get("status", "Current")
    if affiliation_id and not entry_is_accessible(data, "affiliation", affiliation_id, user_level):
        flash("You do not have access to that affiliation.", "error")
        return redirect(url_for("alter_detail", alter_id=alter_id))
    if request.form.get("action") == "remove":
        success, message = remove_affiliation_membership(storage, alter_id, affiliation_id)
    else:
        success, message = update_affiliation_membership(storage, alter_id, affiliation_id, status)
    flash(message, "success" if success else "error")
    clear_request_caches()
    return redirect(url_for("alter_detail", alter_id=alter_id))


@app.route("/alter/<alter_id>/occupations", methods=["POST"])
@login_required
@tracker_write_required
def update_alter_occupations(alter_id):
    if not entry_is_accessible(get_tracker_data(), "alter", alter_id, current_user_level()):
        flash("You do not have access to that alter.", "error")
        return redirect(url_for("dashboard"))
    if request.form.get("action") == "remove":
        success, message = remove_occupation_entry(storage, alter_id, request.form.get("occupation", ""))
    else:
        success, message = update_occupation_entry(storage, alter_id, request.form.get("occupation", ""), request.form.get("status", "Current"))
    flash(message, "success" if success else "error")
    clear_request_caches()
    return redirect(url_for("alter_detail", alter_id=alter_id))


@app.route("/alter/<alter_id>/relation", methods=["POST"])
@login_required
@tracker_write_required
def update_relation(alter_id):
    data = get_tracker_data()
    user_level = current_user_level()
    if not can_view_relations_for_entry(data, current_user(), alter_id):
        flash("You do not have permission to view those locked relations.", "error")
        return redirect(url_for("alter_detail", alter_id=alter_id))
    other_id = resolve_entry_reference(data, "alter", request.form.get("other_id", ""), user_level)
    if not entry_is_accessible(data, "alter", alter_id, user_level) or (other_id and not entry_is_accessible(data, "alter", other_id, user_level)):
        flash("You do not have access to one or more alters in that relation.", "error")
        return redirect(url_for("dashboard"))
    if request.form.get("action") == "remove":
        success, message = remove_relation(storage, alter_id, request.form.get("tag", ""), other_id)
    else:
        success, message = set_relation_tag(storage, alter_id, request.form.get("tag", ""), other_id)
    flash(message, "success" if success else "error")
    clear_request_caches()
    return redirect(url_for("alter_detail", alter_id=alter_id))


@app.route("/alter/<alter_id>/bulk", methods=["POST"])
@login_required
@tracker_write_required
def bulk_relations(alter_id):
    data = get_tracker_data()
    user_level = current_user_level()
    if not can_view_relations_for_entry(data, current_user(), alter_id):
        flash("You do not have permission to view those locked relations.", "error")
        return redirect(url_for("alter_detail", alter_id=alter_id))
    if not entry_is_accessible(data, "alter", alter_id, user_level):
        flash("You do not have access to that alter.", "error")
        return redirect(url_for("dashboard"))
    selected_ids = request.form.getlist("selected_ids")
    messages = []
    for other_id in selected_ids:
        if not entry_is_accessible(data, "alter", other_id, user_level):
            messages.append(f"Skipped inaccessible alter {other_id}.")
            continue
        if request.form.get("bulk_action") == "remove":
            success, message = remove_relation(storage, alter_id, "", other_id)
        else:
            success, message = set_relation_tag(storage, alter_id, request.form.get(f"tag_{other_id}", ""), other_id)
        messages.append(message)
    flash(" | ".join(messages) if messages else "No alters selected.", "success" if messages else "error")
    clear_request_caches()
    return redirect(url_for("alter_detail", alter_id=alter_id))


@app.route("/alter/<alter_id>/memory", methods=["POST"])
@login_required
@tracker_write_required
def update_alter_memory(alter_id):
    if not user_can_view_memory(current_user()):
        flash("You do not have permission to view Memory Trees.", "error")
        return redirect(url_for("alter_detail", alter_id=alter_id))
    if not entry_is_accessible(get_tracker_data(), "alter", alter_id, current_user_level()):
        flash("You do not have access to that alter.", "error")
        return redirect(url_for("dashboard"))
    era = request.form.get("era", "")
    entry_id = request.form.get("entry_id", "").strip()
    if request.form.get("action") == "remove":
        success, message = remove_memory_entry(storage, alter_id, era, entry_id)
    else:
        success, message = update_memory_tree(
            storage,
            alter_id,
            era,
            request.form.get("memory_date", ""),
            request.form.get("memory_text", ""),
            request.form.getlist("ordered_ids"),
        )
    flash(message, "success" if success else "error")
    clear_request_caches()
    return redirect(url_for("alter_detail", alter_id=alter_id))


@app.route("/alter/<alter_id>/notes", methods=["POST"])
@login_required
@tracker_write_required
def update_alter_notes(alter_id):
    if not entry_is_accessible(get_tracker_data(), "alter", alter_id, current_user_level()):
        flash("You do not have access to that alter.", "error")
        return redirect(url_for("dashboard"))
    entry_id = request.form.get("entry_id", "").strip()
    if request.form.get("action") == "remove":
        success, message = remove_note_entry(storage, alter_id, entry_id)
    else:
        success, message = update_notes(
            storage,
            alter_id,
            request.form.get("note_text", ""),
            request.form.getlist("ordered_note_ids"),
        )
    flash(message, "success" if success else "error")
    clear_request_caches()
    return redirect(url_for("alter_detail", alter_id=alter_id))


@app.route("/location/<location_id>")
@login_required
def location_detail(location_id):
    data = get_tracker_data()
    view = build_location_view(data, location_id, current_user_level())
    if not view:
        flash("Unknown or inaccessible location.", "error")
        return redirect(url_for("dashboard"))
    return render_template(
        "location_detail.html",
        view=view,
        can_view_gallery=can_view_gallery_for_entry(data, current_user(), "location", location_id),
    )


@app.route("/gallery/<kind>/<entry_id>", methods=["POST"])
@login_required
@tracker_write_required
def update_gallery(kind, entry_id):
    if kind not in {"alter", "location", "affiliation"}:
        flash("Unsupported gallery type.", "error")
        return redirect(url_for("dashboard"))
    if not can_view_gallery_for_entry(get_tracker_data(), current_user(), kind, entry_id):
        flash("You do not have permission to view that locked gallery.", "error")
        target = "alter_detail" if kind == "alter" else "location_detail" if kind == "location" else "affiliation_detail"
        arg_name = "alter_id" if kind == "alter" else "location_id" if kind == "location" else "affiliation_id"
        return redirect(url_for(target, **{arg_name: entry_id}))
    if not entry_is_accessible(get_tracker_data(), kind, entry_id, current_user_level()):
        flash("You do not have access to that entry.", "error")
        return redirect(url_for("dashboard"))
    image_url = request.form.get("image_url", "").strip()
    uploaded_file = request.files.get("image_file")
    action = request.form.get("action", "add")
    if action == "remove":
        success, message = remove_gallery_item(storage, kind, entry_id, image_url)
        if success:
            delete_gallery_upload(image_url)
    else:
        uploaded_image_url = ""
        if uploaded_file and uploaded_file.filename:
            try:
                uploaded_image_url = save_gallery_upload(kind, entry_id, uploaded_file)
                image_url = uploaded_image_url
            except ValueError as error:
                flash(str(error), "error")
                target = "alter_detail" if kind == "alter" else "location_detail" if kind == "location" else "affiliation_detail"
                arg_name = "alter_id" if kind == "alter" else "location_id" if kind == "location" else "affiliation_id"
                return redirect(url_for(target, **{arg_name: entry_id}))
        if image_url and not is_managed_media_url(image_url):
            success, managed_image_url = import_gallery_media_from_url(storage, kind, entry_id, image_url)
            if success:
                image_url = managed_image_url
            else:
                flash(managed_image_url, "error")
                target = "alter_detail" if kind == "alter" else "location_detail" if kind == "location" else "affiliation_detail"
                arg_name = "alter_id" if kind == "alter" else "location_id" if kind == "location" else "affiliation_id"
                return redirect(url_for(target, **{arg_name: entry_id}))
        success, message = add_gallery_item(storage, kind, entry_id, image_url)
        if not success and uploaded_image_url:
            delete_gallery_upload(uploaded_image_url)
    flash(message, "success" if success else "error")
    clear_request_caches()
    target = "alter_detail" if kind == "alter" else "location_detail" if kind == "location" else "affiliation_detail"
    arg_name = "alter_id" if kind == "alter" else "location_id" if kind == "location" else "affiliation_id"
    return redirect(url_for(target, **{arg_name: entry_id}))


@app.route("/gallery-lock/<kind>/<entry_id>", methods=["POST"])
@login_required
@tracker_write_required
def update_gallery_lock(kind, entry_id):
    if kind not in {"alter", "location", "affiliation"}:
        flash("Unsupported gallery type.", "error")
        return redirect(url_for("dashboard"))
    if not entry_is_accessible(get_tracker_data(), kind, entry_id, current_user_level()):
        flash("You do not have access to that entry.", "error")
        return redirect(url_for("dashboard"))
    success, message = set_gallery_locked(storage, kind, entry_id, request.form.get("gallery_locked") == "on")
    flash(message, "success" if success else "error")
    clear_request_caches()
    target = "alter_detail" if kind == "alter" else "location_detail" if kind == "location" else "affiliation_detail"
    arg_name = "alter_id" if kind == "alter" else "location_id" if kind == "location" else "affiliation_id"
    return redirect(url_for(target, **{arg_name: entry_id}))


@app.route("/alter-lock/<section>/<alter_id>", methods=["POST"])
@login_required
@tracker_write_required
def update_alter_lock(section, alter_id):
    data = get_tracker_data()
    if not entry_is_accessible(data, "alter", alter_id, current_user_level()):
        flash("You do not have access to that alter.", "error")
        return redirect(url_for("dashboard"))
    if section not in {"profile", "relations"}:
        flash("Unsupported lock type.", "error")
        return redirect(url_for("alter_detail", alter_id=alter_id))
    field_name = f"{section}_locked"
    success, message = set_alter_section_lock(storage, alter_id, section, request.form.get(field_name) == "on")
    flash(message, "success" if success else "error")
    clear_request_caches()
    return redirect(url_for("alter_detail", alter_id=alter_id))


@app.route("/affiliation/<affiliation_id>")
@login_required
def affiliation_detail(affiliation_id):
    data = get_tracker_data()
    view = build_affiliation_view(data, affiliation_id, current_user_level())
    if not view:
        flash("Unknown or inaccessible affiliation.", "error")
        return redirect(url_for("dashboard"))
    return render_template(
        "affiliation_detail.html",
        view=view,
        can_view_memory=user_can_view_memory(current_user()),
        can_view_gallery=can_view_gallery_for_entry(data, current_user(), "affiliation", affiliation_id),
    )


@app.route("/affiliation/<affiliation_id>/summary", methods=["POST"])
@login_required
@tracker_write_required
def update_affiliation_summary(affiliation_id):
    if not entry_is_accessible(get_tracker_data(), "affiliation", affiliation_id, current_user_level()):
        flash("You do not have access to that affiliation.", "error")
        return redirect(url_for("dashboard"))
    success, message = save_affiliation_summary(storage, affiliation_id, request.form.get("summary", ""))
    flash(message, "success" if success else "error")
    clear_request_caches()
    return redirect(url_for("affiliation_detail", affiliation_id=affiliation_id))


@app.route("/affiliation/<affiliation_id>/timeline", methods=["POST"])
@login_required
@tracker_write_required
def update_affiliation_timeline_route(affiliation_id):
    if not user_can_view_memory(current_user()):
        flash("You do not have permission to view timelines.", "error")
        return redirect(url_for("affiliation_detail", affiliation_id=affiliation_id))
    if not entry_is_accessible(get_tracker_data(), "affiliation", affiliation_id, current_user_level()):
        flash("You do not have access to that affiliation.", "error")
        return redirect(url_for("dashboard"))
    entry_id = request.form.get("entry_id", "").strip()
    if request.form.get("action") == "remove":
        success, message = remove_affiliation_timeline_entry(storage, affiliation_id, entry_id)
    else:
        success, message = update_affiliation_timeline(
            storage,
            affiliation_id,
            request.form.get("timeline_date", ""),
            request.form.get("timeline_text", ""),
            request.form.getlist("ordered_ids"),
        )
    flash(message, "success" if success else "error")
    clear_request_caches()
    return redirect(url_for("affiliation_detail", affiliation_id=affiliation_id))


@app.route("/document/<document_id>")
@login_required
def document_detail(document_id):
    data = get_tracker_data()
    view = build_document_view(data, document_id, current_user_level())
    if not view:
        flash("Unknown or inaccessible document.", "error")
        return redirect(url_for("dashboard"))
    return render_template(
        "document_detail.html",
        view=view,
        can_view_document=can_view_document_for_entry(data, current_user(), document_id),
        rendered_content=render_document_content(view["record"]["format"], view["record"]["content"]),
    )


@app.route("/document/<document_id>/save", methods=["POST"])
@login_required
@tracker_write_required
def save_document(document_id):
    data = get_tracker_data()
    if not entry_is_accessible(data, "document", document_id, current_user_level()):
        flash("You do not have access to that document.", "error")
        return redirect(url_for("dashboard"))
    if not can_view_document_for_entry(data, current_user(), document_id):
        flash("You do not have permission to view that locked document.", "error")
        return redirect(url_for("document_detail", document_id=document_id))
    success, message = rename_entry(storage, "documents", "document", document_id, request.form.get("name", ""))
    if success:
        success, message = save_document_record(storage, document_id, request.form)
    flash(message, "success" if success else "error")
    clear_request_caches()
    return redirect(url_for("document_detail", document_id=document_id))


@app.route("/document-lock/<document_id>", methods=["POST"])
@login_required
@tracker_write_required
def update_document_lock(document_id):
    data = get_tracker_data()
    if not entry_is_accessible(data, "document", document_id, current_user_level()):
        flash("You do not have access to that document.", "error")
        return redirect(url_for("dashboard"))
    success, message = set_document_locked(storage, document_id, request.form.get("document_locked") == "on")
    flash(message, "success" if success else "error")
    clear_request_caches()
    return redirect(url_for("document_detail", document_id=document_id))


@app.route("/rename/<kind>/<entry_id>", methods=["POST"])
@login_required
@tracker_write_required
def rename_record(kind, entry_id):
    bucket_map = {"alter": ("alters", "alter"), "location": ("locations", "location"), "affiliation": ("affiliations", "affiliation")}
    if not entry_is_accessible(get_tracker_data(), kind, entry_id, current_user_level()):
        flash("You do not have access to that entry.", "error")
        return redirect(url_for("dashboard"))
    success, message = rename_entry(storage, bucket_map[kind][0], bucket_map[kind][1], entry_id, request.form.get("name", ""))
    flash(message, "success" if success else "error")
    clear_request_caches()
    return redirect(request.referrer or url_for("dashboard"))


@app.route("/admin/import", methods=["GET", "POST"])
@login_required
@roles_required("admin")
def admin_import():
    if request.method == "POST":
        target = request.form.get("target", "")
        file = request.files.get("file")
        target_map = {"tracker": DATA_FILE, "hashes": HASH_FILE, "users": USER_FILE}
        if not file or target not in target_map:
            flash("Choose a valid target and JSON file.", "error")
            return redirect(url_for("admin_import"))
        success, message = save_uploaded_json(storage, target_map[target], file.read())
        flash(message, "success" if success else "error")
        clear_request_caches()
        return redirect(url_for("admin_import"))
    return render_template("admin_import.html")


@app.route("/admin/users", methods=["GET", "POST"])
@login_required
@roles_required("admin")
def admin_users():
    users_data = get_users_data()
    if request.method == "POST":
        target_id = request.form.get("user_id", "")
        for user in users_data["users"]:
            if user["id"] != target_id:
                continue
            action = request.form.get("action")
            if action == "role" and user.get("role") != "admin":
                role = request.form.get("role", "user").strip().lower()
                user["role"] = role if role in {"user", "mod"} else "user"
            elif action == "password":
                new_password = request.form.get("new_password", "")
                if new_password:
                    user["password_hash"] = generate_password_hash(new_password)
            elif action == "creation" and user.get("role") != "admin":
                user["creation_permission"] = request.form.get("creation_permission") == "on"
            elif action == "memory" and user.get("role") != "admin":
                user["memory_tree_permission"] = request.form.get("memory_tree_permission") == "on"
            elif action == "profile" and user.get("role") != "admin":
                user["profile_permission"] = request.form.get("profile_permission") == "on"
            elif action == "relations" and user.get("role") != "admin":
                user["relation_permission"] = request.form.get("relation_permission") == "on"
            elif action == "document" and user.get("role") != "admin":
                user["document_permission"] = request.form.get("document_permission") == "on"
            elif action == "locked_gallery" and user.get("role") != "admin":
                user["locked_gallery_permission"] = request.form.get("locked_gallery_permission") == "on"
            elif action == "active":
                user["active"] = request.form.get("active") == "on"
        save_users(storage, users_data)
        clear_request_caches()
        flash("User updated.", "success")
        return redirect(url_for("admin_users"))
    return render_template("admin_users.html", users=users_data["users"])


@app.route("/admin/storage", methods=["GET", "POST"])
@login_required
@roles_required("admin")
def admin_storage():
    current_settings = get_storage_settings_data()
    if request.method == "POST":
        action = request.form.get("action", "save")
        access_key = request.form.get("s3_access_key", "").strip()
        secret_key = request.form.get("s3_secret_key", "").strip()
        settings = {
            "backend": request.form.get("backend", "local").strip().lower(),
            "s3_endpoint": request.form.get("s3_endpoint", "").strip(),
            "s3_bucket": request.form.get("s3_bucket", "").strip(),
            "s3_region": request.form.get("s3_region", "auto").strip() or "auto",
            "s3_access_key": access_key or current_settings.get("s3_access_key", ""),
            "s3_secret_key": secret_key or current_settings.get("s3_secret_key", ""),
            "s3_prefix": request.form.get("s3_prefix", "").strip(),
            "s3_path_style": request.form.get("s3_path_style") == "on",
        }
        if settings["backend"] not in {"local", "s3"}:
            flash("Storage backend must be local or s3.", "error")
            return redirect(url_for("admin_storage"))
        if settings["backend"] == "s3" and not settings["s3_bucket"]:
            flash("S3 bucket is required when using S3 storage.", "error")
            return redirect(url_for("admin_storage"))
        if action == "save":
            save_storage_settings(DATA_DIR, settings)
            try:
                refresh_storage()
            except RuntimeError as error:
                flash(str(error), "error")
                return redirect(url_for("admin_storage"))
            clear_request_caches()
            flash("Storage settings saved.", "success")
            return redirect(url_for("admin_storage"))
        if action == "migrate":
            save_storage_settings(DATA_DIR, settings)
            source_storage = LocalStorage(DATA_DIR)
            try:
                destination_storage = get_storage(DATA_DIR)
                migrate_storage_data(source_storage, destination_storage)
                refresh_storage()
            except RuntimeError as error:
                flash(str(error), "error")
                return redirect(url_for("admin_storage"))
            clear_request_caches()
            flash("Data migrated to the configured storage backend.", "success")
            return redirect(url_for("admin_storage"))
    settings = get_storage_settings_data()
    return render_template(
        "admin_storage.html",
        backend=settings.get("backend", "local"),
        endpoint=settings.get("s3_endpoint", ""),
        bucket=settings.get("s3_bucket", ""),
        region=settings.get("s3_region", "auto"),
        prefix=settings.get("s3_prefix", ""),
        path_style=settings.get("s3_path_style", False),
        has_access_key=bool(settings.get("s3_access_key")),
        has_secret_key=bool(settings.get("s3_secret_key")),
        config_file=STORAGE_SETTINGS_FILE,
        data_dir=str(DATA_DIR),
    )


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "8000")))
