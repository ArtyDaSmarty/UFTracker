import os
import logging
from pathlib import Path

from flask import Flask, Response, flash, g, has_request_context, jsonify, redirect, render_template, request, session, url_for
from werkzeug.security import check_password_hash, generate_password_hash
from werkzeug.utils import secure_filename

from tracker_core import (
    DATA_FILE,
    ENTRY_LEVEL_OPTIONS,
    GENDER_OPTIONS,
    HASH_FILE,
    LEGACY_VALUE,
    LOCATION_PREFIX,
    MONTH_OPTIONS,
    ORGAN_OPTIONS,
    RELATIONSHIP_STYLE_OPTIONS,
    STATUS_OPTIONS,
    StorageError,
    STORAGE_SETTINGS_FILE,
    USER_FILE,
    USER_LEVEL_OPTIONS,
    add_gallery_item,
    bind_location,
    build_alter_view,
    build_affiliation_view,
    build_dashboard_context,
    build_location_view,
    create_entry_with_level,
    create_affiliation_prefix,
    create_alter_prefix,
    create_relation_tag,
    create_special_relation_tag,
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
    remove_gallery_item,
    remove_memory_entry,
    remove_note_entry,
    remove_occupation_entry,
    remove_relation,
    resolve_entry_reference,
    rename_entry,
    save_alter_profile,
    save_storage_settings,
    save_uploaded_json,
    save_users,
    search_entries,
    set_relation_tag,
    set_entry_level,
    update_memory_tree,
    update_notes,
    update_affiliation_membership,
    update_occupation_entry,
    user_can_create,
)


APP_DIR = Path(__file__).resolve().parent
DATA_DIR = Path(os.getenv("DATA_DIR", str(APP_DIR / "data"))).resolve()
DATA_DIR.mkdir(parents=True, exist_ok=True)
migrate_legacy_local_files(APP_DIR, DATA_DIR)
app = Flask(__name__)
app.config["SECRET_KEY"] = os.getenv("SECRET_KEY", "change-me-for-production")
logger = logging.getLogger(__name__)


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


def clear_request_caches():
    if has_request_context():
        for attr in ("users_data_cache", "tracker_data_cache", "storage_settings_cache", "current_user_cache"):
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
    user = current_user()
    return int(user.get("level", 1)) if user else 0


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
    return {
        "current_user": user,
        "current_role": user["role"] if user else None,
        "current_level": int(user.get("level", 1)) if user else None,
        "can_manage_tracker": can_manage_tracker(),
        "can_create_records": can_manage_tracker(),
        "status_options": STATUS_OPTIONS,
        "user_level_options": USER_LEVEL_OPTIONS,
        "entry_level_options": ENTRY_LEVEL_OPTIONS,
        "month_options": MONTH_OPTIONS,
        "gender_options": GENDER_OPTIONS,
        "organ_options": ORGAN_OPTIONS,
        "relationship_style_options": RELATIONSHIP_STYLE_OPTIONS,
        "legacy_value": LEGACY_VALUE,
        "storage_settings": storage_settings,
        "data_dir": str(DATA_DIR),
    }


@app.route("/")
def index():
    return redirect(url_for("dashboard" if current_user() else "login"))


@app.route("/media/<kind>/<entry_id>/<path:filename>")
@login_required
def media_file(kind, entry_id, filename):
    if kind not in {"alter", "location"}:
        flash("Unknown media type.", "error")
        return redirect(url_for("dashboard"))
    if not entry_is_accessible(get_tracker_data(), kind, entry_id, current_user_level()):
        flash("You do not have access to that media.", "error")
        return redirect(url_for("dashboard"))
    media_name = media_storage_name(kind, entry_id, filename)
    payload = storage.read_bytes(media_name)
    if payload is None:
        flash("Media file not found.", "error")
        return redirect(url_for("dashboard"))
    return Response(payload, mimetype=Path(filename).suffix and ({".jpg":"image/jpeg",".jpeg":"image/jpeg",".png":"image/png",".gif":"image/gif",".webp":"image/webp"}.get(Path(filename).suffix.lower(), "application/octet-stream")) or "application/octet-stream")


@app.route("/admin")
@login_required
@roles_required("admin")
def admin_options():
    return render_template("admin_options.html")


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

        level = 4 if not users_data["users"] else 1
        role = "admin" if level == 4 else "user"
        user = {
            "id": os.urandom(12).hex(),
            "username": username,
            "password_hash": generate_password_hash(password),
            "level": level,
            "role": role,
            "creation_permission": level == 4,
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
    user_level = current_user_level()
    name = request.form.get("name", "")
    entry_id = request.form.get("entry_id", "").strip()
    level = request.form.get("level", "3")
    try:
        requested_level = int(level)
    except ValueError:
        requested_level = 3
    if user_level < 4 and requested_level > user_level:
        flash("You cannot create an entry above your own level.", "error")
        return redirect(url_for("dashboard"))
    if kind == "alter" and not entry_id:
        entry_id = generate_unique_hash(storage, data["alter_prefixes"][0])
    elif kind == "location" and not entry_id:
        entry_id = generate_unique_hash(storage, LOCATION_PREFIX)
    elif kind == "affiliation" and not entry_id:
        entry_id = generate_unique_hash(storage, data["affiliation_prefixes"][0])
    bucket_map = {"alter": ("alters", "alter"), "location": ("locations", "location"), "affiliation": ("affiliations", "affiliation")}
    success, message = create_entry_with_level(storage, bucket_map[kind][0], bucket_map[kind][1], name, entry_id, level)
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
    kind = request.args.get("kind", "alter")
    query = request.args.get("q", "")
    results = search_entries(get_tracker_data(), kind, query, current_user_level())
    return render_template("search_results.html", kind=kind, query=query, results=results)


@app.route("/alter/<alter_id>")
@login_required
def alter_detail(alter_id):
    view = build_alter_view(get_tracker_data(), alter_id, current_user_level())
    if not view:
        flash("Unknown or inaccessible alter.", "error")
        return redirect(url_for("dashboard"))
    return render_template("alter_detail.html", view=view)


@app.route("/alter/<alter_id>/profile", methods=["POST"])
@login_required
@tracker_write_required
def save_alter(alter_id):
    data = get_tracker_data()
    user_level = current_user_level()
    if not entry_is_accessible(data, "alter", alter_id, user_level):
        flash("You do not have access to that alter.", "error")
        return redirect(url_for("dashboard"))
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
    if affiliation_id and not entry_is_accessible(data, "affiliation", affiliation_id, user_level):
        flash("You do not have access to that affiliation.", "error")
        return redirect(url_for("alter_detail", alter_id=alter_id))
    if request.form.get("action") == "remove":
        success, message = remove_affiliation_membership(storage, alter_id, affiliation_id)
    else:
        success, message = update_affiliation_membership(storage, alter_id, affiliation_id, request.form.get("status", "Current"))
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
    view = build_location_view(get_tracker_data(), location_id, current_user_level())
    if not view:
        flash("Unknown or inaccessible location.", "error")
        return redirect(url_for("dashboard"))
    return render_template("location_detail.html", view=view)


@app.route("/gallery/<kind>/<entry_id>", methods=["POST"])
@login_required
@tracker_write_required
def update_gallery(kind, entry_id):
    if kind not in {"alter", "location"}:
        flash("Unsupported gallery type.", "error")
        return redirect(url_for("dashboard"))
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
                target = "alter_detail" if kind == "alter" else "location_detail"
                arg_name = "alter_id" if kind == "alter" else "location_id"
                return redirect(url_for(target, **{arg_name: entry_id}))
        if image_url and not is_managed_media_url(image_url):
            success, managed_image_url = import_gallery_media_from_url(storage, kind, entry_id, image_url)
            if success:
                image_url = managed_image_url
            else:
                flash(managed_image_url, "error")
                target = "alter_detail" if kind == "alter" else "location_detail"
                arg_name = "alter_id" if kind == "alter" else "location_id"
                return redirect(url_for(target, **{arg_name: entry_id}))
        success, message = add_gallery_item(storage, kind, entry_id, image_url)
        if not success and uploaded_image_url:
            delete_gallery_upload(uploaded_image_url)
    flash(message, "success" if success else "error")
    clear_request_caches()
    target = "alter_detail" if kind == "alter" else "location_detail"
    arg_name = "alter_id" if kind == "alter" else "location_id"
    return redirect(url_for(target, **{arg_name: entry_id}))


@app.route("/affiliation/<affiliation_id>")
@login_required
def affiliation_detail(affiliation_id):
    view = build_affiliation_view(get_tracker_data(), affiliation_id, current_user_level())
    if not view:
        flash("Unknown or inaccessible affiliation.", "error")
        return redirect(url_for("dashboard"))
    return render_template("affiliation_detail.html", view=view)


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


@app.route("/entry-level/<kind>/<entry_id>", methods=["POST"])
@login_required
@tracker_write_required
def update_record_level(kind, entry_id):
    bucket_map = {"alter": "alters", "location": "locations", "affiliation": "affiliations"}
    if not entry_is_accessible(get_tracker_data(), kind, entry_id, current_user_level()):
        flash("You do not have access to that entry.", "error")
        return redirect(url_for("dashboard"))
    try:
        requested_level = int(request.form.get("level", "3"))
    except ValueError:
        requested_level = 3
    if current_user_level() < 4 and requested_level > current_user_level():
        flash("You cannot assign an entry above your own level.", "error")
        return redirect(request.referrer or url_for("dashboard"))
    success, message = set_entry_level(storage, bucket_map[kind], entry_id, requested_level)
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
            if action == "level" and int(user.get("level", 1)) != 4:
                try:
                    level = int(request.form.get("level", "1"))
                except ValueError:
                    level = 1
                user["level"] = max(1, min(3, level))
                user["role"] = "mod" if user["level"] == 3 else "user"
            elif action == "password":
                new_password = request.form.get("new_password", "")
                if new_password:
                    user["password_hash"] = generate_password_hash(new_password)
            elif action == "creation" and int(user.get("level", 1)) != 4:
                user["creation_permission"] = request.form.get("creation_permission") == "on"
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
