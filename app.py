import os
from pathlib import Path

from flask import Flask, flash, redirect, render_template, request, session, url_for
from werkzeug.security import check_password_hash, generate_password_hash

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
    load_data,
    load_storage_settings,
    load_users,
    LocalStorage,
    migrate_storage_data,
    migrate_legacy_local_files,
    remove_affiliation_membership,
    remove_gallery_item,
    remove_occupation_entry,
    remove_relation,
    rename_entry,
    save_alter_profile,
    save_storage_settings,
    save_uploaded_json,
    save_users,
    search_entries,
    set_relation_tag,
    set_entry_level,
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
storage = get_storage(DATA_DIR)
ensure_storage_files(storage)


def refresh_storage():
    global storage
    storage = get_storage(DATA_DIR)
    ensure_storage_files(storage)
    return storage


def current_user():
    user_id = session.get("user_id")
    if not user_id:
        return None
    for user in load_users(storage)["users"]:
        if user["id"] == user_id and user.get("active", True):
            return user
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
    storage_settings = load_storage_settings(DATA_DIR)
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


@app.route("/admin")
@login_required
@roles_required("admin")
def admin_options():
    return render_template("admin_options.html")


@app.route("/register", methods=["GET", "POST"])
def register():
    users_data = load_users(storage)
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
        session["user_id"] = user["id"]
        flash(f"Account created. Logged in as {role}.", "success")
        return redirect(url_for("dashboard"))
    return render_template("register.html")


@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        username = request.form.get("username", "").strip()
        password = request.form.get("password", "")
        for user in load_users(storage)["users"]:
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
    data = load_data(storage)
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
    data = load_data(storage)
    prefix = request.form.get("prefix", "")
    if kind == "alter":
        if prefix and prefix not in get_alter_prefixes(data):
            flash("Unknown alter prefix.", "error")
            return redirect(url_for("dashboard"))
    elif kind == "location":
        prefix = LOCATION_PREFIX if prefix != "" else ""
    elif kind == "affiliation":
        if prefix and prefix not in get_affiliation_prefixes(data):
            flash("Unknown affiliation prefix.", "error")
            return redirect(url_for("dashboard"))
    else:
        flash("Unknown ID type.", "error")
        return redirect(url_for("dashboard"))
    flash(f"Generated ID: {generate_unique_hash(storage, prefix)}", "success")
    return redirect(url_for("dashboard"))


@app.route("/create/<kind>", methods=["POST"])
@login_required
@tracker_write_required
def create_record(kind):
    data = load_data(storage)
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
    results = search_entries(load_data(storage), kind, query, current_user_level())
    return render_template("search_results.html", kind=kind, query=query, results=results)


@app.route("/alter/<alter_id>")
@login_required
def alter_detail(alter_id):
    view = build_alter_view(load_data(storage), alter_id, current_user_level())
    if not view:
        flash("Unknown or inaccessible alter.", "error")
        return redirect(url_for("dashboard"))
    return render_template("alter_detail.html", view=view)


@app.route("/alter/<alter_id>/profile", methods=["POST"])
@login_required
@tracker_write_required
def save_alter(alter_id):
    data = load_data(storage)
    user_level = current_user_level()
    if not entry_is_accessible(data, "alter", alter_id, user_level):
        flash("You do not have access to that alter.", "error")
        return redirect(url_for("dashboard"))
    success, message = rename_entry(storage, "alters", "alter", alter_id, request.form.get("name", ""))
    if success:
        success, message = save_alter_profile(storage, alter_id, request.form)
    location_id = request.form.get("location_id", "").strip()
    if success and location_id and entry_is_accessible(data, "location", location_id, user_level):
        success, message = bind_location(storage, alter_id, location_id)
    elif success and location_id:
        success, message = False, "You do not have access to that location."
    flash(message, "success" if success else "error")
    return redirect(url_for("alter_detail", alter_id=alter_id))


@app.route("/alter/<alter_id>/affiliations", methods=["POST"])
@login_required
@tracker_write_required
def update_alter_affiliations(alter_id):
    data = load_data(storage)
    user_level = current_user_level()
    if not entry_is_accessible(data, "alter", alter_id, user_level):
        flash("You do not have access to that alter.", "error")
        return redirect(url_for("dashboard"))
    affiliation_id = request.form.get("affiliation_id", "")
    if affiliation_id and not entry_is_accessible(data, "affiliation", affiliation_id, user_level):
        flash("You do not have access to that affiliation.", "error")
        return redirect(url_for("alter_detail", alter_id=alter_id))
    if request.form.get("action") == "remove":
        success, message = remove_affiliation_membership(storage, alter_id, affiliation_id)
    else:
        success, message = update_affiliation_membership(storage, alter_id, affiliation_id, request.form.get("status", "Current"))
    flash(message, "success" if success else "error")
    return redirect(url_for("alter_detail", alter_id=alter_id))


@app.route("/alter/<alter_id>/occupations", methods=["POST"])
@login_required
@tracker_write_required
def update_alter_occupations(alter_id):
    if not entry_is_accessible(load_data(storage), "alter", alter_id, current_user_level()):
        flash("You do not have access to that alter.", "error")
        return redirect(url_for("dashboard"))
    if request.form.get("action") == "remove":
        success, message = remove_occupation_entry(storage, alter_id, request.form.get("occupation", ""))
    else:
        success, message = update_occupation_entry(storage, alter_id, request.form.get("occupation", ""), request.form.get("status", "Current"))
    flash(message, "success" if success else "error")
    return redirect(url_for("alter_detail", alter_id=alter_id))


@app.route("/alter/<alter_id>/relation", methods=["POST"])
@login_required
@tracker_write_required
def update_relation(alter_id):
    data = load_data(storage)
    user_level = current_user_level()
    other_id = request.form.get("other_id", "")
    if not entry_is_accessible(data, "alter", alter_id, user_level) or (other_id and not entry_is_accessible(data, "alter", other_id, user_level)):
        flash("You do not have access to one or more alters in that relation.", "error")
        return redirect(url_for("dashboard"))
    if request.form.get("action") == "remove":
        success, message = remove_relation(storage, alter_id, request.form.get("tag", ""), other_id)
    else:
        success, message = set_relation_tag(storage, alter_id, request.form.get("tag", ""), other_id)
    flash(message, "success" if success else "error")
    return redirect(url_for("alter_detail", alter_id=alter_id))


@app.route("/alter/<alter_id>/bulk", methods=["POST"])
@login_required
@tracker_write_required
def bulk_relations(alter_id):
    data = load_data(storage)
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
    return redirect(url_for("alter_detail", alter_id=alter_id))


@app.route("/location/<location_id>")
@login_required
def location_detail(location_id):
    view = build_location_view(load_data(storage), location_id, current_user_level())
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
    if not entry_is_accessible(load_data(storage), kind, entry_id, current_user_level()):
        flash("You do not have access to that entry.", "error")
        return redirect(url_for("dashboard"))
    image_url = request.form.get("image_url", "").strip()
    action = request.form.get("action", "add")
    if action == "remove":
        success, message = remove_gallery_item(storage, kind, entry_id, image_url)
    else:
        success, message = add_gallery_item(storage, kind, entry_id, image_url)
    flash(message, "success" if success else "error")
    target = "alter_detail" if kind == "alter" else "location_detail"
    arg_name = "alter_id" if kind == "alter" else "location_id"
    return redirect(url_for(target, **{arg_name: entry_id}))


@app.route("/affiliation/<affiliation_id>")
@login_required
def affiliation_detail(affiliation_id):
    view = build_affiliation_view(load_data(storage), affiliation_id, current_user_level())
    if not view:
        flash("Unknown or inaccessible affiliation.", "error")
        return redirect(url_for("dashboard"))
    return render_template("affiliation_detail.html", view=view)


@app.route("/rename/<kind>/<entry_id>", methods=["POST"])
@login_required
@tracker_write_required
def rename_record(kind, entry_id):
    bucket_map = {"alter": ("alters", "alter"), "location": ("locations", "location"), "affiliation": ("affiliations", "affiliation")}
    if not entry_is_accessible(load_data(storage), kind, entry_id, current_user_level()):
        flash("You do not have access to that entry.", "error")
        return redirect(url_for("dashboard"))
    success, message = rename_entry(storage, bucket_map[kind][0], bucket_map[kind][1], entry_id, request.form.get("name", ""))
    flash(message, "success" if success else "error")
    return redirect(request.referrer or url_for("dashboard"))


@app.route("/entry-level/<kind>/<entry_id>", methods=["POST"])
@login_required
@tracker_write_required
def update_record_level(kind, entry_id):
    bucket_map = {"alter": "alters", "location": "locations", "affiliation": "affiliations"}
    if not entry_is_accessible(load_data(storage), kind, entry_id, current_user_level()):
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
        return redirect(url_for("admin_import"))
    return render_template("admin_import.html")


@app.route("/admin/users", methods=["GET", "POST"])
@login_required
@roles_required("admin")
def admin_users():
    users_data = load_users(storage)
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
        flash("User updated.", "success")
        return redirect(url_for("admin_users"))
    return render_template("admin_users.html", users=users_data["users"])


@app.route("/admin/storage", methods=["GET", "POST"])
@login_required
@roles_required("admin")
def admin_storage():
    current_settings = load_storage_settings(DATA_DIR)
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
            flash("Data migrated to the configured storage backend.", "success")
            return redirect(url_for("admin_storage"))
    settings = load_storage_settings(DATA_DIR)
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
