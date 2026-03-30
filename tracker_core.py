import json
import secrets
import shutil
import string
from datetime import date
from pathlib import Path

try:
    import boto3
    from botocore.exceptions import ClientError
except ImportError:  # pragma: no cover
    boto3 = None
    ClientError = Exception


DATA_FILE = "relationship_data.json"
HASH_FILE = "saved_hashes.json"
USER_FILE = "users.json"
AFFILIATION_PREFIX_FILE = "affiliation_prefixes.json"
STORAGE_SETTINGS_FILE = "storage_settings.json"
DEFAULT_RELATION_TAGS = ["NONE", "Sibling", "Friend", "Partner", "Spouse"]
DEFAULT_SPECIAL_RELATION_TAGS = {"Parent": "Child", "Child": "Parent"}
DEFAULT_ALTER_PREFIXES = ["UFA-"]
DEFAULT_AFFILIATION_PREFIXES = ["AFF-"]
LOCATION_PREFIX = "LOC-"
LEGACY_VALUE = "LEGACY"
STATUS_OPTIONS = ["Current", "Formerly"]
ROLE_OPTIONS = ["user", "mod", "admin"]
USER_LEVEL_OPTIONS = [1, 2, 3, 4]
ENTRY_LEVEL_OPTIONS = [1, 2, 3]
GENDER_OPTIONS = ["Cisgender", "Transgender", "Nonbinary"]
ORGAN_OPTIONS = ["Penis", "Vagina", "Mixed", "Varies"]
RELATIONSHIP_STYLE_OPTIONS = ["Polyamorous", "Monogamous", "Harem (Center)", "Harem (Member)"]
MONTH_OPTIONS = [
    "January", "February", "March", "April", "May", "June",
    "July", "August", "September", "October", "November", "December",
]
HASH_LENGTH = 24
ALPHABET = string.ascii_letters + string.digits


class LocalStorage:
    def __init__(self, root):
        self.root = Path(root)

    def read_json(self, name, default):
        path = self.root / name
        if not path.exists():
            return default
        try:
            with path.open("r", encoding="utf-8") as file:
                return json.load(file)
        except (json.JSONDecodeError, OSError):
            return default

    def write_json(self, name, data):
        path = self.root / name
        with path.open("w", encoding="utf-8") as file:
            json.dump(data, file, indent=2)


class S3Storage:
    def __init__(self, bucket, prefix=""):
        if boto3 is None:
            raise RuntimeError("boto3 is required for S3 storage.")
        self.bucket = bucket
        self.prefix = prefix.strip("/")
        self.client = boto3.client("s3")

    def _key(self, name):
        return f"{self.prefix}/{name}" if self.prefix else name

    def read_json(self, name, default):
        try:
            response = self.client.get_object(Bucket=self.bucket, Key=self._key(name))
            return json.loads(response["Body"].read().decode("utf-8"))
        except (ClientError, json.JSONDecodeError):
            return default

    def write_json(self, name, data):
        body = json.dumps(data, indent=2).encode("utf-8")
        self.client.put_object(Bucket=self.bucket, Key=self._key(name), Body=body, ContentType="application/json")


def get_storage(root):
    import os

    settings = load_storage_settings(root)
    backend = os.getenv("STORAGE_BACKEND", settings.get("backend", "local")).lower()
    if backend == "s3":
        bucket = os.getenv("S3_BUCKET", settings.get("s3_bucket", "")).strip()
        prefix = os.getenv("S3_PREFIX", settings.get("s3_prefix", ""))
        if not bucket:
            raise RuntimeError("S3_BUCKET is required when STORAGE_BACKEND=s3.")
        return S3Storage(bucket, prefix)
    return LocalStorage(root)


def unique_items(items):
    seen = set()
    ordered = []
    for item in items:
        if item not in seen:
            seen.add(item)
            ordered.append(item)
    return ordered


def tracker_default():
    return {
        "alters": {},
        "locations": {},
        "affiliations": {},
        "entry_levels": {"alters": {}, "locations": {}, "affiliations": {}},
        "relations": [],
        "location_bindings": {},
        "relation_tags": list(DEFAULT_RELATION_TAGS),
        "special_relation_tags": dict(DEFAULT_SPECIAL_RELATION_TAGS),
        "alter_prefixes": list(DEFAULT_ALTER_PREFIXES),
        "affiliation_prefixes": list(DEFAULT_AFFILIATION_PREFIXES),
        "alter_profiles": {},
        "location_galleries": {},
    }


def users_default():
    return {"users": []}


def hashes_default():
    return {"hashes": []}


def storage_settings_default():
    return {
        "backend": "local",
        "s3_bucket": "",
        "s3_prefix": "",
    }


def load_storage_settings(root):
    path = Path(root) / STORAGE_SETTINGS_FILE
    if not path.exists():
        return storage_settings_default()
    try:
        with path.open("r", encoding="utf-8") as file:
            data = json.load(file)
    except (json.JSONDecodeError, OSError):
        return storage_settings_default()
    settings = storage_settings_default()
    settings.update({key: data.get(key, value) for key, value in settings.items()})
    return settings


def save_storage_settings(root, settings):
    path = Path(root) / STORAGE_SETTINGS_FILE
    payload = storage_settings_default()
    payload.update({key: settings.get(key, value) for key, value in payload.items()})
    with path.open("w", encoding="utf-8") as file:
        json.dump(payload, file, indent=2)


def migrate_legacy_local_files(source_root, destination_root):
    source_root = Path(source_root)
    destination_root = Path(destination_root)
    destination_root.mkdir(parents=True, exist_ok=True)
    filenames = [DATA_FILE, HASH_FILE, USER_FILE, STORAGE_SETTINGS_FILE]
    for filename in filenames:
        source_path = source_root / filename
        destination_path = destination_root / filename
        if source_path.exists() and not destination_path.exists():
            shutil.move(str(source_path), str(destination_path))


def ensure_storage_files(storage):
    storage.write_json(DATA_FILE, storage.read_json(DATA_FILE, tracker_default()))
    storage.write_json(HASH_FILE, storage.read_json(HASH_FILE, hashes_default()))
    storage.write_json(USER_FILE, storage.read_json(USER_FILE, users_default()))


def legacy_profile():
    return {
        "aliases": [],
        "species": LEGACY_VALUE,
        "age": LEGACY_VALUE,
        "birthday_month": None,
        "birthday_year": None,
        "birthday_last_processed_year": None,
        "gender": LEGACY_VALUE,
        "reproductive_organ": LEGACY_VALUE,
        "sexual_romantic_attraction": LEGACY_VALUE,
        "relationship_style": LEGACY_VALUE,
        "height": LEGACY_VALUE,
        "occupations": [],
        "affiliations": [],
        "gallery": [],
    }


def load_users(storage):
    data = storage.read_json(USER_FILE, users_default())
    data.setdefault("users", [])
    changed = False
    admin_assigned = False
    for user in data["users"]:
        role = user.get("role", "user")
        if "level" not in user:
            if role == "admin" and not admin_assigned:
                user["level"] = 4
            elif role in {"admin", "mod"}:
                user["level"] = 3
            else:
                user["level"] = 1
            changed = True
        try:
            user["level"] = int(user["level"])
        except (TypeError, ValueError):
            user["level"] = 1
            changed = True
        if user["level"] >= 4:
            if admin_assigned:
                user["level"] = 3
                changed = True
            else:
                user["level"] = 4
                admin_assigned = True
        elif user["level"] < 1:
            user["level"] = 1
            changed = True
        elif user["level"] > 3:
            user["level"] = 3
            changed = True
        if user["level"] == 4:
            user["role"] = "admin"
        elif user["level"] >= 3:
            user["role"] = "mod"
        else:
            user["role"] = "user"
        if "creation_permission" not in user:
            user["creation_permission"] = user["level"] >= 3
            changed = True
        user.setdefault("active", True)
        if user["level"] == 4 and not user["creation_permission"]:
            user["creation_permission"] = True
            changed = True
    if data["users"] and not admin_assigned:
        data["users"][0]["level"] = 4
        data["users"][0]["role"] = "admin"
        data["users"][0]["creation_permission"] = True
        changed = True
    if changed:
        save_users(storage, data)
    return data


def save_users(storage, data):
    storage.write_json(USER_FILE, data)


def load_saved_hashes(storage):
    return set(storage.read_json(HASH_FILE, hashes_default()).get("hashes", []))


def save_saved_hashes(storage, hashes):
    storage.write_json(HASH_FILE, {"hashes": sorted(hashes)})


def normalize_status_entries(entries):
    normalized = []
    for item in entries or []:
        value = str(item.get("value", "")).strip()
        status = str(item.get("status", STATUS_OPTIONS[0])).strip()
        if value:
            normalized.append({"value": value, "status": status if status in STATUS_OPTIONS else STATUS_OPTIONS[0]})
    return normalized


def update_profile_birthday_age(profile):
    month = profile.get("birthday_month")
    age = profile.get("age")
    if month is None or not isinstance(age, int):
        return False
    today = date.today()
    last_processed = profile.get("birthday_last_processed_year")
    if (today.month, today.day) >= (month, 1) and last_processed != today.year:
        profile["age"] = age + 1
        profile["birthday_last_processed_year"] = today.year
        return True
    return False


def sync_saved_hashes_with_tracker(storage, data):
    hashes = load_saved_hashes(storage)
    hashes.update(data["alters"].keys())
    hashes.update(data["locations"].keys())
    hashes.update(data["affiliations"].keys())
    save_saved_hashes(storage, hashes)
    return hashes


def load_data(storage):
    data = storage.read_json(DATA_FILE, tracker_default())
    data.setdefault("alters", {})
    data.setdefault("locations", {})
    data.setdefault("affiliations", {})
    data.setdefault("entry_levels", {"alters": {}, "locations": {}, "affiliations": {}})
    data.setdefault("relations", [])
    data.setdefault("location_bindings", {})
    data.setdefault("relation_tags", list(DEFAULT_RELATION_TAGS))
    data.setdefault("special_relation_tags", dict(DEFAULT_SPECIAL_RELATION_TAGS))
    data.setdefault("alter_prefixes", list(DEFAULT_ALTER_PREFIXES))
    data.setdefault("affiliation_prefixes", list(DEFAULT_AFFILIATION_PREFIXES))
    data.setdefault("alter_profiles", {})
    data.setdefault("location_galleries", {})
    data["entry_levels"].setdefault("alters", {})
    data["entry_levels"].setdefault("locations", {})
    data["entry_levels"].setdefault("affiliations", {})

    data["relation_tags"] = unique_items(
        list(DEFAULT_RELATION_TAGS)
        + data["relation_tags"]
        + list(data["special_relation_tags"].keys())
        + list(data["special_relation_tags"].values())
    )
    data["alter_prefixes"] = unique_items(list(DEFAULT_ALTER_PREFIXES) + data["alter_prefixes"])
    data["affiliation_prefixes"] = unique_items(list(DEFAULT_AFFILIATION_PREFIXES) + data["affiliation_prefixes"])

    changed = False
    migrated_relations = []
    for relation in data["relations"]:
        if "source_id" in relation:
            migrated_relations.append(
                {
                    "source_id": relation["source_id"],
                    "target_id": relation["target_id"],
                    "tag": relation["tag"],
                    "reverse_tag": relation.get("reverse_tag", relation["tag"]),
                    "legacy_relation": relation.get("legacy_relation"),
                }
            )
        elif "id_one" in relation:
            changed = True
            migrated_relations.append(
                {
                    "source_id": relation["id_one"],
                    "target_id": relation["id_two"],
                    "tag": "NONE",
                    "reverse_tag": "NONE",
                    "legacy_relation": relation.get("relation", "NONE"),
                }
            )
    data["relations"] = migrated_relations

    for alter_id in data["alters"]:
        if alter_id not in data["entry_levels"]["alters"]:
            data["entry_levels"]["alters"][alter_id] = 3
            changed = True
        profile = data["alter_profiles"].setdefault(alter_id, legacy_profile())
        for key, default_value in legacy_profile().items():
            if key not in profile:
                profile[key] = default_value
                changed = True
        profile["occupations"] = normalize_status_entries(profile.get("occupations"))
        profile["affiliations"] = normalize_status_entries(profile.get("affiliations"))
        profile["gallery"] = [item for item in profile.get("gallery", []) if str(item).strip()]
        if update_profile_birthday_age(profile):
            changed = True

    for location_id in data["locations"]:
        if location_id not in data["entry_levels"]["locations"]:
            data["entry_levels"]["locations"][location_id] = 3
            changed = True
        if location_id not in data["location_galleries"]:
            data["location_galleries"][location_id] = []
            changed = True
        else:
            data["location_galleries"][location_id] = [item for item in data["location_galleries"].get(location_id, []) if str(item).strip()]

    for affiliation_id in data["affiliations"]:
        if affiliation_id not in data["entry_levels"]["affiliations"]:
            data["entry_levels"]["affiliations"][affiliation_id] = 3
            changed = True

    if changed:
        save_data(storage, data)
    sync_saved_hashes_with_tracker(storage, data)
    return data


def save_data(storage, data):
    storage.write_json(DATA_FILE, data)


def get_synced_hashes(storage):
    return sync_saved_hashes_with_tracker(storage, load_data(storage))


def generate_hash(prefix=""):
    return prefix + "".join(secrets.choice(ALPHABET) for _ in range(HASH_LENGTH))


def generate_unique_hash(storage, prefix=""):
    hashes = get_synced_hashes(storage)
    while True:
        candidate = generate_hash(prefix)
        if candidate not in hashes:
            hashes.add(candidate)
            save_saved_hashes(storage, hashes)
            return candidate


def export_hashes(storage, filename):
    filename = filename.strip()
    if not filename:
        return False, "Specify a filename."
    try:
        with Path(filename).open("w", encoding="utf-8") as file:
            for item in sorted(get_synced_hashes(storage)):
                file.write(item + "\n")
    except OSError as error:
        return False, f"Failed to export hashes: {error}"
    return True, f"Exported hashes to {filename}"


def clear_hashes(storage):
    save_saved_hashes(storage, set())
    hashes = get_synced_hashes(storage)
    return True, f"Cleared unassigned hashes. Reserved hashes still attached: {len(hashes)}"


def get_counts(data):
    return {
        "alters": len(data["alters"]),
        "locations": len(data["locations"]),
        "affiliations": len(data["affiliations"]),
        "relations": len(data["relations"]),
        "bindings": len(data["location_bindings"]),
        "tags": len(get_available_relation_tags(data)),
    }


def get_alter_prefixes(data):
    return data["alter_prefixes"]


def get_affiliation_prefixes(data):
    return data["affiliation_prefixes"]


def create_entry(storage, bucket_name, entity_label, name, entry_id):
    return create_entry_with_level(storage, bucket_name, entity_label, name, entry_id, 3)


def create_entry_with_level(storage, bucket_name, entity_label, name, entry_id, level):
    data = load_data(storage)
    name = name.strip()
    entry_id = entry_id.strip()
    if not name:
        return False, f"{entity_label.title()} name is required."
    if entry_id in data[bucket_name]:
        return False, f'ID "{entry_id}" already exists.'
    try:
        numeric_level = int(level)
    except (TypeError, ValueError):
        return False, "Entry level must be numeric."
    if numeric_level < 1 or numeric_level > 3:
        return False, "Entry level must be between 1 and 3."
    data[bucket_name][entry_id] = name
    data["entry_levels"][bucket_name][entry_id] = numeric_level
    if bucket_name == "alters":
        data["alter_profiles"].setdefault(entry_id, legacy_profile())
    save_data(storage, data)
    sync_saved_hashes_with_tracker(storage, data)
    return True, f"Created {entity_label}: {name} ({entry_id})"


def get_entry_level(data, bucket_name, entry_id):
    return int(data.get("entry_levels", {}).get(bucket_name, {}).get(entry_id, 3))


def set_entry_level(storage, bucket_name, entry_id, level):
    data = load_data(storage)
    if entry_id not in data[bucket_name]:
        return False, "That entry does not exist."
    try:
        numeric_level = int(level)
    except ValueError:
        return False, "Level must be numeric."
    if numeric_level < 1 or numeric_level > 3:
        return False, "Entry level must be between 1 and 3."
    data["entry_levels"][bucket_name][entry_id] = numeric_level
    save_data(storage, data)
    return True, f"Updated entry level to {numeric_level}."


def can_access_level(user_level, entry_level):
    return user_level >= entry_level


def user_can_create(user):
    return bool(user and (int(user.get("level", 1)) >= 4 or user.get("creation_permission")))


def entry_is_accessible(data, kind, entry_id, user_level):
    bucket_map = {"alter": "alters", "location": "locations", "affiliation": "affiliations"}
    bucket_name = bucket_map[kind]
    if entry_id not in data[bucket_name]:
        return False
    return can_access_level(user_level, get_entry_level(data, bucket_name, entry_id))


def visible_entries(data, kind, user_level):
    bucket_map = {"alter": "alters", "location": "locations", "affiliation": "affiliations"}
    bucket_name = bucket_map[kind]
    return [
        (entry_id, name)
        for entry_id, name in data[bucket_name].items()
        if entry_is_accessible(data, kind, entry_id, user_level)
    ]


def rename_entry(storage, bucket_name, entity_label, entry_id, name):
    data = load_data(storage)
    if entry_id not in data[bucket_name]:
        return False, f'Unknown {entity_label} ID "{entry_id}".'
    name = name.strip()
    if not name:
        return False, f"{entity_label.title()} name is required."
    data[bucket_name][entry_id] = name
    save_data(storage, data)
    return True, f"Renamed {entity_label}."


def create_alter_prefix(storage, prefix):
    data = load_data(storage)
    prefix = prefix.strip()
    if not prefix:
        return False, "An alter prefix is required."
    if prefix in data["alter_prefixes"]:
        return False, "That alter prefix already exists."
    data["alter_prefixes"] = unique_items(data["alter_prefixes"] + [prefix])
    save_data(storage, data)
    return True, f'Created alter prefix "{prefix}".'


def delete_alter_prefix(storage, prefix):
    data = load_data(storage)
    prefix = prefix.strip()
    if prefix not in data["alter_prefixes"]:
        return False, "That alter prefix does not exist."
    if len(data["alter_prefixes"]) == 1:
        return False, "At least one alter prefix must remain."
    data["alter_prefixes"] = [item for item in data["alter_prefixes"] if item != prefix]
    save_data(storage, data)
    return True, f'Removed alter prefix "{prefix}".'


def create_affiliation_prefix(storage, prefix):
    data = load_data(storage)
    prefix = prefix.strip()
    if not prefix:
        return False, "An affiliation prefix is required."
    if prefix in data["affiliation_prefixes"]:
        return False, "That affiliation prefix already exists."
    data["affiliation_prefixes"] = unique_items(data["affiliation_prefixes"] + [prefix])
    save_data(storage, data)
    return True, f'Created affiliation prefix "{prefix}".'


def delete_affiliation_prefix(storage, prefix):
    data = load_data(storage)
    prefix = prefix.strip()
    if prefix not in data["affiliation_prefixes"]:
        return False, "That affiliation prefix does not exist."
    if len(data["affiliation_prefixes"]) == 1:
        return False, "At least one affiliation prefix must remain."
    data["affiliation_prefixes"] = [item for item in data["affiliation_prefixes"] if item != prefix]
    save_data(storage, data)
    return True, f'Removed affiliation prefix "{prefix}".'


def get_available_relation_tags(data):
    return unique_items(data["relation_tags"] + list(data["special_relation_tags"].keys()) + list(data["special_relation_tags"].values()))


def create_relation_tag(storage, tag_name):
    data = load_data(storage)
    tag_name = tag_name.strip()
    if not tag_name:
        return False, "A relation tag is required."
    if tag_name in get_available_relation_tags(data):
        return False, "That relation tag already exists."
    data["relation_tags"] = unique_items(data["relation_tags"] + [tag_name])
    save_data(storage, data)
    return True, f'Created relation tag "{tag_name}".'


def create_special_relation_tag(storage, forward_tag, reverse_tag):
    data = load_data(storage)
    forward_tag = forward_tag.strip()
    reverse_tag = reverse_tag.strip()
    if not forward_tag or not reverse_tag:
        return False, "Both relation tags are required."
    data["special_relation_tags"][forward_tag] = reverse_tag
    data["special_relation_tags"][reverse_tag] = forward_tag
    data["relation_tags"] = unique_items(data["relation_tags"] + [forward_tag, reverse_tag])
    save_data(storage, data)
    return True, f'Created relation tag pair "{forward_tag}" and "{reverse_tag}".'


def bind_location(storage, alter_id, location_id):
    data = load_data(storage)
    if alter_id not in data["alters"] or location_id not in data["locations"]:
        return False, "Alter and location IDs must exist."
    data["location_bindings"][alter_id] = location_id
    save_data(storage, data)
    return True, "Updated location binding."


def save_alter_profile(storage, alter_id, form):
    data = load_data(storage)
    if alter_id not in data["alters"]:
        return False, "Unknown alter."
    profile = data["alter_profiles"].setdefault(alter_id, legacy_profile())
    profile["aliases"] = [item.strip() for item in form.get("aliases", "").split(",") if item.strip()]
    profile["species"] = form.get("species", "").strip() or LEGACY_VALUE
    age_text = form.get("age", "").strip()
    if age_text and age_text != LEGACY_VALUE:
        if not age_text.isdigit():
            return False, "Age must be numeric or LEGACY."
        profile["age"] = int(age_text)
    else:
        profile["age"] = LEGACY_VALUE
    birthday_month = form.get("birthday_month", "")
    birthday_year = form.get("birthday_year", "").strip()
    if birthday_month and birthday_year:
        if birthday_month not in MONTH_OPTIONS or not birthday_year.isdigit():
            return False, "Birthday month/year is invalid."
        month = MONTH_OPTIONS.index(birthday_month) + 1
        profile["birthday_month"] = month
        profile["birthday_year"] = int(birthday_year)
        today = date.today()
        profile["birthday_last_processed_year"] = today.year if (today.month, today.day) >= (month, 1) else today.year - 1
    else:
        profile["birthday_month"] = None
        profile["birthday_year"] = None
        profile["birthday_last_processed_year"] = None
    profile["gender"] = form.get("gender", "") or LEGACY_VALUE
    profile["reproductive_organ"] = form.get("reproductive_organ", "") or LEGACY_VALUE
    profile["sexual_romantic_attraction"] = form.get("attraction", "").strip() or LEGACY_VALUE
    profile["relationship_style"] = form.get("relationship_style", "") or LEGACY_VALUE
    profile["height"] = form.get("height", "").strip() or LEGACY_VALUE
    save_data(storage, data)
    return True, "Saved alter profile."


def update_affiliation_membership(storage, alter_id, affiliation_id, status):
    data = load_data(storage)
    if alter_id not in data["alters"] or affiliation_id not in data["affiliations"]:
        return False, "Alter and affiliation must exist."
    profile = data["alter_profiles"].setdefault(alter_id, legacy_profile())
    profile["affiliations"] = [item for item in profile["affiliations"] if item["value"] != affiliation_id]
    profile["affiliations"].append({"value": affiliation_id, "status": status if status in STATUS_OPTIONS else STATUS_OPTIONS[0]})
    save_data(storage, data)
    return True, "Updated affiliation membership."


def remove_affiliation_membership(storage, alter_id, affiliation_id):
    data = load_data(storage)
    profile = data["alter_profiles"].setdefault(alter_id, legacy_profile())
    before = len(profile["affiliations"])
    profile["affiliations"] = [item for item in profile["affiliations"] if item["value"] != affiliation_id]
    if len(profile["affiliations"]) == before:
        return False, "That affiliation is not assigned."
    save_data(storage, data)
    return True, "Removed affiliation membership."


def update_occupation_entry(storage, alter_id, occupation, status):
    data = load_data(storage)
    if alter_id not in data["alters"]:
        return False, "Unknown alter."
    occupation = occupation.strip()
    if not occupation:
        return False, "Occupation/role is required."
    data["alter_profiles"].setdefault(alter_id, legacy_profile())["occupations"].append(
        {"value": occupation, "status": status if status in STATUS_OPTIONS else STATUS_OPTIONS[0]}
    )
    save_data(storage, data)
    return True, "Added occupation/role."


def remove_occupation_entry(storage, alter_id, occupation):
    data = load_data(storage)
    profile = data["alter_profiles"].setdefault(alter_id, legacy_profile())
    before = len(profile["occupations"])
    profile["occupations"] = [item for item in profile["occupations"] if item["value"] != occupation]
    if len(profile["occupations"]) == before:
        return False, "That occupation/role was not found."
    save_data(storage, data)
    return True, "Removed occupation/role."


def make_relation_record(data, first_id, tag, second_id, legacy_relation=None):
    reverse_tag = data["special_relation_tags"].get(tag, tag)
    if reverse_tag == tag and str(first_id) > str(second_id):
        first_id, second_id = second_id, first_id
    return {
        "source_id": str(first_id),
        "target_id": str(second_id),
        "tag": str(tag),
        "reverse_tag": str(reverse_tag),
        "legacy_relation": legacy_relation,
    }


def relations_between(data, first_id, second_id):
    return [
        relation
        for relation in data["relations"]
        if {relation["source_id"], relation["target_id"]} == {str(first_id), str(second_id)}
    ]


def set_relation_tag(storage, first_id, tag, second_id):
    data = load_data(storage)
    if first_id not in data["alters"] or second_id not in data["alters"]:
        return False, "Both alters must exist."
    if tag not in get_available_relation_tags(data):
        return False, "Unknown relation tag."
    data["relations"] = [
        relation for relation in data["relations"]
        if {relation["source_id"], relation["target_id"]} != {str(first_id), str(second_id)}
    ]
    data["relations"].append(make_relation_record(data, first_id, tag, second_id))
    save_data(storage, data)
    return True, "Set relation tag."


def remove_relation(storage, first_id, relation_name, second_id):
    data = load_data(storage)
    matches = relations_between(data, first_id, second_id)
    if relation_name:
        matches = [item for item in matches if item["tag"] == relation_name or item["reverse_tag"] == relation_name]
    if not matches:
        return False, "That relation does not exist."
    for relation in matches:
        data["relations"].remove(relation)
    save_data(storage, data)
    return True, "Removed relation."


def add_gallery_item(storage, kind, entry_id, image_url):
    data = load_data(storage)
    image_url = image_url.strip()
    if not image_url:
        return False, "An image URL is required."
    if kind == "alter":
        if entry_id not in data["alters"]:
            return False, "Unknown alter."
        gallery = data["alter_profiles"].setdefault(entry_id, legacy_profile()).setdefault("gallery", [])
    else:
        if entry_id not in data["locations"]:
            return False, "Unknown location."
        gallery = data["location_galleries"].setdefault(entry_id, [])
    if image_url in gallery:
        return False, "That image is already in the gallery."
    gallery.append(image_url)
    save_data(storage, data)
    return True, "Added gallery image."


def remove_gallery_item(storage, kind, entry_id, image_url):
    data = load_data(storage)
    if kind == "alter":
        if entry_id not in data["alters"]:
            return False, "Unknown alter."
        gallery = data["alter_profiles"].setdefault(entry_id, legacy_profile()).setdefault("gallery", [])
    else:
        if entry_id not in data["locations"]:
            return False, "Unknown location."
        gallery = data["location_galleries"].setdefault(entry_id, [])
    if image_url not in gallery:
        return False, "That image was not found in the gallery."
    gallery.remove(image_url)
    save_data(storage, data)
    return True, "Removed gallery image."


def search_entries(data, kind, query, user_level=4):
    query = query.strip().casefold()
    matches = []
    for entry_id, name in visible_entries(data, kind, user_level):
        haystacks = [name.casefold()]
        if kind == "alter":
            haystacks.extend(alias.casefold() for alias in data["alter_profiles"].get(entry_id, {}).get("aliases", []))
        if not query or any(query in item for item in haystacks):
            matches.append((entry_id, name))
    return sorted(matches, key=lambda item: item[1].casefold())


def format_entry(entry_id, entries):
    return f'{entries.get(entry_id, "<unknown>")} ({entry_id})'


def relation_view_label(relation, viewer_id):
    label = relation["tag"] if relation["source_id"] == viewer_id else relation["reverse_tag"]
    legacy = relation.get("legacy_relation")
    if label == "NONE" and legacy:
        return f"NONE [legacy: {legacy}]"
    return label


def relation_pair_label(relation):
    pair = relation["tag"] if relation["tag"] == relation["reverse_tag"] else f'{relation["tag"]}/{relation["reverse_tag"]}'
    legacy = relation.get("legacy_relation")
    if pair == "NONE" and legacy:
        return f"NONE [legacy: {legacy}]"
    return pair


def format_aliases(profile):
    aliases = profile.get("aliases", [])
    return ", ".join(aliases) if aliases else LEGACY_VALUE


def birthday_summary(profile):
    month = profile.get("birthday_month")
    year = profile.get("birthday_year")
    if month is None or year in (None, ""):
        return LEGACY_VALUE
    return f"{MONTH_OPTIONS[month - 1]} {year}"


def format_status_entries(entries):
    if not entries:
        return LEGACY_VALUE
    return "; ".join(f'{item["value"]} ({item["status"]})' for item in entries)


def format_affiliation_entries(data, entries):
    if not entries:
        return LEGACY_VALUE
    return "; ".join(
        f'{data["affiliations"].get(item["value"], item["value"])} ({item["value"]}) [{item["status"]}]'
        for item in entries
    )


def format_age(profile):
    value = profile.get("age", LEGACY_VALUE)
    return str(value) if isinstance(value, int) else value


def build_dashboard_context(data, user_level=4):
    alters = sorted(visible_entries(data, "alter", user_level), key=lambda item: item[1].casefold())
    locations = sorted(visible_entries(data, "location", user_level), key=lambda item: item[1].casefold())
    affiliations = sorted(visible_entries(data, "affiliation", user_level), key=lambda item: item[1].casefold())
    relations = sorted(
        [
            item for item in data["relations"]
            if entry_is_accessible(data, "alter", item["source_id"], user_level)
            and entry_is_accessible(data, "alter", item["target_id"], user_level)
        ],
        key=lambda item: (
            data["alters"].get(item["source_id"], "").casefold(),
            item["tag"].casefold(),
            data["alters"].get(item["target_id"], "").casefold(),
        ),
    )
    return {"alters": alters, "locations": locations, "affiliations": affiliations, "relations": relations, "tags": get_available_relation_tags(data)}


def build_alter_view(data, alter_id, user_level=4):
    if not entry_is_accessible(data, "alter", alter_id, user_level):
        return None
    profile = data["alter_profiles"].get(alter_id, legacy_profile())
    relations = []
    for relation in data["relations"]:
        if alter_id in (relation["source_id"], relation["target_id"]):
            other_id = relation["target_id"] if relation["source_id"] == alter_id else relation["source_id"]
            if not entry_is_accessible(data, "alter", other_id, user_level):
                continue
            relations.append({"other_id": other_id, "other_name": data["alters"].get(other_id, "<unknown>"), "label": relation_view_label(relation, alter_id)})
    bulk_rows = []
    for other_id, name in sorted(visible_entries(data, "alter", user_level), key=lambda item: item[1].casefold()):
        if other_id == alter_id:
            continue
        current_tag = ""
        for relation in relations_between(data, alter_id, other_id):
            current_tag = relation["tag"] if relation["source_id"] == alter_id else relation["reverse_tag"]
        bulk_rows.append({"id": other_id, "name": name, "current_tag": current_tag or "NONE"})
    visible_affiliations = sorted(visible_entries(data, "affiliation", user_level), key=lambda item: item[1].casefold())
    visible_affiliation_ids = {entry_id for entry_id, _ in visible_affiliations}
    visible_location_id = data["location_bindings"].get(alter_id, "")
    if visible_location_id and not entry_is_accessible(data, "location", visible_location_id, user_level):
        visible_location_id = ""
    return {
        "id": alter_id,
        "name": data["alters"][alter_id],
        "level": get_entry_level(data, "alters", alter_id),
        "profile": profile,
        "profile_summary": [
            ("Aliases", format_aliases(profile)),
            ("Species", profile.get("species", LEGACY_VALUE)),
            ("Age", format_age(profile)),
            ("Birthday", birthday_summary(profile)),
            ("Gender", profile.get("gender", LEGACY_VALUE)),
            ("Reproductive Organ", profile.get("reproductive_organ", LEGACY_VALUE)),
            ("Sexual/Romantic Attraction", profile.get("sexual_romantic_attraction", LEGACY_VALUE)),
            ("Relationship Style", profile.get("relationship_style", LEGACY_VALUE)),
            ("Height", profile.get("height", LEGACY_VALUE)),
            ("Occupations/Roles", format_status_entries(profile.get("occupations", []))),
            ("Affiliations", format_affiliation_entries(data, [item for item in profile.get("affiliations", []) if item["value"] in visible_affiliation_ids])),
        ],
        "location_id": visible_location_id,
        "location_name": data["locations"].get(visible_location_id, "Restricted" if data["location_bindings"].get(alter_id) else "None"),
        "relations": sorted(relations, key=lambda item: (item["label"].casefold(), item["other_name"].casefold())),
        "relation_tags": get_available_relation_tags(data),
        "affiliations": visible_affiliations,
        "bulk_rows": bulk_rows,
        "gallery": list(profile.get("gallery", [])),
    }


def build_location_view(data, location_id, user_level=4):
    if not entry_is_accessible(data, "location", location_id, user_level):
        return None
    bound = [
        {"id": alter_id, "name": data["alters"][alter_id]}
        for alter_id, bound_location in data["location_bindings"].items()
        if bound_location == location_id and alter_id in data["alters"] and entry_is_accessible(data, "alter", alter_id, user_level)
    ]
    return {
        "id": location_id,
        "name": data["locations"][location_id],
        "level": get_entry_level(data, "locations", location_id),
        "bound_alters": sorted(bound, key=lambda item: item["name"].casefold()),
        "gallery": list(data["location_galleries"].get(location_id, [])),
    }


def build_affiliation_view(data, affiliation_id, user_level=4):
    if not entry_is_accessible(data, "affiliation", affiliation_id, user_level):
        return None
    current_members = []
    former_members = []
    for alter_id, profile in data["alter_profiles"].items():
        for item in profile.get("affiliations", []):
            if item["value"] == affiliation_id and alter_id in data["alters"] and entry_is_accessible(data, "alter", alter_id, user_level):
                payload = {"id": alter_id, "name": data["alters"][alter_id]}
                if item["status"] == "Formerly":
                    former_members.append(payload)
                else:
                    current_members.append(payload)
    return {
        "id": affiliation_id,
        "name": data["affiliations"][affiliation_id],
        "level": get_entry_level(data, "affiliations", affiliation_id),
        "current_members": sorted(current_members, key=lambda item: item["name"].casefold()),
        "former_members": sorted(former_members, key=lambda item: item["name"].casefold()),
    }


def save_uploaded_json(storage, target_name, raw_bytes):
    try:
        payload = json.loads(raw_bytes.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return False, "Uploaded file is not valid JSON."
    storage.write_json(target_name, payload)
    if target_name == DATA_FILE:
        load_data(storage)
    return True, f"Imported {target_name}."


def migrate_storage_data(source_storage, destination_storage):
    destination_storage.write_json(DATA_FILE, source_storage.read_json(DATA_FILE, tracker_default()))
    destination_storage.write_json(HASH_FILE, source_storage.read_json(HASH_FILE, hashes_default()))
    destination_storage.write_json(USER_FILE, source_storage.read_json(USER_FILE, users_default()))
    ensure_storage_files(destination_storage)
    return True, "Migrated tracker, hash, and user data."
