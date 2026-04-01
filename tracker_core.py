import json
import mimetypes
import os
import secrets
import shutil
import string
import urllib.parse
import urllib.request
import random
from base64 import urlsafe_b64encode
from datetime import date, datetime, timezone
from hashlib import sha256
from pathlib import Path

try:
    import boto3
    from botocore.config import Config
    from botocore.exceptions import BotoCoreError, ClientError
except ImportError:  # pragma: no cover
    boto3 = None
    Config = None
    BotoCoreError = Exception
    ClientError = Exception

try:
    from cryptography.fernet import Fernet, InvalidToken
except ImportError:  # pragma: no cover
    Fernet = None
    InvalidToken = Exception


DATA_FILE = "relationship_data.json"
HASH_FILE = "saved_hashes.json"
USER_FILE = "users.json"
AFFILIATION_PREFIX_FILE = "affiliation_prefixes.json"
STORAGE_SETTINGS_FILE = "storage_settings.json"
SITE_SETTINGS_FILE = "site_settings.json"
DEFAULT_RELATION_TAGS = ["NONE", "Sibling", "Friend", "Partner", "Spouse"]
DEFAULT_SPECIAL_RELATION_TAGS = {"Parent": "Child", "Child": "Parent"}
DEFAULT_ALTER_PREFIXES = ["UFA-"]
DEFAULT_AFFILIATION_PREFIXES = ["AFF-"]
LOCATION_PREFIX = "LOC-"
DOCUMENT_PREFIX = "DOC-"
WHEEL_PREFIX = "WHL-"
LEGACY_VALUE = "LEGACY"
STATUS_OPTIONS = ["Current", "Formerly", "Independent"]
ROLE_OPTIONS = ["user", "mod", "admin"]
GENDER_OPTIONS = ["Cisgender", "Transgender", "Nonbinary"]
ORGAN_OPTIONS = ["Penis", "Vagina", "Mixed", "Varies"]
RELATIONSHIP_STYLE_OPTIONS = ["Polyamorous", "Monogamous", "Harem (Center)", "Harem (Member)"]
MONTH_OPTIONS = [
    "January", "February", "March", "April", "May", "June",
    "July", "August", "September", "October", "November", "December",
]
HASH_LENGTH = 24
ALPHABET = string.ascii_letters + string.digits


class StorageError(RuntimeError):
    pass


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
        try:
            with path.open("w", encoding="utf-8") as file:
                json.dump(data, file, indent=2)
        except OSError as exc:
            raise StorageError(f"Unable to save {name}.") from exc

    def read_bytes(self, name):
        path = self.root / name
        if not path.exists():
            return None
        try:
            return path.read_bytes()
        except OSError:
            return None

    def write_bytes(self, name, payload):
        path = self.root / name
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(payload)
        except OSError as exc:
            raise StorageError(f"Unable to save {name}.") from exc

    def delete_bytes(self, name):
        path = self.root / name
        try:
            if path.exists():
                path.unlink()
        except OSError:
            return False
        return True

    def iter_files(self, prefix):
        root = self.root / prefix
        if not root.exists():
            return []
        return [str(path.relative_to(self.root)).replace("\\", "/") for path in root.rglob("*") if path.is_file()]

    def get_download_url(self, name, expires_in=300):
        return None


class S3Storage:
    def __init__(self, bucket, prefix="", endpoint="", region="", access_key="", secret_key="", path_style=False):
        if boto3 is None:
            raise RuntimeError("boto3 is required for S3 storage.")
        self.bucket = bucket
        self.prefix = prefix.strip("/")
        client_kwargs = {}
        if endpoint:
            client_kwargs["endpoint_url"] = endpoint
        if region and region.lower() != "auto":
            client_kwargs["region_name"] = region
        if access_key:
            client_kwargs["aws_access_key_id"] = access_key
        if secret_key:
            client_kwargs["aws_secret_access_key"] = secret_key
        if path_style and Config is not None:
            client_kwargs["config"] = Config(s3={"addressing_style": "path"})
        self.client = boto3.client("s3", **client_kwargs)

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
        try:
            self.client.put_object(Bucket=self.bucket, Key=self._key(name), Body=body, ContentType="application/json")
        except (ClientError, BotoCoreError) as exc:
            raise StorageError(f"Unable to save {name} to S3.") from exc

    def read_bytes(self, name):
        try:
            response = self.client.get_object(Bucket=self.bucket, Key=self._key(name))
            return response["Body"].read()
        except ClientError:
            return None

    def write_bytes(self, name, payload):
        content_type = mimetypes.guess_type(name)[0] or "application/octet-stream"
        try:
            self.client.put_object(Bucket=self.bucket, Key=self._key(name), Body=payload, ContentType=content_type)
        except (ClientError, BotoCoreError) as exc:
            raise StorageError(f"Unable to save {name} to S3.") from exc

    def delete_bytes(self, name):
        try:
            self.client.delete_object(Bucket=self.bucket, Key=self._key(name))
        except ClientError:
            return False
        return True

    def iter_files(self, prefix):
        paginator = self.client.get_paginator("list_objects_v2")
        keys = []
        for page in paginator.paginate(Bucket=self.bucket, Prefix=self._key(prefix)):
            for item in page.get("Contents", []):
                key = item["Key"]
                if self.prefix:
                    key = key[len(self.prefix) + 1 :]
                keys.append(key)
        return keys

    def get_download_url(self, name, expires_in=300):
        try:
            return self.client.generate_presigned_url(
                "get_object",
                Params={"Bucket": self.bucket, "Key": self._key(name)},
                ExpiresIn=expires_in,
            )
        except (ClientError, BotoCoreError):
            return None


def get_storage(root):
    settings = load_storage_settings(root)
    backend = os.getenv("STORAGE_BACKEND", settings.get("backend", "local")).lower()
    if backend == "s3":
        endpoint = os.getenv("S3_ENDPOINT", settings.get("s3_endpoint", "")).strip()
        bucket = os.getenv("S3_BUCKET", settings.get("s3_bucket", "")).strip()
        region = os.getenv("S3_REGION", settings.get("s3_region", "auto")).strip()
        access_key = os.getenv("S3_ACCESS_KEY", settings.get("s3_access_key", "")).strip()
        secret_key = os.getenv("S3_SECRET_KEY", settings.get("s3_secret_key", "")).strip()
        prefix = os.getenv("S3_PREFIX", settings.get("s3_prefix", ""))
        path_style = os.getenv("S3_PATH_STYLE", str(settings.get("s3_path_style", False))).strip().lower() in {"1", "true", "yes", "on"}
        if not bucket:
            raise RuntimeError("S3_BUCKET is required when STORAGE_BACKEND=s3.")
        return S3Storage(bucket, prefix, endpoint, region, access_key, secret_key, path_style)
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
        "documents": {},
        "wheels": {},
        "relations": [],
        "location_bindings": {},
        "last_modified": {"alters": {}, "locations": {}, "affiliations": {}, "documents": {}, "wheels": {}},
        "relation_tags": list(DEFAULT_RELATION_TAGS),
        "special_relation_tags": dict(DEFAULT_SPECIAL_RELATION_TAGS),
        "alter_prefixes": list(DEFAULT_ALTER_PREFIXES),
        "affiliation_prefixes": list(DEFAULT_AFFILIATION_PREFIXES),
        "alter_profiles": {},
        "location_galleries": {},
        "document_records": {},
        "affiliation_records": {},
        "wheel_records": {},
    }


def users_default():
    return {"users": []}


def hashes_default():
    return {"hashes": []}


def storage_settings_default():
    return {
        "backend": "local",
        "s3_endpoint": "",
        "s3_bucket": "",
        "s3_region": "auto",
        "s3_access_key_encrypted": "",
        "s3_secret_key_encrypted": "",
        "s3_prefix": "",
        "s3_path_style": False,
    }


def site_settings_default():
    return {
        "site_name": "United Front Technical Database",
        "favicon_name": "",
    }


def get_settings_fernet():
    secret = os.getenv("STORAGE_SETTINGS_KEY") or os.getenv("SECRET_KEY", "change-me-for-production")
    if Fernet is None:
        raise RuntimeError("cryptography is required for secure storage settings.")
    key = urlsafe_b64encode(sha256(secret.encode("utf-8")).digest())
    return Fernet(key)


def encrypt_storage_value(value):
    value = str(value or "").strip()
    if not value:
        return ""
    return get_settings_fernet().encrypt(value.encode("utf-8")).decode("utf-8")


def decrypt_storage_value(value):
    value = str(value or "").strip()
    if not value:
        return ""
    try:
        return get_settings_fernet().decrypt(value.encode("utf-8")).decode("utf-8")
    except InvalidToken as error:
        raise RuntimeError("Stored S3 credentials could not be decrypted. Check SECRET_KEY or STORAGE_SETTINGS_KEY.") from error


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
    settings["s3_path_style"] = bool(settings.get("s3_path_style"))
    settings["s3_access_key"] = decrypt_storage_value(settings.get("s3_access_key_encrypted", ""))
    settings["s3_secret_key"] = decrypt_storage_value(settings.get("s3_secret_key_encrypted", ""))
    return settings


def save_storage_settings(root, settings):
    path = Path(root) / STORAGE_SETTINGS_FILE
    payload = storage_settings_default()
    payload.update({key: settings.get(key, value) for key, value in payload.items()})
    payload["s3_path_style"] = bool(settings.get("s3_path_style", False))
    payload["s3_access_key_encrypted"] = encrypt_storage_value(settings.get("s3_access_key", ""))
    payload["s3_secret_key_encrypted"] = encrypt_storage_value(settings.get("s3_secret_key", ""))
    payload.pop("s3_access_key", None)
    payload.pop("s3_secret_key", None)
    with path.open("w", encoding="utf-8") as file:
        json.dump(payload, file, indent=2)


def load_site_settings(root):
    path = Path(root) / SITE_SETTINGS_FILE
    if not path.exists():
        return site_settings_default()
    try:
        with path.open("r", encoding="utf-8") as file:
            data = json.load(file)
    except (json.JSONDecodeError, OSError):
        return site_settings_default()
    settings = site_settings_default()
    settings.update({key: data.get(key, value) for key, value in settings.items()})
    return settings


def save_site_settings(root, settings):
    path = Path(root) / SITE_SETTINGS_FILE
    payload = site_settings_default()
    payload.update({key: settings.get(key, value) for key, value in payload.items()})
    with path.open("w", encoding="utf-8") as file:
        json.dump(payload, file, indent=2)


def migrate_legacy_local_files(source_root, destination_root):
    source_root = Path(source_root)
    destination_root = Path(destination_root)
    destination_root.mkdir(parents=True, exist_ok=True)
    filenames = [DATA_FILE, HASH_FILE, USER_FILE, STORAGE_SETTINGS_FILE, SITE_SETTINGS_FILE]
    for filename in filenames:
        source_path = source_root / filename
        destination_path = destination_root / filename
        if source_path.exists() and not destination_path.exists():
            shutil.move(str(source_path), str(destination_path))


def media_storage_name(kind, entry_id, filename):
    return f"media/{kind}/{entry_id}/{filename}"


def is_managed_media_url(value):
    return str(value or "").startswith("/media/")


def media_name_from_url(value):
    value = str(value or "")
    if not is_managed_media_url(value):
        return None
    return value.removeprefix("/media/")


def managed_media_url(kind, entry_id, filename):
    return f"/media/{kind}/{entry_id}/{filename}"


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
        "birthday_day": None,
        "birthday_last_processed_year": None,
        "gender": LEGACY_VALUE,
        "pronouns": LEGACY_VALUE,
        "reproductive_organ": LEGACY_VALUE,
        "sexual_romantic_attraction": LEGACY_VALUE,
        "relationship_style": LEGACY_VALUE,
        "height": LEGACY_VALUE,
        "occupations": [],
        "affiliations": [],
        "memory_tree": {"pre_systemhood": [], "current": []},
        "notes": [],
        "gallery": [],
        "gallery_locked": False,
        "profile_locked": False,
        "relations_locked": False,
    }


def legacy_document():
    return {
        "format": "markdown",
        "content": "",
        "tags": [],
        "ties": {"alters": [], "locations": [], "affiliations": [], "documents": []},
        "document_locked": False,
    }


def legacy_affiliation():
    return {
        "summary": "",
        "timeline": [],
        "gallery": [],
        "gallery_locked": False,
    }


def legacy_wheel():
    return {
        "entries": [],
        "permissions": {"view": [], "edit": []},
        "options": {
            "entry_deletion": 1,
            "stop_repeat_entry": 0,
            "repeat_exceptions": [],
        },
        "used_entries": [],
    }


def load_users(storage):
    data = storage.read_json(USER_FILE, users_default())
    data.setdefault("users", [])
    changed = False
    admin_assigned = False
    for user in data["users"]:
        role = str(user.get("role", "user")).strip().lower() or "user"
        if role == "admin":
            if admin_assigned:
                role = "mod"
                changed = True
            else:
                admin_assigned = True
        elif role not in {"mod", "user"}:
            role = "user"
            changed = True
        user["role"] = role
        if "level" in user:
            user.pop("level", None)
            changed = True
        if "creation_permission" not in user:
            user["creation_permission"] = role in {"admin", "mod"}
            changed = True
        if "memory_tree_permission" not in user:
            user["memory_tree_permission"] = role == "admin"
            changed = True
        if "profile_permission" not in user:
            user["profile_permission"] = role == "admin"
            changed = True
        if "relation_permission" not in user:
            user["relation_permission"] = role == "admin"
            changed = True
        if "document_permission" not in user:
            user["document_permission"] = role == "admin"
            changed = True
        if "locked_gallery_permission" not in user:
            user["locked_gallery_permission"] = user.get("gallery_permission", role == "admin")
            changed = True
        if "gallery_permission" in user:
            user.pop("gallery_permission", None)
            changed = True
        user.setdefault("active", True)
        if user["role"] == "admin":
            user["memory_tree_permission"] = True
            user["profile_permission"] = True
            user["relation_permission"] = True
            user["document_permission"] = True
            user["locked_gallery_permission"] = True
        if user["role"] == "admin" and not user["creation_permission"]:
            user["creation_permission"] = True
            changed = True
    if data["users"] and not admin_assigned:
        data["users"][0]["role"] = "admin"
        data["users"][0]["creation_permission"] = True
        data["users"][0]["memory_tree_permission"] = True
        data["users"][0]["profile_permission"] = True
        data["users"][0]["relation_permission"] = True
        data["users"][0]["document_permission"] = True
        data["users"][0]["locked_gallery_permission"] = True
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


def normalize_memory_entries(entries):
    normalized = []
    for item in entries or []:
        entry_id = str(item.get("id", "")).strip() or secrets.token_hex(8)
        when = str(item.get("date", "")).strip()
        text = str(item.get("text", "")).strip()
        if when or text:
            normalized.append({"id": entry_id, "date": when, "text": text})
    return normalized


def normalize_notes(entries):
    normalized = []
    for item in entries or []:
        entry_id = str(item.get("id", "")).strip() or secrets.token_hex(8)
        text = str(item.get("text", "")).strip()
        if text:
            normalized.append({"id": entry_id, "text": text})
    return normalized


def update_profile_birthday_age(profile):
    month = profile.get("birthday_month")
    day = profile.get("birthday_day")
    age = profile.get("age")
    if month is None or day is None or not isinstance(age, int):
        return False
    today = date.today()
    last_processed = profile.get("birthday_last_processed_year")
    if (today.month, today.day) >= (month, day) and last_processed != today.year:
        profile["age"] = age + 1
        profile["birthday_last_processed_year"] = today.year
        return True
    return False


def sync_saved_hashes_with_tracker(storage, data):
    hashes = load_saved_hashes(storage)
    hashes.update(data["alters"].keys())
    hashes.update(data["locations"].keys())
    hashes.update(data["affiliations"].keys())
    hashes.update(data["documents"].keys())
    hashes.update(data["wheels"].keys())
    save_saved_hashes(storage, hashes)
    return hashes


def load_data(storage):
    data = storage.read_json(DATA_FILE, tracker_default())
    data.setdefault("alters", {})
    data.setdefault("locations", {})
    data.setdefault("affiliations", {})
    data.setdefault("documents", {})
    data.setdefault("wheels", {})
    data.setdefault("relations", [])
    data.setdefault("location_bindings", {})
    data.setdefault("last_modified", {"alters": {}, "locations": {}, "affiliations": {}, "documents": {}, "wheels": {}})
    data.setdefault("relation_tags", list(DEFAULT_RELATION_TAGS))
    data.setdefault("special_relation_tags", dict(DEFAULT_SPECIAL_RELATION_TAGS))
    data.setdefault("alter_prefixes", list(DEFAULT_ALTER_PREFIXES))
    data.setdefault("affiliation_prefixes", list(DEFAULT_AFFILIATION_PREFIXES))
    data.setdefault("alter_profiles", {})
    data.setdefault("location_galleries", {})
    data.setdefault("location_gallery_locks", {})
    data.setdefault("document_records", {})
    data.setdefault("affiliation_records", {})
    data.setdefault("wheel_records", {})
    data["last_modified"].setdefault("alters", {})
    data["last_modified"].setdefault("locations", {})
    data["last_modified"].setdefault("affiliations", {})
    data["last_modified"].setdefault("documents", {})
    data["last_modified"].setdefault("wheels", {})

    data["relation_tags"] = unique_items(
        list(DEFAULT_RELATION_TAGS)
        + data["relation_tags"]
        + list(data["special_relation_tags"].keys())
        + list(data["special_relation_tags"].values())
    )
    data["alter_prefixes"] = unique_items(list(DEFAULT_ALTER_PREFIXES) + data["alter_prefixes"])
    data["affiliation_prefixes"] = unique_items(data["affiliation_prefixes"])

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

    data.pop("entry_levels", None)
    for alter_id in data["alters"]:
        if alter_id not in data["last_modified"]["alters"]:
            data["last_modified"]["alters"][alter_id] = ""
            changed = True
        profile = data["alter_profiles"].setdefault(alter_id, legacy_profile())
        for key, default_value in legacy_profile().items():
            if key not in profile:
                profile[key] = default_value
                changed = True
        if profile.get("birthday_day") is None:
            legacy_day = profile.get("birthday_year")
            if isinstance(legacy_day, int) and 1 <= legacy_day <= 31:
                profile["birthday_day"] = legacy_day
            else:
                profile["birthday_day"] = None
            changed = True
        if "birthday_year" in profile:
            profile.pop("birthday_year", None)
            changed = True
        profile["occupations"] = normalize_status_entries(profile.get("occupations"))
        profile["affiliations"] = normalize_status_entries(profile.get("affiliations"))
        memory_tree = profile.get("memory_tree", {})
        profile["memory_tree"] = {
            "pre_systemhood": normalize_memory_entries(memory_tree.get("pre_systemhood", [])),
            "current": normalize_memory_entries(memory_tree.get("current", [])),
        }
        profile["notes"] = normalize_notes(profile.get("notes", []))
        profile["gallery"] = [item for item in profile.get("gallery", []) if str(item).strip()]
        profile["gallery_locked"] = bool(profile.get("gallery_locked", False))
        profile["profile_locked"] = bool(profile.get("profile_locked", False))
        profile["relations_locked"] = bool(profile.get("relations_locked", False))
        if update_profile_birthday_age(profile):
            changed = True

    for location_id in data["locations"]:
        if location_id not in data["last_modified"]["locations"]:
            data["last_modified"]["locations"][location_id] = ""
            changed = True
        if location_id not in data["location_galleries"]:
            data["location_galleries"][location_id] = []
            changed = True
        else:
            data["location_galleries"][location_id] = [item for item in data["location_galleries"].get(location_id, []) if str(item).strip()]
        if location_id not in data["location_gallery_locks"]:
            data["location_gallery_locks"][location_id] = False
            changed = True
        else:
            data["location_gallery_locks"][location_id] = bool(data["location_gallery_locks"].get(location_id, False))

    for affiliation_id in data["affiliations"]:
        if affiliation_id not in data["last_modified"]["affiliations"]:
            data["last_modified"]["affiliations"][affiliation_id] = ""
            changed = True
        record = data["affiliation_records"].setdefault(affiliation_id, legacy_affiliation())
        for key, default_value in legacy_affiliation().items():
            if key not in record:
                record[key] = default_value
                changed = True
        record["timeline"] = normalize_memory_entries(record.get("timeline", []))
        record["gallery"] = [item for item in record.get("gallery", []) if str(item).strip()]
        record["gallery_locked"] = bool(record.get("gallery_locked", False))

    for document_id in data["documents"]:
        if document_id not in data["last_modified"]["documents"]:
            data["last_modified"]["documents"][document_id] = ""
            changed = True
        record = data["document_records"].setdefault(document_id, legacy_document())
        for key, default_value in legacy_document().items():
            if key not in record:
                record[key] = default_value
                changed = True
        record["format"] = "html" if str(record.get("format", "markdown")).strip().lower() == "html" else "markdown"
        record["content"] = str(record.get("content", ""))
        record["tags"] = [item.strip() for item in record.get("tags", []) if str(item).strip()]
        ties = record.get("ties", {})
        normalized_ties = {
            "alters": [item for item in ties.get("alters", []) if item in data["alters"]],
            "locations": [item for item in ties.get("locations", []) if item in data["locations"]],
            "affiliations": [item for item in ties.get("affiliations", []) if item in data["affiliations"]],
            "documents": [item for item in ties.get("documents", []) if item in data["documents"] and item != document_id],
        }
        if normalized_ties != ties:
            changed = True
        record["ties"] = normalized_ties
        record["document_locked"] = bool(record.get("document_locked", False))

    for wheel_id in data["wheels"]:
        if wheel_id not in data["last_modified"]["wheels"]:
            data["last_modified"]["wheels"][wheel_id] = ""
            changed = True
        record = data["wheel_records"].setdefault(wheel_id, legacy_wheel())
        permissions = record.get("permissions", {})
        record["permissions"] = {
            "view": [str(item) for item in permissions.get("view", []) if str(item).strip()],
            "edit": [str(item) for item in permissions.get("edit", []) if str(item).strip()],
        }
        options = record.get("options", {})
        record["options"] = {
            "entry_deletion": 2 if int(options.get("entry_deletion", 1)) == 2 else (0 if int(options.get("entry_deletion", 1)) == 0 else 1),
            "stop_repeat_entry": 1 if int(options.get("stop_repeat_entry", 0)) == 1 else 0,
            "repeat_exceptions": [str(item).strip() for item in options.get("repeat_exceptions", []) if str(item).strip()],
        }
        record["used_entries"] = [str(item) for item in record.get("used_entries", []) if str(item).strip()]
        normalized_entries = []
        for entry in record.get("entries", []):
            entry_id = str(entry.get("id", "")).strip() or secrets.token_hex(8)
            kind = "image" if str(entry.get("kind", "text")).strip().lower() == "image" else "text"
            text = str(entry.get("text", "") or "")
            media_url = str(entry.get("media_url", "") or "")
            label = str(entry.get("label", "") or "")
            if kind == "text" and not text.strip():
                continue
            if kind == "image" and not media_url.strip():
                continue
            normalized_entries.append({"id": entry_id, "kind": kind, "text": text, "media_url": media_url, "label": label})
        record["entries"] = normalized_entries

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


def touch_entry(data, bucket_name, entry_id):
    data.setdefault("last_modified", {"alters": {}, "locations": {}, "affiliations": {}})
    data["last_modified"].setdefault(bucket_name, {})
    data["last_modified"][bucket_name][entry_id] = datetime.now(timezone.utc).isoformat()


def parse_timestamp(value):
    if not value:
        return datetime.fromtimestamp(0, tz=timezone.utc)
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return datetime.fromtimestamp(0, tz=timezone.utc)


def create_entry(storage, bucket_name, entity_label, name, entry_id):
    return create_entry_with_level(storage, bucket_name, entity_label, name, entry_id, None)


def create_entry_with_level(storage, bucket_name, entity_label, name, entry_id, level):
    data = load_data(storage)
    name = name.strip()
    entry_id = entry_id.strip()
    if not name:
        return False, f"{entity_label.title()} name is required."
    if entry_id in data[bucket_name]:
        return False, f'ID "{entry_id}" already exists.'
    data[bucket_name][entry_id] = name
    touch_entry(data, bucket_name, entry_id)
    if bucket_name == "alters":
        data["alter_profiles"].setdefault(entry_id, legacy_profile())
    elif bucket_name == "locations":
        data["location_galleries"].setdefault(entry_id, [])
        data.setdefault("location_gallery_locks", {})[entry_id] = False
    elif bucket_name == "documents":
        data["document_records"].setdefault(entry_id, legacy_document())
    elif bucket_name == "affiliations":
        data["affiliation_records"].setdefault(entry_id, legacy_affiliation())
    elif bucket_name == "wheels":
        try:
            default_wheel = legacy_wheel()
        except NameError:
            default_wheel = {
                "entries": [],
                "permissions": {"view": [], "edit": []},
                "options": {"entry_deletion": 1, "stop_repeat_entry": 0, "repeat_exceptions": []},
                "used_entries": [],
            }
        data["wheel_records"].setdefault(entry_id, default_wheel)
    save_data(storage, data)
    sync_saved_hashes_with_tracker(storage, data)
    return True, f"Created {entity_label}: {name} ({entry_id})"


def user_can_create(user):
    return bool(user and (user.get("role") == "admin" or user.get("creation_permission")))


def user_can_view_memory(user):
    return bool(user and (user.get("role") == "admin" or user.get("memory_tree_permission")))


def user_can_view_profile(user):
    return bool(user and (user.get("role") == "admin" or user.get("profile_permission")))


def user_can_view_relations(user):
    return bool(user and (user.get("role") == "admin" or user.get("relation_permission")))


def user_can_view_documents(user):
    return bool(user and (user.get("role") == "admin" or user.get("document_permission")))


def user_can_view_gallery(user):
    return bool(user)


def user_can_view_locked_gallery(user):
    return bool(user and (user.get("role") == "admin" or user.get("locked_gallery_permission")))


def is_gallery_locked(data, kind, entry_id):
    if kind == "alter":
        return bool(data["alter_profiles"].get(entry_id, {}).get("gallery_locked", False))
    if kind == "location":
        return bool(data.get("location_gallery_locks", {}).get(entry_id, False))
    if kind == "affiliation":
        return bool(data["affiliation_records"].get(entry_id, {}).get("gallery_locked", False))
    return False


def can_view_gallery_for_entry(data, user, kind, entry_id):
    if not user:
        return False
    if not is_gallery_locked(data, kind, entry_id):
        return True
    return user_can_view_locked_gallery(user)


def is_profile_locked(data, alter_id):
    return bool(data["alter_profiles"].get(alter_id, {}).get("profile_locked", False))


def is_relations_locked(data, alter_id):
    return bool(data["alter_profiles"].get(alter_id, {}).get("relations_locked", False))


def can_view_profile_for_entry(data, user, alter_id):
    if not user:
        return False
    if not is_profile_locked(data, alter_id):
        return True
    return user_can_view_profile(user)


def can_view_relations_for_entry(data, user, alter_id):
    if not user:
        return False
    if not is_relations_locked(data, alter_id):
        return True
    return user_can_view_relations(user)


def is_document_locked(data, document_id):
    return bool(data["document_records"].get(document_id, {}).get("document_locked", False))


def can_view_document_for_entry(data, user, document_id):
    if not user:
        return False
    if not is_document_locked(data, document_id):
        return True
    return user_can_view_documents(user)


def user_can_view_wheel(wheel_record, user):
    if not user or not wheel_record:
        return False
    if user.get("role") == "admin":
        return True
    user_id = str(user.get("id", "")).strip()
    permissions = wheel_record.get("permissions", {})
    return user_id in permissions.get("view", []) or user_id in permissions.get("edit", [])


def user_can_edit_wheel(wheel_record, user):
    if not user or not wheel_record:
        return False
    if user.get("role") == "admin":
        return True
    user_id = str(user.get("id", "")).strip()
    return user_id in wheel_record.get("permissions", {}).get("edit", [])


def entry_is_accessible(data, kind, entry_id, user_level=None):
    bucket_map = {"alter": "alters", "location": "locations", "affiliation": "affiliations", "document": "documents", "wheel": "wheels"}
    bucket_name = bucket_map[kind]
    return entry_id in data[bucket_name]


def visible_entries(data, kind, user_level=None):
    bucket_map = {"alter": "alters", "location": "locations", "affiliation": "affiliations", "document": "documents", "wheel": "wheels"}
    bucket_name = bucket_map[kind]
    return [
        (entry_id, name)
        for entry_id, name in data[bucket_name].items()
        if entry_is_accessible(data, kind, entry_id, user_level)
    ]


def resolve_entry_reference(data, kind, raw_value, user_level=4):
    raw_value = str(raw_value or "").strip()
    if not raw_value:
        return ""
    if entry_is_accessible(data, kind, raw_value, user_level):
        return raw_value
    match = raw_value.rsplit("(", 1)
    if len(match) == 2 and match[1].endswith(")"):
        candidate_id = match[1][:-1].strip()
        if entry_is_accessible(data, kind, candidate_id, user_level):
            return candidate_id
    visible = visible_entries(data, kind, user_level)
    exact_name_matches = [entry_id for entry_id, name in visible if name.casefold() == raw_value.casefold()]
    if len(exact_name_matches) == 1:
        return exact_name_matches[0]
    partial_matches = [entry_id for entry_id, name in visible if raw_value.casefold() in name.casefold()]
    if len(partial_matches) == 1:
        return partial_matches[0]
    return raw_value


def rename_entry(storage, bucket_name, entity_label, entry_id, name):
    data = load_data(storage)
    if entry_id not in data[bucket_name]:
        return False, f'Unknown {entity_label} ID "{entry_id}".'
    name = name.strip()
    if not name:
        return False, f"{entity_label.title()} name is required."
    data[bucket_name][entry_id] = name
    touch_entry(data, bucket_name, entry_id)
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
    touch_entry(data, "alters", alter_id)
    touch_entry(data, "locations", location_id)
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
    birthday_day = form.get("birthday_day", "").strip()
    if birthday_month and birthday_day:
        if birthday_month not in MONTH_OPTIONS or not birthday_day.isdigit():
            return False, "Birthday month/day is invalid."
        month = MONTH_OPTIONS.index(birthday_month) + 1
        day = int(birthday_day)
        if day < 1 or day > 31:
            return False, "Birthday day must be between 1 and 31."
        profile["birthday_month"] = month
        profile["birthday_day"] = day
        today = date.today()
        profile["birthday_last_processed_year"] = today.year if (today.month, today.day) >= (month, day) else today.year - 1
    else:
        profile["birthday_month"] = None
        profile["birthday_day"] = None
        profile["birthday_last_processed_year"] = None
    profile["gender"] = form.get("gender", "") or LEGACY_VALUE
    profile["pronouns"] = form.get("pronouns", "").strip() or LEGACY_VALUE
    profile["reproductive_organ"] = form.get("reproductive_organ", "") or LEGACY_VALUE
    profile["sexual_romantic_attraction"] = form.get("attraction", "").strip() or LEGACY_VALUE
    profile["relationship_style"] = form.get("relationship_style", "") or LEGACY_VALUE
    profile["height"] = form.get("height", "").strip() or LEGACY_VALUE
    touch_entry(data, "alters", alter_id)
    save_data(storage, data)
    return True, "Saved alter profile."


def update_affiliation_membership(storage, alter_id, affiliation_id, status):
    data = load_data(storage)
    if alter_id not in data["alters"] or affiliation_id not in data["affiliations"]:
        return False, "Alter and affiliation must exist."
    status = status if status in STATUS_OPTIONS else STATUS_OPTIONS[0]
    profile = data["alter_profiles"].setdefault(alter_id, legacy_profile())
    profile["affiliations"] = [item for item in profile["affiliations"] if item["value"] != affiliation_id]
    profile["affiliations"].append({"value": affiliation_id, "status": status})
    touch_entry(data, "alters", alter_id)
    touch_entry(data, "affiliations", affiliation_id)
    save_data(storage, data)
    return True, "Updated affiliation membership."


def remove_affiliation_membership(storage, alter_id, affiliation_id):
    data = load_data(storage)
    profile = data["alter_profiles"].setdefault(alter_id, legacy_profile())
    before = len(profile["affiliations"])
    profile["affiliations"] = [item for item in profile["affiliations"] if item["value"] != affiliation_id]
    if len(profile["affiliations"]) == before:
        return False, "That affiliation is not assigned."
    touch_entry(data, "alters", alter_id)
    touch_entry(data, "affiliations", affiliation_id)
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
    touch_entry(data, "alters", alter_id)
    save_data(storage, data)
    return True, "Added occupation/role."


def remove_occupation_entry(storage, alter_id, occupation):
    data = load_data(storage)
    profile = data["alter_profiles"].setdefault(alter_id, legacy_profile())
    before = len(profile["occupations"])
    profile["occupations"] = [item for item in profile["occupations"] if item["value"] != occupation]
    if len(profile["occupations"]) == before:
        return False, "That occupation/role was not found."
    touch_entry(data, "alters", alter_id)
    save_data(storage, data)
    return True, "Removed occupation/role."


def update_memory_tree(storage, alter_id, era, date_text, note_text, order_ids=None):
    data = load_data(storage)
    if alter_id not in data["alters"]:
        return False, "Unknown alter."
    if era not in {"pre_systemhood", "current"}:
        return False, "Unknown memory section."
    profile = data["alter_profiles"].setdefault(alter_id, legacy_profile())
    memory_tree = profile.setdefault("memory_tree", {"pre_systemhood": [], "current": []})
    entries = normalize_memory_entries(memory_tree.get(era, []))
    date_text = str(date_text or "").strip()
    note_text = str(note_text or "").strip()
    if date_text or note_text:
        entries.append({"id": secrets.token_hex(8), "date": date_text, "text": note_text})
    if order_ids:
        order_lookup = {item["id"]: index for index, item in enumerate(order_ids)}
        entries.sort(key=lambda item: (order_lookup.get(item["id"], len(order_lookup)), item["id"]))
    memory_tree[era] = entries
    touch_entry(data, "alters", alter_id)
    save_data(storage, data)
    return True, "Updated memory tree."


def remove_memory_entry(storage, alter_id, era, entry_id):
    data = load_data(storage)
    if alter_id not in data["alters"]:
        return False, "Unknown alter."
    if era not in {"pre_systemhood", "current"}:
        return False, "Unknown memory section."
    profile = data["alter_profiles"].setdefault(alter_id, legacy_profile())
    memory_tree = profile.setdefault("memory_tree", {"pre_systemhood": [], "current": []})
    entries = normalize_memory_entries(memory_tree.get(era, []))
    updated = [item for item in entries if item["id"] != entry_id]
    if len(updated) == len(entries):
        return False, "That memory entry was not found."
    memory_tree[era] = updated
    touch_entry(data, "alters", alter_id)
    save_data(storage, data)
    return True, "Removed memory entry."


def update_notes(storage, alter_id, note_text, order_ids=None):
    data = load_data(storage)
    if alter_id not in data["alters"]:
        return False, "Unknown alter."
    profile = data["alter_profiles"].setdefault(alter_id, legacy_profile())
    notes = normalize_notes(profile.get("notes", []))
    note_text = str(note_text or "").strip()
    if note_text:
        notes.append({"id": secrets.token_hex(8), "text": note_text})
    if order_ids:
        order_lookup = {item["id"]: index for index, item in enumerate(order_ids)}
        notes.sort(key=lambda item: (order_lookup.get(item["id"], len(order_lookup)), item["id"]))
    profile["notes"] = notes
    touch_entry(data, "alters", alter_id)
    save_data(storage, data)
    return True, "Updated notes."


def remove_note_entry(storage, alter_id, entry_id):
    data = load_data(storage)
    if alter_id not in data["alters"]:
        return False, "Unknown alter."
    profile = data["alter_profiles"].setdefault(alter_id, legacy_profile())
    notes = normalize_notes(profile.get("notes", []))
    updated = [item for item in notes if item["id"] != entry_id]
    if len(updated) == len(notes):
        return False, "That note was not found."
    profile["notes"] = updated
    touch_entry(data, "alters", alter_id)
    save_data(storage, data)
    return True, "Removed note."


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
    touch_entry(data, "alters", first_id)
    touch_entry(data, "alters", second_id)
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
    touch_entry(data, "alters", first_id)
    touch_entry(data, "alters", second_id)
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
    elif kind == "location":
        if entry_id not in data["locations"]:
            return False, "Unknown location."
        gallery = data["location_galleries"].setdefault(entry_id, [])
    else:
        if entry_id not in data["affiliations"]:
            return False, "Unknown affiliation."
        gallery = data["affiliation_records"].setdefault(entry_id, legacy_affiliation()).setdefault("gallery", [])
    if image_url in gallery:
        return False, "That image is already in the gallery."
    gallery.append(image_url)
    bucket = "alters" if kind == "alter" else "locations" if kind == "location" else "affiliations"
    touch_entry(data, bucket, entry_id)
    save_data(storage, data)
    return True, "Added gallery image."


def set_gallery_locked(storage, kind, entry_id, locked):
    data = load_data(storage)
    locked = bool(locked)
    if kind == "alter":
        if entry_id not in data["alters"]:
            return False, "Unknown alter."
        data["alter_profiles"].setdefault(entry_id, legacy_profile())["gallery_locked"] = locked
        touch_entry(data, "alters", entry_id)
    elif kind == "location":
        if entry_id not in data["locations"]:
            return False, "Unknown location."
        data.setdefault("location_gallery_locks", {})[entry_id] = locked
        touch_entry(data, "locations", entry_id)
    else:
        if entry_id not in data["affiliations"]:
            return False, "Unknown affiliation."
        data["affiliation_records"].setdefault(entry_id, legacy_affiliation())["gallery_locked"] = locked
        touch_entry(data, "affiliations", entry_id)
    save_data(storage, data)
    return True, "Gallery lock updated."


def set_alter_section_lock(storage, alter_id, section, locked):
    data = load_data(storage)
    if alter_id not in data["alters"]:
        return False, "Unknown alter."
    profile = data["alter_profiles"].setdefault(alter_id, legacy_profile())
    if section == "profile":
        profile["profile_locked"] = bool(locked)
    elif section == "relations":
        profile["relations_locked"] = bool(locked)
    else:
        return False, "Unknown lock section."
    touch_entry(data, "alters", alter_id)
    save_data(storage, data)
    return True, "Section lock updated."


def remove_gallery_item(storage, kind, entry_id, image_url):
    data = load_data(storage)
    if kind == "alter":
        if entry_id not in data["alters"]:
            return False, "Unknown alter."
        gallery = data["alter_profiles"].setdefault(entry_id, legacy_profile()).setdefault("gallery", [])
    elif kind == "location":
        if entry_id not in data["locations"]:
            return False, "Unknown location."
        gallery = data["location_galleries"].setdefault(entry_id, [])
    else:
        if entry_id not in data["affiliations"]:
            return False, "Unknown affiliation."
        gallery = data["affiliation_records"].setdefault(entry_id, legacy_affiliation()).setdefault("gallery", [])
    if image_url not in gallery:
        return False, "That image was not found in the gallery."
    gallery.remove(image_url)
    bucket = "alters" if kind == "alter" else "locations" if kind == "location" else "affiliations"
    touch_entry(data, bucket, entry_id)
    save_data(storage, data)
    return True, "Removed gallery image."


def parse_document_tags(raw_value):
    return [item.strip() for item in str(raw_value or "").split(",") if item.strip()]


def resolve_document_ties(data, form):
    return {
        "alters": unique_items(
            filter(
                None,
                [
                    resolve_entry_reference(data, "alter", item, 0)
                    for item in str(form.get("tie_alters", "")).split(",")
                ],
            )
        ),
        "locations": unique_items(
            filter(
                None,
                [
                    resolve_entry_reference(data, "location", item, 0)
                    for item in str(form.get("tie_locations", "")).split(",")
                ],
            )
        ),
        "affiliations": unique_items(
            filter(
                None,
                [
                    resolve_entry_reference(data, "affiliation", item, 0)
                    for item in str(form.get("tie_affiliations", "")).split(",")
                ],
            )
        ),
        "documents": unique_items(
            filter(
                None,
                [
                    resolve_entry_reference(data, "document", item, 0)
                    for item in str(form.get("tie_documents", "")).split(",")
                ],
            )
        ),
    }


def save_document_record(storage, document_id, form):
    data = load_data(storage)
    if document_id not in data["documents"]:
        return False, "Unknown document."
    record = data["document_records"].setdefault(document_id, legacy_document())
    record["format"] = "html" if str(form.get("document_format", "markdown")).strip().lower() == "html" else "markdown"
    record["content"] = str(form.get("content", "") or "")
    record["tags"] = parse_document_tags(form.get("tags", ""))
    ties = resolve_document_ties(data, form)
    ties["documents"] = [item for item in ties["documents"] if item != document_id]
    record["ties"] = ties
    touch_entry(data, "documents", document_id)
    save_data(storage, data)
    return True, "Saved document."


def set_document_locked(storage, document_id, locked):
    data = load_data(storage)
    if document_id not in data["documents"]:
        return False, "Unknown document."
    data["document_records"].setdefault(document_id, legacy_document())["document_locked"] = bool(locked)
    touch_entry(data, "documents", document_id)
    save_data(storage, data)
    return True, "Document lock updated."


def save_wheel_settings(storage, wheel_id, options):
    data = load_data(storage)
    if wheel_id not in data["wheels"]:
        return False, "Unknown wheel."
    record = data["wheel_records"].setdefault(wheel_id, legacy_wheel())
    try:
        entry_deletion = int(options.get("entry_deletion", 1))
    except Exception:
        entry_deletion = 1
    if entry_deletion not in {0, 1, 2}:
        entry_deletion = 1
    try:
        stop_repeat = int(options.get("stop_repeat_entry", 0))
    except Exception:
        stop_repeat = 0
    stop_repeat = 1 if stop_repeat == 1 else 0
    record["options"] = {
        "entry_deletion": entry_deletion,
        "stop_repeat_entry": stop_repeat,
        "repeat_exceptions": [item.strip() for item in options.get("repeat_exceptions", []) if str(item).strip()],
    }
    touch_entry(data, "wheels", wheel_id)
    save_data(storage, data)
    return True, "Saved wheel settings."


def save_wheel_permissions(storage, wheel_id, view_ids, edit_ids):
    data = load_data(storage)
    if wheel_id not in data["wheels"]:
        return False, "Unknown wheel."
    edit_ids = unique_items([str(item).strip() for item in edit_ids if str(item).strip()])
    view_ids = unique_items([str(item).strip() for item in view_ids if str(item).strip()] + edit_ids)
    data["wheel_records"].setdefault(wheel_id, legacy_wheel())["permissions"] = {"view": view_ids, "edit": edit_ids}
    touch_entry(data, "wheels", wheel_id)
    save_data(storage, data)
    return True, "Saved wheel permissions."


def add_wheel_text_entries(storage, wheel_id, texts):
    data = load_data(storage)
    if wheel_id not in data["wheels"]:
        return False, "Unknown wheel."
    record = data["wheel_records"].setdefault(wheel_id, legacy_wheel())
    added = 0
    for text in texts:
        text = str(text or "").strip()
        if not text or text.startswith("#"):
            continue
        record["entries"].append({"id": secrets.token_hex(8), "kind": "text", "text": text, "media_url": "", "label": ""})
        added += 1
    if not added:
        return False, "No usable text entries were found."
    touch_entry(data, "wheels", wheel_id)
    save_data(storage, data)
    return True, f"Added {added} text entr{'y' if added == 1 else 'ies'}."


def add_wheel_image_entry(storage, wheel_id, filename, payload):
    data = load_data(storage)
    if wheel_id not in data["wheels"]:
        return False, "Unknown wheel."
    suffix = Path(filename or "").suffix.lower() or ".bin"
    stored_name = f"{secrets.token_hex(12)}{suffix}"
    media_name = media_storage_name("wheel", wheel_id, stored_name)
    storage.write_bytes(media_name, payload)
    media_url = managed_media_url("wheel", wheel_id, stored_name)
    record = data["wheel_records"].setdefault(wheel_id, legacy_wheel())
    record["entries"].append({"id": secrets.token_hex(8), "kind": "image", "text": "", "media_url": media_url, "label": Path(filename or stored_name).name})
    touch_entry(data, "wheels", wheel_id)
    save_data(storage, data)
    return True, "Added image entry."


def remove_wheel_entry(storage, wheel_id, entry_id):
    data = load_data(storage)
    if wheel_id not in data["wheels"]:
        return False, "Unknown wheel."
    record = data["wheel_records"].setdefault(wheel_id, legacy_wheel())
    entries = record.get("entries", [])
    target = next((item for item in entries if item.get("id") == entry_id), None)
    if not target:
        return False, "That wheel entry was not found."
    entries.remove(target)
    record["used_entries"] = [item for item in record.get("used_entries", []) if item != entry_id]
    touch_entry(data, "wheels", wheel_id)
    save_data(storage, data)
    return True, target


def clear_wheel_used_entries(storage, wheel_id):
    data = load_data(storage)
    if wheel_id not in data["wheels"]:
        return False, "Unknown wheel."
    data["wheel_records"].setdefault(wheel_id, legacy_wheel())["used_entries"] = []
    touch_entry(data, "wheels", wheel_id)
    save_data(storage, data)
    return True, "Cleared wheel repeat cache."


def spin_wheel(storage, wheel_id):
    data = load_data(storage)
    if wheel_id not in data["wheels"]:
        return False, "Unknown wheel.", None
    record = data["wheel_records"].setdefault(wheel_id, legacy_wheel())
    entries = list(record.get("entries", []))
    if not entries:
        return False, "This wheel has no entries.", None
    options = record.get("options", {})
    usable = list(entries)
    if int(options.get("stop_repeat_entry", 0)) == 1:
        used_ids = set(record.get("used_entries", []))
        repeat_exceptions = {item.strip() for item in options.get("repeat_exceptions", []) if str(item).strip()}
        filtered = []
        for entry in usable:
            if entry["kind"] == "text" and entry.get("text", "").strip() in repeat_exceptions:
                filtered.append(entry)
                continue
            if entry["id"] in used_ids:
                continue
            filtered.append(entry)
        if filtered:
            usable = filtered
        else:
            record["used_entries"] = []
            usable = list(entries)
    selected = random.choice(usable)
    if int(options.get("stop_repeat_entry", 0)) == 1 and selected["id"] not in record["used_entries"]:
        record["used_entries"].append(selected["id"])
    deleted = False
    if int(options.get("entry_deletion", 1)) == 2:
        target = next((item for item in record["entries"] if item.get("id") == selected["id"]), None)
        if target:
            record["entries"].remove(target)
            record["used_entries"] = [item for item in record.get("used_entries", []) if item != selected["id"]]
            deleted = True
    touch_entry(data, "wheels", wheel_id)
    save_data(storage, data)
    return True, "Selected wheel entry.", {"entry": selected, "deleted": deleted, "options": options}


def save_affiliation_summary(storage, affiliation_id, summary):
    data = load_data(storage)
    if affiliation_id not in data["affiliations"]:
        return False, "Unknown affiliation."
    record = data["affiliation_records"].setdefault(affiliation_id, legacy_affiliation())
    record["summary"] = str(summary or "").strip()
    touch_entry(data, "affiliations", affiliation_id)
    save_data(storage, data)
    return True, "Saved affiliation summary."


def update_affiliation_timeline(storage, affiliation_id, date_text, note_text, order_ids=None):
    data = load_data(storage)
    if affiliation_id not in data["affiliations"]:
        return False, "Unknown affiliation."
    record = data["affiliation_records"].setdefault(affiliation_id, legacy_affiliation())
    entries = normalize_memory_entries(record.get("timeline", []))
    date_text = str(date_text or "").strip()
    note_text = str(note_text or "").strip()
    if date_text or note_text:
        entries.append({"id": secrets.token_hex(8), "date": date_text, "text": note_text})
    if order_ids:
        order_lookup = {item["id"]: index for index, item in enumerate(order_ids)}
        entries.sort(key=lambda item: (order_lookup.get(item["id"], len(order_lookup)), item["id"]))
    record["timeline"] = entries
    touch_entry(data, "affiliations", affiliation_id)
    save_data(storage, data)
    return True, "Updated affiliation timeline."


def remove_affiliation_timeline_entry(storage, affiliation_id, entry_id):
    data = load_data(storage)
    if affiliation_id not in data["affiliations"]:
        return False, "Unknown affiliation."
    record = data["affiliation_records"].setdefault(affiliation_id, legacy_affiliation())
    timeline = normalize_memory_entries(record.get("timeline", []))
    updated = [item for item in timeline if item["id"] != entry_id]
    if len(updated) == len(timeline):
        return False, "That timeline entry was not found."
    record["timeline"] = updated
    touch_entry(data, "affiliations", affiliation_id)
    save_data(storage, data)
    return True, "Removed affiliation timeline entry."


def delete_entry(storage, kind, entry_id):
    data = load_data(storage)
    bucket_map = {"alter": "alters", "location": "locations", "affiliation": "affiliations", "document": "documents", "wheel": "wheels"}
    if kind not in bucket_map:
        return False, "Unknown entry type."
    bucket_name = bucket_map[kind]
    if entry_id not in data[bucket_name]:
        return False, "That entry was not found."

    if kind == "alter":
        data["alters"].pop(entry_id, None)
        data["alter_profiles"].pop(entry_id, None)
        data["location_bindings"].pop(entry_id, None)
        data["relations"] = [
            relation for relation in data["relations"]
            if relation["source_id"] != entry_id and relation["target_id"] != entry_id
        ]
        data["last_modified"]["alters"].pop(entry_id, None)
    elif kind == "location":
        data["locations"].pop(entry_id, None)
        data["location_galleries"].pop(entry_id, None)
        data.get("location_gallery_locks", {}).pop(entry_id, None)
        data["location_bindings"] = {
            alter_id: location_id
            for alter_id, location_id in data["location_bindings"].items()
            if location_id != entry_id
        }
        data["last_modified"]["locations"].pop(entry_id, None)
    else:
        if kind == "affiliation":
            data["affiliations"].pop(entry_id, None)
            data["affiliation_records"].pop(entry_id, None)
            for profile in data["alter_profiles"].values():
                profile["affiliations"] = [item for item in profile.get("affiliations", []) if item["value"] != entry_id]
            data["last_modified"]["affiliations"].pop(entry_id, None)
        else:
            if kind == "document":
                data["documents"].pop(entry_id, None)
                data["document_records"].pop(entry_id, None)
                for record in data["document_records"].values():
                    ties = record.get("ties", {})
                    ties["documents"] = [item for item in ties.get("documents", []) if item != entry_id]
                data["last_modified"]["documents"].pop(entry_id, None)
            else:
                data["wheels"].pop(entry_id, None)
                data["wheel_records"].pop(entry_id, None)
                data["last_modified"]["wheels"].pop(entry_id, None)

    save_data(storage, data)
    return True, f"Removed {kind} entry."


def store_gallery_media_bytes(storage, kind, entry_id, filename, payload):
    storage.write_bytes(media_storage_name(kind, entry_id, filename), payload)
    return managed_media_url(kind, entry_id, filename)


def import_gallery_media_from_url(storage, kind, entry_id, source_url):
    source_url = str(source_url or "").strip()
    if not source_url:
        return False, "An image URL is required."
    try:
        with urllib.request.urlopen(source_url) as response:
            payload = response.read()
            content_type = response.headers.get_content_type()
    except Exception as error:
        return False, f"Failed to fetch image URL: {error}"
    suffix = mimetypes.guess_extension(content_type or "") or Path(urllib.parse.urlparse(source_url).path).suffix.lower() or ".bin"
    filename = f"{secrets.token_hex(12)}{suffix}"
    return True, store_gallery_media_bytes(storage, kind, entry_id, filename, payload)


def migrate_gallery_media(storage, data_root):
    if isinstance(storage, LocalStorage):
        return False
    data = load_data(storage)
    changed = False
    local_storage = LocalStorage(data_root)
    gallery_sets = []
    for alter_id, profile in data["alter_profiles"].items():
        gallery_sets.append(("alter", alter_id, profile.get("gallery", [])))
    for location_id, gallery in data.get("location_galleries", {}).items():
        gallery_sets.append(("location", location_id, gallery))
    for affiliation_id, record in data.get("affiliation_records", {}).items():
        gallery_sets.append(("affiliation", affiliation_id, record.get("gallery", [])))
    for kind, entry_id, gallery in gallery_sets:
        updated = []
        for item in gallery:
            if is_managed_media_url(item):
                media_name = media_name_from_url(item)
                payload = local_storage.read_bytes(media_name)
                if payload is not None and storage.read_bytes(media_name) is None:
                    storage.write_bytes(media_name, payload)
                    local_storage.delete_bytes(media_name)
                updated.append(item)
                continue
            success, managed_url = import_gallery_media_from_url(storage, kind, entry_id, item)
            if success:
                updated.append(managed_url)
                changed = True
            else:
                updated.append(item)
        if kind == "alter":
            data["alter_profiles"].setdefault(entry_id, legacy_profile())["gallery"] = updated
        elif kind == "location":
            data["location_galleries"][entry_id] = updated
        else:
            data["affiliation_records"].setdefault(entry_id, legacy_affiliation())["gallery"] = updated
    if changed:
        save_data(storage, data)
    return changed


def search_entries(data, kind, query, user_level=4):
    query = query.strip().casefold()
    matches = []
    for entry_id, name in visible_entries(data, kind, user_level):
        haystacks = [name.casefold()]
        if kind == "alter":
            haystacks.extend(alias.casefold() for alias in data["alter_profiles"].get(entry_id, {}).get("aliases", []))
        elif kind == "document":
            record = data["document_records"].get(entry_id, legacy_document())
            haystacks.extend(tag.casefold() for tag in record.get("tags", []))
            haystacks.append(str(record.get("content", "")).casefold())
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
    day = profile.get("birthday_day")
    if month is None or day in (None, ""):
        return LEGACY_VALUE
    return f"{MONTH_OPTIONS[month - 1]} {day}"


def format_status_entries(entries):
    if not entries:
        return LEGACY_VALUE
    return "; ".join(f'{item["value"]} ({item["status"]})' for item in entries)


def format_affiliation_entries(data, entries):
    if not entries:
        return LEGACY_VALUE
    return "; ".join(
        f'{data["affiliations"].get(item["value"], item["value"])} [{item["status"]}]'
        for item in entries
    )


def build_affiliation_links(data, entries):
    linked = []
    for item in entries:
        affiliation_id = item["value"]
        linked.append(
            {
                "id": affiliation_id,
                "name": data["affiliations"].get(affiliation_id, affiliation_id),
                "status": item["status"],
                "is_linkable": True,
            }
        )
    return linked


def format_age(profile):
    value = profile.get("age", LEGACY_VALUE)
    return str(value) if isinstance(value, int) else value


def build_recent_changes(data, user_level=4, limit=12):
    recent = []
    for kind, bucket_name, label in (("alter", "alters", "Alter"), ("location", "locations", "Location"), ("affiliation", "affiliations", "Affiliation"), ("document", "documents", "Document")):
        for entry_id, name in data[bucket_name].items():
            if not entry_is_accessible(data, kind, entry_id, user_level):
                continue
            recent.append(
                {
                    "kind": kind,
                    "label": label,
                    "id": entry_id,
                    "name": name,
                    "timestamp": data.get("last_modified", {}).get(bucket_name, {}).get(entry_id, ""),
                }
            )
    recent.sort(key=lambda item: parse_timestamp(item["timestamp"]), reverse=True)
    return recent[:limit]


def build_dashboard_context(data, user_level=4):
    alters = sorted(visible_entries(data, "alter", user_level), key=lambda item: item[1].casefold())
    locations = sorted(visible_entries(data, "location", user_level), key=lambda item: item[1].casefold())
    affiliations = sorted(visible_entries(data, "affiliation", user_level), key=lambda item: item[1].casefold())
    documents = sorted(visible_entries(data, "document", user_level), key=lambda item: item[1].casefold())
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
    return {
        "alters": alters,
        "locations": locations,
        "affiliations": affiliations,
        "documents": documents,
        "relations": relations,
        "tags": get_available_relation_tags(data),
        "recent_changes": build_recent_changes(data, user_level),
    }


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
    visible_locations = sorted(visible_entries(data, "location", user_level), key=lambda item: item[1].casefold())
    visible_location_id = data["location_bindings"].get(alter_id, "")
    if visible_location_id and not entry_is_accessible(data, "location", visible_location_id, user_level):
        visible_location_id = ""
    visible_profile_affiliations = [item for item in profile.get("affiliations", []) if item["value"] in visible_affiliation_ids]
    return {
        "id": alter_id,
        "name": data["alters"][alter_id],
        "profile": profile,
        "profile_summary": [
            ("Aliases", format_aliases(profile)),
            ("Species", profile.get("species", LEGACY_VALUE)),
            ("Age", format_age(profile)),
            ("Birthday", birthday_summary(profile)),
            ("Gender", profile.get("gender", LEGACY_VALUE)),
            ("Pronouns", profile.get("pronouns", LEGACY_VALUE)),
            ("Reproductive Organ", profile.get("reproductive_organ", LEGACY_VALUE)),
            ("Sexual/Romantic Attraction", profile.get("sexual_romantic_attraction", LEGACY_VALUE)),
            ("Relationship Style", profile.get("relationship_style", LEGACY_VALUE)),
            ("Height", profile.get("height", LEGACY_VALUE)),
            ("Occupations/Roles", format_status_entries(profile.get("occupations", []))),
        ],
        "profile_affiliations": build_affiliation_links(data, visible_profile_affiliations),
        "location_id": visible_location_id,
        "location_name": data["locations"].get(visible_location_id, "Restricted" if data["location_bindings"].get(alter_id) else "None"),
        "relations": sorted(relations, key=lambda item: (item["label"].casefold(), item["other_name"].casefold())),
        "relation_tags": get_available_relation_tags(data),
        "affiliations": visible_affiliations,
        "locations": visible_locations,
        "bulk_rows": bulk_rows,
        "memory_tree": profile.get("memory_tree", {"pre_systemhood": [], "current": []}),
        "notes": profile.get("notes", []),
        "gallery": list(profile.get("gallery", [])),
        "gallery_locked": bool(profile.get("gallery_locked", False)),
        "profile_locked": bool(profile.get("profile_locked", False)),
        "relations_locked": bool(profile.get("relations_locked", False)),
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
        "bound_alters": sorted(bound, key=lambda item: item["name"].casefold()),
        "gallery": list(data["location_galleries"].get(location_id, [])),
        "gallery_locked": bool(data.get("location_gallery_locks", {}).get(location_id, False)),
    }


def build_affiliation_view(data, affiliation_id, user_level=4):
    if not entry_is_accessible(data, "affiliation", affiliation_id, user_level):
        return None
    record = data["affiliation_records"].get(affiliation_id, legacy_affiliation())
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
        "summary": record.get("summary", ""),
        "timeline": record.get("timeline", []),
        "gallery": list(record.get("gallery", [])),
        "gallery_locked": bool(record.get("gallery_locked", False)),
        "current_members": sorted(current_members, key=lambda item: item["name"].casefold()),
        "former_members": sorted(former_members, key=lambda item: item["name"].casefold()),
    }


def build_document_view(data, document_id, user_level=4):
    if not entry_is_accessible(data, "document", document_id, user_level):
        return None
    record = data["document_records"].get(document_id, legacy_document())
    ties = record.get("ties", {})
    return {
        "id": document_id,
        "name": data["documents"][document_id],
        "record": record,
        "tags_text": ", ".join(record.get("tags", [])),
        "ties": {
            "alters": [{"id": item, "name": data["alters"][item]} for item in ties.get("alters", []) if item in data["alters"]],
            "locations": [{"id": item, "name": data["locations"][item]} for item in ties.get("locations", []) if item in data["locations"]],
            "affiliations": [{"id": item, "name": data["affiliations"][item]} for item in ties.get("affiliations", []) if item in data["affiliations"]],
            "documents": [{"id": item, "name": data["documents"][item]} for item in ties.get("documents", []) if item in data["documents"]],
        },
        "tie_inputs": {
            "alters": ", ".join(ties.get("alters", [])),
            "locations": ", ".join(ties.get("locations", [])),
            "affiliations": ", ".join(ties.get("affiliations", [])),
            "documents": ", ".join(ties.get("documents", [])),
        },
        "document_locked": bool(record.get("document_locked", False)),
        "alters": sorted(visible_entries(data, "alter", user_level), key=lambda item: item[1].casefold()),
        "locations": sorted(visible_entries(data, "location", user_level), key=lambda item: item[1].casefold()),
        "affiliations": sorted(visible_entries(data, "affiliation", user_level), key=lambda item: item[1].casefold()),
        "documents": sorted([(entry_id, name) for entry_id, name in visible_entries(data, "document", user_level) if entry_id != document_id], key=lambda item: item[1].casefold()),
    }


def build_wheels_context(data, user):
    wheels = []
    for wheel_id, name in sorted(data["wheels"].items(), key=lambda item: item[1].casefold()):
        record = data["wheel_records"].get(wheel_id, legacy_wheel())
        if not user_can_view_wheel(record, user):
            continue
        wheels.append(
            {
                "id": wheel_id,
                "name": name,
                "can_edit": user_can_edit_wheel(record, user),
                "entry_count": len(record.get("entries", [])),
            }
        )
    return wheels


def build_wheel_view(data, wheel_id, user, users_data):
    if wheel_id not in data["wheels"]:
        return None
    record = data["wheel_records"].get(wheel_id, legacy_wheel())
    if not user_can_view_wheel(record, user):
        return None
    entries = []
    for entry in record.get("entries", []):
        entries.append(
            {
                "id": entry["id"],
                "kind": entry["kind"],
                "display": entry.get("text", "") if entry["kind"] == "text" else f"Image | {entry.get('label') or 'uploaded file'}",
                "text": entry.get("text", ""),
                "media_url": entry.get("media_url", ""),
                "label": entry.get("label", ""),
            }
        )
    user_rows = []
    for row in users_data.get("users", []):
        if row.get("role") == "admin":
            continue
        user_rows.append(
            {
                "id": row["id"],
                "username": row["username"],
                "can_view": row["id"] in record.get("permissions", {}).get("view", []) or row["id"] in record.get("permissions", {}).get("edit", []),
                "can_edit": row["id"] in record.get("permissions", {}).get("edit", []),
            }
        )
    return {
        "id": wheel_id,
        "name": data["wheels"][wheel_id],
        "entries": entries,
        "can_edit": user_can_edit_wheel(record, user),
        "options": record.get("options", legacy_wheel()["options"]),
        "permissions": record.get("permissions", legacy_wheel()["permissions"]),
        "user_rows": user_rows,
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
