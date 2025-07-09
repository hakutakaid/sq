import concurrent.futures
import time
import os
import re
import cv2
import logging
import asyncio
import asyncpg
from datetime import datetime, timedelta, timezone # <--- Import timezone
import json

# Import DATABASE_URL from config.py
from config import DATABASE_URL

logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

PUBLIC_LINK_PATTERN = re.compile(r'(https?://)?(t\.me|telegram\.me)/([^/]+)(/(\d+))?')
PRIVATE_LINK_PATTERN = re.compile(r'(https?://)?(t\.me|telegram\.me)/c/(\d+)(/(\d+))?')
VIDEO_EXTENSIONS = {"mp4", "mkv", "avi", "mov", "wmv", "flv", "webm", "mpeg", "mpg", "3gp"}


class DatabaseManager:
    def __init__(self, db_url):
        self.db_url = db_url
        self._pool = None

    async def connect(self):
        if self._pool is None:
            # Create a connection pool
            self._pool = await asyncpg.create_pool(self.db_url)
            await self._create_tables()

    async def close(self):
        if self._pool:
            await self._pool.close()
            self._pool = None
            logger.info("Database connection pool closed.")

    async def _execute(self, query, *params):
        async with self._pool.acquire() as conn:
            try:
                await conn.execute(query, *params)
            except Exception as e:
                logger.error(f"Error executing query: {query} with params {params} - {e}")
                raise

    async def _fetchrow(self, query, *params):
        async with self._pool.acquire() as conn:
            try:
                return await conn.fetchrow(query, *params)
            except Exception as e:
                logger.error(f"Error fetching one row: {query} with params {params} - {e}")
                raise

    async def _fetch(self, query, *params):
        async with self._pool.acquire() as conn:
            try:
                return await conn.fetch(query, *params)
            except Exception as e:
                logger.error(f"Error fetching all rows: {query} with params {params} - {e}")
                raise

    async def _create_tables(self):
        await self._execute('''
            CREATE TABLE IF NOT EXISTS users (
                user_id BIGINT PRIMARY KEY,
                session_string TEXT,
                bot_token TEXT,
                replacement_words JSONB DEFAULT '{}',
                delete_words JSONB DEFAULT '[]',
                chat_id TEXT,
                caption TEXT,
                rename_tag TEXT,
                updated_at TIMESTAMPTZ
            )
        ''')
        await self._execute('''
            CREATE TABLE IF NOT EXISTS premium_users (
                user_id BIGINT PRIMARY KEY,
                subscription_start TIMESTAMPTZ,
                subscription_end TIMESTAMPTZ
            )
        ''')

        await self._execute('''
            CREATE TABLE IF NOT EXISTS statistics (
                id BIGSERIAL PRIMARY KEY,
                event_type TEXT,
                timestamp TIMESTAMPTZ,
                user_id BIGINT
            )
        ''')
        await self._execute('''
            CREATE TABLE IF NOT EXISTS redeem_code (
                code TEXT PRIMARY KEY,
                duration_value INTEGER,
                duration_unit TEXT,
                used_by BIGINT,
                used_at TIMESTAMPTZ
            )
        ''')
        logger.info("Database tables initialized for asyncpg.")

    async def get_users_collection(self):
        return UsersCollection(self)

    async def get_premium_users_collection(self):
        return PremiumUsersCollection(self)

    async def get_statistics_collection(self):
        return StatisticsCollection(self)

    async def get_codedb_collection(self):
        return RedeemCodeCollection(self)

# Initialize db_manager with the DATABASE_URL from config.py
db_manager = DatabaseManager(DATABASE_URL)

class UsersCollection:
    def __init__(self, db_manager):
        self.db_manager = db_manager

    async def update_one(self, filter_query, update_query, upsert=False):
        user_id = filter_query.get("user_id")
        if not user_id:
            raise ValueError("user_id is required for update_one in users collection.")

        set_fields = update_query.get("$set", {})
        unset_fields = update_query.get("$unset", {})

        # Ensure updated_at is timezone-aware UTC
        if "updated_at" not in set_fields:
            set_fields["updated_at"] = datetime.now(timezone.utc)

        if upsert:
            insert_columns = ["user_id"]
            insert_placeholders = ["$1"]
            insert_values = [user_id]
            
            update_on_conflict_parts = []
            param_index = 2
            
            for k, v in set_fields.items():
                insert_columns.append(k)
                insert_placeholders.append(f"${param_index}")
                insert_values.append(v)
                update_on_conflict_parts.append(f"{k} = EXCLUDED.{k}")
                param_index += 1

            for k in unset_fields:
                update_on_conflict_parts.append(f"{k} = NULL")

            insert_sql = f"INSERT INTO users ({', '.join(insert_columns)}) VALUES ({', '.join(insert_placeholders)})"
            if update_on_conflict_parts:
                insert_sql += f" ON CONFLICT (user_id) DO UPDATE SET {', '.join(update_on_conflict_parts)}"
            else:
                insert_sql += " ON CONFLICT (user_id) DO NOTHING"

            await self.db_manager._execute(insert_sql, *insert_values)
        else:
            set_clauses = []
            set_values = []
            param_index = 1

            for k, v in set_fields.items():
                set_clauses.append(f"{k} = ${param_index}")
                set_values.append(v)
                param_index += 1

            unset_clauses = [f"{k} = NULL" for k in unset_fields]

            if not set_clauses and not unset_clauses:
                logger.debug(f"No fields to update for user {user_id}.")
                return

            update_parts = []
            if set_clauses:
                update_parts.append(", ".join(set_clauses))
            if unset_clauses:
                update_parts.append(", ".join(unset_clauses))

            update_sql_set_part = ", ".join(update_parts)
            
            query = f"UPDATE users SET {update_sql_set_part} WHERE user_id = ${param_index}"
            values = set_values + [user_id]
            await self.db_manager._execute(query, *values)


    async def find_one(self, filter_query):
        user_id = filter_query.get("user_id")
        if not user_id:
            return None

        row = await self.db_manager._fetchrow("SELECT * FROM users WHERE user_id = $1", user_id)
        if row:
            data = dict(row)
            data['replacement_words'] = data.get('replacement_words') or {}
            data['delete_words'] = data.get('delete_words') or []
            return data
        return None


class PremiumUsersCollection:
    def __init__(self, db_manager):
        self.db_manager = db_manager

    async def update_one(self, filter_query, update_query, upsert=False):
        user_id = filter_query.get("user_id")
        if not user_id:
            raise ValueError("user_id is required for update_one in premium_users collection.")

        set_fields = update_query.get("$set", {})

        # Ensure datetime objects are timezone-aware UTC when stored
        for field in ["subscription_start", "subscription_end"]:
            if field in set_fields and isinstance(set_fields[field], datetime):
                # Ensure it's UTC-aware. If naive, assume UTC.
                if set_fields[field].tzinfo is None:
                    set_fields[field] = set_fields[field].replace(tzinfo=timezone.utc)
                else: # Convert to UTC if it's aware but not UTC
                    set_fields[field] = set_fields[field].astimezone(timezone.utc)


        if upsert:
            insert_columns = ["user_id"]
            insert_placeholders = ["$1"]
            insert_values = [user_id]
            
            update_set_clauses = []
            param_index_for_insert = 2

            for k, v in set_fields.items():
                insert_columns.append(k)
                insert_placeholders.append(f"${param_index_for_insert}")
                insert_values.append(v)
                update_set_clauses.append(f"{k} = EXCLUDED.{k}")
                param_index_for_insert += 1

            insert_sql = f"INSERT INTO premium_users ({', '.join(insert_columns)}) VALUES ({', '.join(insert_placeholders)})"
            
            if update_set_clauses:
                insert_sql += f" ON CONFLICT (user_id) DO UPDATE SET {', '.join(update_set_clauses)}"
            else:
                insert_sql += " ON CONFLICT (user_id) DO NOTHING"

            await self.db_manager._execute(insert_sql, *insert_values)

        else:
            set_clauses = []
            set_values = []
            param_index = 1

            for k, v in set_fields.items():
                set_clauses.append(f"{k} = ${param_index}")
                set_values.append(v)
                param_index += 1

            if not set_clauses:
                logger.warning(f"No fields to update for premium user {user_id}.")
                return

            query = f"UPDATE premium_users SET {', '.join(set_clauses)} WHERE user_id = ${param_index}"
            await self.db_manager._execute(query, *(set_values + [user_id]))


    async def find_one(self, filter_query):
        user_id = filter_query.get("user_id")
        if not user_id:
            return None

        row = await self.db_manager._fetchrow("SELECT * FROM premium_users WHERE user_id = $1", user_id)
        if row:
            # asyncpg returns timezone-aware datetime objects for TIMESTAMPTZ columns
            return dict(row)
        return None

    async def create_index(self, field_name, expireAfterSeconds=None):
        logger.info(f"PostgreSQL does not support TTL indexes like MongoDB. "
                    f"Index creation for '{field_name}' with expireAfterSeconds={expireAfterSeconds} "
                    f"will not automatically delete expired records. "
                    f"You need to implement a separate cleanup mechanism for premium_users based on subscription_end.")
        pass


class StatisticsCollection:
    def __init__(self, db_manager):
        self.db_manager = db_manager

    async def insert_one(self, document):
        # Ensure timestamp is timezone-aware UTC
        timestamp = document.get('timestamp', datetime.now(timezone.utc))
        if timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=timezone.utc)
        else:
            timestamp = timestamp.astimezone(timezone.utc)

        await self.db_manager._execute(
            "INSERT INTO statistics (event_type, timestamp, user_id) VALUES ($1, $2, $3)",
            document.get('event_type'), timestamp, document.get('user_id')
        )

    async def count_documents(self, filter_query={}):
        where_clauses = []
        params = []
        param_index = 1

        if 'event_type' in filter_query:
            where_clauses.append(f"event_type = ${param_index}")
            params.append(filter_query['event_type'])
            param_index += 1

        if 'user_id' in filter_query:
            where_clauses.append(f"user_id = ${param_index}")
            params.append(filter_query['user_id'])
            param_index += 1

        where_sql = ""
        if where_clauses:
            where_sql = "WHERE " + " AND ".join(where_clauses)

        row = await self.db_manager._fetchrow(f"SELECT COUNT(*) as count FROM statistics {where_sql}", *params)
        return row['count'] if row else 0

    async def find(self, filter_query={}, sort_query=None, limit=None):
        where_clauses = []
        params = []
        param_index = 1

        if 'event_type' in filter_query:
            where_clauses.append(f"event_type = ${param_index}")
            params.append(filter_query['event_type'])
            param_index += 1

        if 'user_id' in filter_query:
            where_clauses.append(f"user_id = ${param_index}")
            params.append(filter_query['user_id'])
            param_index += 1

        where_sql = ""
        if where_clauses:
            where_sql = "WHERE " + " AND ".join(where_clauses)

        order_by_sql = ""
        if sort_query:
            sort_parts = []
            for field, order in sort_query:
                direction = "DESC" if order == -1 else "ASC"
                sort_parts.append(f"{field} {direction}")
            order_by_sql = "ORDER BY " + ", ".join(sort_parts)

        limit_sql = ""
        if limit is not None:
            limit_sql = f"LIMIT {limit}"

        query = f"SELECT * FROM statistics {where_sql} {order_by_sql} {limit_sql}"
        rows = await self.db_manager._fetch(query, *params)
        return [dict(row) for row in rows] if rows else []


class RedeemCodeCollection:
    def __init__(self, db_manager):
        self.db_manager = db_manager

    async def find_one(self, filter_query):
        code = filter_query.get("code")
        if not code:
            return None
        row = await self.db_manager._fetchrow("SELECT * FROM redeem_code WHERE code = $1", code)
        if row:
            return dict(row)
        return None

    async def insert_one(self, document):
        # Ensure used_at is timezone-aware UTC
        used_at = document.get('used_at')
        if used_at and used_at.tzinfo is None:
            used_at = used_at.replace(tzinfo=timezone.utc)
        elif used_at:
            used_at = used_at.astimezone(timezone.utc)

        await self.db_manager._execute(
            "INSERT INTO redeem_code (code, duration_value, duration_unit, used_by, used_at) VALUES ($1, $2, $3, $4, $5)",
            document.get('code'), document.get('duration_value'), document.get('duration_unit'),
            document.get('used_by'), used_at
        )

    async def update_one(self, filter_query, update_query):
        code = filter_query.get("code")
        if not code:
            raise ValueError("code is required for update_one in redeem_code collection.")

        set_fields = update_query.get("$set", {})

        # Ensure used_at is timezone-aware UTC when updated
        if "used_at" in set_fields and isinstance(set_fields["used_at"], datetime):
            if set_fields["used_at"].tzinfo is None:
                set_fields["used_at"] = set_fields["used_at"].replace(tzinfo=timezone.utc)
            else:
                set_fields["used_at"] = set_fields["used_at"].astimezone(timezone.utc)

        set_clauses = []
        set_values = []
        param_index = 1
        for k, v in set_fields.items():
            set_clauses.append(f"{k} = ${param_index}")
            set_values.append(v)
            param_index += 1

        if not set_clauses:
            logger.warning(f"No fields to update for redeem code {code}.")
            return

        query = f"UPDATE redeem_code SET {', '.join(set_clauses)} WHERE code = ${param_index}"
        await self.db_manager._execute(query, *(set_values + [code]))

    async def delete_one(self, filter_query):
        code = filter_query.get("code")
        if not code:
            raise ValueError("code is required for delete_one in redeem_code collection.")
        await self.db_manager._execute("DELETE FROM redeem_code WHERE code = $1", code)

users_collection = None
premium_users_collection = None
statistics_collection = None
codedb = None

async def init_db_collections():
    global users_collection, premium_users_collection, statistics_collection, codedb
    await db_manager.connect()
    users_collection = await db_manager.get_users_collection()
    premium_users_collection = await db_manager.get_premium_users_collection()
    statistics_collection = await db_manager.get_statistics_collection()
    codedb = await db_manager.get_codedb_collection()
    logger.info("Database collections initialized for asyncpg.")

# ------- < start > Session Encoder don't change -------

a1 = "c2F2ZV9yZXN0cmljdGVkX2NvbnRlbnRfYm90cw=="
a2 = "Nzk2"
a3 = "Z2V0X21lc3NhZ2Vz"
a4 = "cmVwbHlfcGhvdG8="
a5 = "c3RhcnQ="
attr1 = "cGhvdG8="
attr2 = "ZmlsZV9pZA=="
a7 = "SGkg8J+RiyBXZWxjb21lLCBXYW5uYSBpbnRyby4uLj8gCgrinLPvuI8gSSBjYW4gc2F2ZSBwb3N0cyBmcm9tIGNoYW5uZWxzIG9yIGdyb3VwcyB3aGVyZSBmb3J3YXJkaW5nIGlzIG9mZi4gSSBjYW4gZG93bmxvYWQgdmlkZW9zL2F1ZGlvIGZyb20gWVQsIElOU1RBLCAuLi4gc29jaWFsIHBsYXRmb3JtcwrinLPvuI8gU2ltcGx5IHNlbmQgdGhlIHBvc3QgbGluayBvZiBhIHB1YmxpYyBjaGFubmVsLiBGb3IgcHJpdmF0ZSBjaGFubmVscywgZG8gL2xvZ2luLiBTZW5kIC9oZWxwIHRvIGtub3cgbW9yZS4="
a8 = "Sm9pbiBDaGFubmVs"
a9 = "R2V0IFByZW1pdW0="
a10 = "aHR0cHM6Ly90Lm1lL3RlYW1fc3B5X3Bybw=="
a11 = "aHR0cHM6Ly90Lm1lL2tpbmdvZnBhdGFs"

# ------- < end > Session Encoder don't change --------

def is_private_link(link):
    return bool(PRIVATE_LINK_PATTERN.match(link))


def thumbnail(sender):
    # This should likely create a unique filename per screenshot if multiple are taken
    # or rely on a user-specific custom thumbnail setup.
    # If used for temporary screenshots during download, consider using tempfile module.
    return f'{sender}.jpg' if os.path.exists(f'{sender}.jpg') else None


def hhmmss(seconds):
    return time.strftime('%H:%M:%S', time.gmtime(seconds))


def E(L):
    private_match = re.match(r'https://t\.me/c/(\d+)/(?:\d+/)?(\d+)', L)
    public_match = re.match(r'https://t\.me/([^/]+)/(?:\d+/)?(\d+)', L)

    if private_match:
        return f'-100{private_match.group(1)}', int(private_match.group(2)), 'private'
    elif public_match:
        return public_match.group(1), int(public_match.group(2)), 'public'

    return None, None, None


def get_display_name(user):
    if user.first_name and user.last_name:
        return f"{user.first_name} {user.last_name}"
    elif user.first_name:
        return user.first_name
    elif user.last_name:
        return user.last_name
    elif user.username:
        return user.username
    else:
        return "Unknown User"


def sanitize_filename(filename):
    return re.sub(r'[<>:"/\\|?*]', '_', filename)


def get_dummy_filename(info):
    file_type = info.get("type", "file")
    extension = {
        "video": "mp4",
        "photo": "jpg",
        "document": "pdf",
        "audio": "mp3"
    }.get(file_type, "bin")

    return f"downloaded_file_{int(time.time())}.{extension}"


async def is_private_chat(event):
    return event.is_private


async def save_user_data(user_id, key, value):
    await users_collection.update_one(
        {"user_id": user_id},
        {"$set": {key: value}},
        upsert=True
    )


async def get_user_data_key(user_id, key, default=None):
    user_data = await users_collection.find_one({"user_id": int(user_id)})
    return user_data.get(key, default) if user_data else default


async def get_user_data(user_id):
    try:
        user_data = await users_collection.find_one({"user_id": user_id})
        return user_data
    except Exception as e:
        logger.error(f"Error retrieving user data for {user_id}: {e}")
        return None


async def save_user_session(user_id, session_string):
    try:
        await users_collection.update_one(
            {"user_id": user_id},
            {"$set": {
                "session_string": session_string,
                "updated_at": datetime.now(timezone.utc) # Ensure UTC
            }},
            upsert=True
        )
        logger.info(f"Saved session for user {user_id}")
        return True
    except Exception as e:
        logger.error(f"Error saving session for user {user_id}: {e}")
        return False


async def remove_user_session(user_id):
    try:
        await users_collection.update_one(
            {"user_id": user_id},
            {"$unset": {"session_string": ""}}
        )
        logger.info(f"Removed session for user {user_id}")
        return True
    except Exception as e:
        logger.error(f"Error removing session for user {user_id}: {e}")
        return False


async def save_user_bot(user_id, bot_token):
    try:
        await users_collection.update_one(
            {"user_id": user_id},
            {"$set": {
                "bot_token": bot_token,
                "updated_at": datetime.now(timezone.utc) # Ensure UTC
            }},
            upsert=True
        )
        logger.info(f"Saved bot token for user {user_id}")
        return True
    except Exception as e:
        logger.error(f"Error saving bot token for user {user_id}: {e}")
        return False


async def remove_user_bot(user_id):
    try:
        await users_collection.update_one(
            {"user_id": user_id},
            {"$unset": {"bot_token": ""}}
        )
        logger.info(f"Removed bot token for user {user_id}")
        return True
    except Exception as e:
        logger.error(f"Error removing bot token for user {user_id}: {e}")
        return False


async def process_text_with_rules(user_id, text):
    if not text:
        return ""

    try:
        replacements = await get_user_data_key(user_id, "replacement_words", {})
        delete_words = await get_user_data_key(user_id, "delete_words", [])

        processed_text = text
        for word, replacement in replacements.items():
            # Use re.sub with word boundaries to replace whole words only
            processed_text = re.sub(r'\b' + re.escape(word) + r'\b', replacement, processed_text, flags=re.IGNORECASE)

        if delete_words:
            # Create a regex pattern to delete multiple words efficiently
            # Escaping words ensures special characters are treated literally
            # \b for word boundaries
            delete_pattern = r'\b(?:' + '|'.join(re.escape(w) for w in delete_words) + r')\b'
            processed_text = re.sub(delete_pattern, '', processed_text, flags=re.IGNORECASE).strip()
            # Remove extra spaces left by deletion
            processed_text = re.sub(r'\s+', ' ', processed_text).strip()


        return processed_text
    except Exception as e:
        logger.error(f"Error processing text with rules: {e}")
        return text


async def screenshot(video: str, duration: int, sender: str) -> str | None:
    output_file = f"{sender}_{int(time.time())}.jpg"

    time_stamp = hhmmss(duration // 2)
    
    cmd = [
        "ffmpeg",
        "-ss", time_stamp,
        "-i", video,
        "-frames:v", "1",
        output_file,
        "-y"
    ]

    process = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE
    )

    stdout, stderr = await process.communicate()

    if os.path.isfile(output_file):
        return output_file
    else:
        print(f"FFmpeg Error: {stderr.decode().strip()}")
        return None


async def get_video_metadata(file_path):
    default_values = {'width': 1, 'height': 1, 'duration': 1}
    loop = asyncio.get_event_loop()
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=4)

    try:
        def _extract_metadata():
            try:
                vcap = cv2.VideoCapture(file_path)
                if not vcap.isOpened():
                    return default_values

                width = round(vcap.get(cv2.CAP_PROP_FRAME_WIDTH))
                height = round(vcap.get(cv2.CAP_PROP_FRAME_HEIGHT))
                fps = vcap.get(cv2.CAP_PROP_FPS)
                frame_count = vcap.get(cv2.CAP_PROP_FRAME_COUNT)

                if fps <= 0:
                    return default_values

                duration = round(frame_count / fps)
                if duration <= 0:
                    return default_values

                vcap.release()
                return {'width': width, 'height': height, 'duration': duration}
            except Exception as e:
                logger.error(f"Error in _extract_metadata (cv2): {e}")
                return default_values

        return await loop.run_in_executor(executor, _extract_metadata)

    except Exception as e:
        logger.error(f"Error in get_video_metadata overall: {e}")
        return default_values


async def add_premium_user(user_id, duration_value, duration_unit):
    try:
        # Always use UTC for consistency
        now = datetime.now(timezone.utc)
        expiry_date = None

        if duration_unit == "min":
            expiry_date = now + timedelta(minutes=duration_value)
        elif duration_unit == "hours":
            expiry_date = now + timedelta(hours=duration_value)
        elif duration_unit == "days":
            expiry_date = now + timedelta(days=duration_value)
        elif duration_unit == "weeks":
            expiry_date = now + timedelta(weeks=duration_value)
        elif duration_unit == "month":
            expiry_date = now + timedelta(days=30 * duration_value) # Approximation for a month
        elif duration_unit == "year":
            expiry_date = now + timedelta(days=365 * duration_value)
        elif duration_unit == "decades":
            expiry_date = now + timedelta(days=3650 * duration_value)
        else:
            return False, "Invalid duration unit"

        update_fields = {
            "subscription_start": now,
            "subscription_end": expiry_date,
        }

        await premium_users_collection.update_one(
            {"user_id": user_id},
            {"$set": update_fields},
            upsert=True
        )

        return True, expiry_date
    except Exception as e:
        logger.error(f"Error adding premium user {user_id}: {e}")
        return False, str(e)


async def is_premium_user(user_id):
    try:
        user = await premium_users_collection.find_one({"user_id": user_id})
        if user and "subscription_end" in user and user["subscription_end"] is not None:
            subscription_end = user["subscription_end"] # This will be timezone-aware from DB
            now = datetime.now(timezone.utc) # <--- Make 'now' timezone-aware UTC
            return now < subscription_end
        return False
    except Exception as e:
        logger.error(f"Error checking premium status for {user_id}: {e}")
        return False


async def get_premium_details(user_id):
    try:
        user = await premium_users_collection.find_one({"user_id": user_id})
        return user
    except Exception as e:
        logger.error(f"Error getting premium details for {user_id}: {e}")
        return None