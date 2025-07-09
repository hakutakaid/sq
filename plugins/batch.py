import os, re, time, asyncio, json
from pyrogram import Client, filters
from pyrogram.types import Message, ReplyParameters # <--- Import ReplyParameters
from pyrogram.errors import UserNotParticipant
from config import API_ID, API_HASH, LOG_GROUP, STRING, FORCE_SUB, FREEMIUM_LIMIT, PREMIUM_LIMIT
from utils.func import get_user_data, screenshot, thumbnail, get_video_metadata
from utils.func import get_user_data_key, process_text_with_rules, is_premium_user, E
from shared_client import app as X
from plugins.settings import rename_file
from plugins.start import subscribe as sub
from utils.custom_filters import login_in_progress
from utils.encrypt import dcs
from typing import Dict, Any, Optional
import logging # <--- Import logging for better error messages

# Configure logging
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

Y = None if not STRING else __import__('shared_client').userbot
Z, P, UB, UC, emp = {}, {}, {}, {}, {}

ACTIVE_USERS = {}
ACTIVE_USERS_FILE = "active_users.json"

# fixed directory file_name problems 
def sanitize(filename):
    # Ensure filename is not None or empty
    if not filename:
        return f"unnamed_file_{int(time.time())}"
    return re.sub(r'[<>:"/\\|?*\']', '_', filename).strip(" .")[:255]

def load_active_users():
    try:
        if os.path.exists(ACTIVE_USERS_FILE):
            with open(ACTIVE_USERS_FILE, 'r') as f:
                return json.load(f)
        return {}
    except Exception as e:
        logger.error(f"Error loading active users: {e}")
        return {}

async def save_active_users_to_file():
    try:
        with open(ACTIVE_USERS_FILE, 'w') as f:
            json.dump(ACTIVE_USERS, f, indent=4) # Add indent for readability
    except Exception as e:
        logger.error(f"Error saving active users: {e}")

async def add_active_batch(user_id: int, batch_info: Dict[str, Any]):
    ACTIVE_USERS[str(user_id)] = batch_info
    await save_active_users_to_file()

def is_user_active(user_id: int) -> bool:
    return str(user_id) in ACTIVE_USERS

async def update_batch_progress(user_id: int, current: int, success: int, total: int, progress_message_id: int, chat_id: int):
    user_str = str(user_id)
    if user_str in ACTIVE_USERS:
        ACTIVE_USERS[user_str]["current"] = current
        ACTIVE_USERS[user_str]["success"] = success
        # Update progress message if it exists
        if progress_message_id and chat_id:
            try:
                bar = '🟢' * int(current / total * 10) + '🔴' * (10 - int(current / total * 10))
                await X.edit_message_text(
                    chat_id,
                    progress_message_id,
                    f"__**Batch Progress...**__\n\n{bar}\n\n✅ **Completed**: {current}/{total}\n⭐ **Success**: {success}/{total}\n\n**__Powered by Team SPY__**"
                )
            except Exception as e:
                logger.warning(f"Failed to update batch progress message for user {user_id}: {e}")
        await save_active_users_to_file()

async def request_batch_cancel(user_id: int):
    user_str = str(user_id)
    if user_str in ACTIVE_USERS:
        ACTIVE_USERS[user_str]["cancel_requested"] = True
        await save_active_users_to_file()
        return True
    return False

def should_cancel(user_id: int) -> bool:
    user_str = str(user_id)
    return user_str in ACTIVE_USERS and ACTIVE_USERS[user_str].get("cancel_requested", False)

async def remove_active_batch(user_id: int):
    user_str = str(user_id)
    if user_str in ACTIVE_USERS:
        del ACTIVE_USERS[user_str]
        await save_active_users_to_file()

def get_batch_info(user_id: int) -> Optional[Dict[str, Any]]:
    return ACTIVE_USERS.get(str(user_id))

# Load active users on bot startup
ACTIVE_USERS = load_active_users()

async def upd_dlg(c: Client):
    try:
        # Use iter_dialogs for more efficient iteration if needed, or simply get_dialogs(limit=1)
        # to ensure connection is active.
        await c.get_dialogs(limit=1)
        return True
    except Exception as e:
        logger.error(f'Failed to update dialogs for client {c.me.id if c.me else "unknown"}: {e}')
        return False

async def get_msg(c: Client, u: Client, channel_id: Any, message_id: int, link_type: str):
    """
    Fetches a message from a given channel/chat using the appropriate client (bot or userbot).
    
    Args:
        c (Client): The bot client (for public channel messages or fallback).
        u (Client): The user client (for private channel messages).
        channel_id (Any): The channel ID or username.
        message_id (int): The message ID.
        link_type (str): 'public' or 'private'.
    
    Returns:
        Message: The fetched message object, or None if not found/error.
    """
    try:
        if link_type == 'public':
            try:
                # First try with bot client if it has access
                msg = await c.get_messages(channel_id, message_id)
                emp[channel_id] = getattr(msg, "empty", False)
                if not emp[channel_id]:
                    return msg
                
                # If bot failed or message empty, try user client (e.g., if channel restricted)
                if u:
                    try:
                        # Try to join if not already a member (userbot only)
                        # This might not always work if channel is private and requires invite link
                        try: await u.join_chat(channel_id) # This may raise if it's a private chat, then resolve_peer will be used
                        except: pass 
                        
                        # Get chat object to resolve ID if it's a username or invite link
                        chat = await u.get_chat(channel_id)
                        return await u.get_messages(chat.id, message_id)
                    except Exception as e:
                        logger.warning(f"Userbot failed to get public message {message_id} from {channel_id}: {e}")
                        return None # Userbot couldn't get it either
            except Exception as e:
                logger.warning(f'Error fetching public message {message_id} from {channel_id} with bot: {e}')
                # Fallback to user client if bot fails for public channels
                if u:
                    try:
                        chat = await u.get_chat(channel_id)
                        return await u.get_messages(chat.id, message_id)
                    except Exception as e_user:
                        logger.error(f"Userbot also failed to get public message {message_id} from {channel_id}: {e_user}")
                        return None
                return None
        else:  # private link_type
            if u:
                try:
                    # Ensure userbot can access the chat
                    chat_id_resolved = channel_id
                    if isinstance(channel_id, str) and not str(channel_id).startswith('-100'):
                        # If channel_id is not already a supergroup ID, try to resolve it
                        try:
                            peer = await u.resolve_peer(channel_id)
                            if hasattr(peer, 'channel_id'): 
                                chat_id_resolved = f'-100{peer.channel_id}'
                            elif hasattr(peer, 'chat_id'): 
                                chat_id_resolved = f'-{peer.chat_id}'
                            elif hasattr(peer, 'user_id'): 
                                chat_id_resolved = peer.user_id
                            else:
                                chat_id_resolved = channel_id # Fallback
                        except Exception as e_resolve:
                            logger.warning(f"Could not resolve peer for {channel_id}: {e_resolve}. Using as is.")
                            chat_id_resolved = channel_id

                    return await u.get_messages(chat_id_resolved, message_id)
                except UserNotParticipant:
                    logger.warning(f"Userbot {u.me.id} is not a participant in private chat {channel_id}. Cannot fetch message.")
                    return None
                except Exception as e:
                    logger.error(f'Error fetching private message {message_id} from {channel_id} with userbot {u.me.id}: {e}')
                    return None
            return None
    except Exception as e:
        logger.error(f'Critical error in get_msg: {e}')
        return None

async def get_ubot(uid: int) -> Optional[Client]:
    """Retrieves or creates a Pyrogram bot client for a given user ID."""
    bt = await get_user_data_key(uid, "bot_token", None)
    if not bt: 
        logger.warning(f"No bot_token found for user {uid}.")
        return None
    if uid in UB and UB.get(uid) and UB.get(uid).is_connected:
        return UB.get(uid)
    try:
        # Client name should be unique and descriptive for debugging
        bot = Client(f"user_bot_{uid}", bot_token=bt, api_id=API_ID, api_hash=API_HASH, no_updates=True) # no_updates for user bots
        await bot.start()
        UB[uid] = bot
        logger.info(f"Bot client started for user {uid}")
        return bot
    except Exception as e:
        logger.error(f"Error starting bot for user {uid}: {e}")
        if uid in UB: # Remove from cache if failed to start/connect
            try: await UB[uid].stop() 
            except: pass
            del UB[uid]
        return None

async def get_uclient(uid: int) -> Optional[Client]:
    """Retrieves or creates a Pyrogram user client for a given user ID."""
    ud = await get_user_data(uid)
    if not ud: 
        logger.warning(f"No user data found for {uid}.")
        return None
    
    if uid in UC and UC.get(uid) and UC.get(uid).is_connected:
        return UC.get(uid)
    
    xxx = ud.get('session_string')
    if not xxx: 
        logger.warning(f"No session_string found for user {uid}. User client not available.")
        return None

    try:
        ss = dcs(xxx)
        gg = Client(f'user_client_{uid}', api_id=API_ID, api_hash=API_HASH, 
                    device_model="v3saver", session_string=ss, no_updates=True) # no_updates for userbots
        await gg.start()
        await upd_dlg(gg) # Ensure dialogs are fetched for peer resolution
        UC[uid] = gg
        logger.info(f"User client started for user {uid}")
        return gg
    except Exception as e:
        logger.error(f'Error starting user client for {uid}: {e}')
        if uid in UC: # Remove from cache if failed to start/connect
            try: await UC[uid].stop() 
            except: pass
            del UC[uid]
        # Fallback to the global userbot if it's available and this user's client failed
        return Y # Consider if Y should be the global userbot or a per-user one

async def prog(current: int, total: int, client: Client, chat_id: int, message_id: int, start_time: float):
    """
    Callback function for Pyrogram's download/upload progress.
    Args:
        current (int): Bytes transferred so far.
        total (int): Total bytes to transfer.
        client (Client): The Pyrogram client.
        chat_id (int): The chat ID where the progress message is.
        message_id (int): The message ID of the progress message.
        start_time (float): Timestamp when the transfer started.
    """
    # Only update progress message at certain intervals to avoid flooding Telegram API
    if total == 0: # Avoid division by zero
        logger.warning("Total size is zero in progress callback.")
        return

    percentage = current * 100 / total
    
    # Update frequency: Adjust based on total size to reduce API calls for small files
    interval = 10 if total >= 100 * 1024 * 1024 else 20 if total >= 50 * 1024 * 1024 else 30 if total >= 10 * 1024 * 1024 else 50
    step = int(percentage // interval) * interval
    
    # Use message_id as key for P to avoid conflicts
    if message_id not in P or P[message_id] != step or percentage >= 100:
        P[message_id] = step
        
        c_mb = current / (1024 * 1024)
        t_mb = total / (1024 * 1024)
        bar = '🟢' * int(percentage / 10) + '🔴' * (10 - int(percentage / 10))
        
        elapsed_time = time.time() - start_time
        speed = (current / elapsed_time) / (1024 * 1024) if elapsed_time > 0 else 0
        eta = time.strftime('%M:%S', time.gmtime((total - current) / (speed * 1024 * 1024))) if speed > 0 else '00:00'
        
        try:
            await client.edit_message_text(
                chat_id,
                message_id,
                f"__**Pyro Handler...**__\n\n{bar}\n\n⚡**__Completed__**: {c_mb:.2f} MB / {t_mb:.2f} MB\n📊 **__Done__**: {percentage:.2f}%\n🚀 **__Speed__**: {speed:.2f} MB/s\n⏳ **__ETA__**: {eta}\n\n**__Powered by Team SPY__**"
            )
        except Exception as e:
            logger.warning(f"Failed to edit progress message {message_id}: {e}")
        
        if percentage >= 100: 
            P.pop(message_id, None)

async def send_direct(c: Client, m: Message, tcid: int, ft: str, rp: ReplyParameters): # Use ReplyParameters
    """
    Attempts to send media directly without downloading if possible (e.g., via file_id).
    
    Args:
        c (Client): The Pyrogram client (bot).
        m (Message): The message object containing the media.
        tcid (int): Target chat ID.
        ft (str): Final caption text.
        rp (ReplyParameters): Reply parameters object.
        
    Returns:
        bool: True if sent directly, False otherwise.
    """
    try:
        # Use ReplyParameters for all send calls
        if m.video:
            await c.send_video(tcid, m.video.file_id, caption=ft, duration=m.video.duration, width=m.video.width, height=m.video.height, reply_parameters=rp)
        elif m.video_note:
            await c.send_video_note(tcid, m.video_note.file_id, reply_parameters=rp)
        elif m.voice:
            await c.send_voice(tcid, m.voice.file_id, reply_parameters=rp)
        elif m.sticker:
            await c.send_sticker(tcid, m.sticker.file_id, reply_parameters=rp)
        elif m.audio:
            await c.send_audio(tcid, m.audio.file_id, caption=ft, duration=m.audio.duration, performer=m.audio.performer, title=m.audio.title, reply_parameters=rp)
        elif m.photo:
            # Pyrogram photo can be a list of Photo objects, take the largest
            photo_id = m.photo.file_id if hasattr(m.photo, 'file_id') else m.photo[-1].file_id
            await c.send_photo(tcid, photo_id, caption=ft, reply_parameters=rp)
        elif m.document:
            await c.send_document(tcid, m.document.file_id, caption=ft, file_name=m.document.file_name, reply_parameters=rp)
        else:
            return False
        return True
    except Exception as e:
        logger.error(f'Direct send error for message {m.id}: {e}')
        return False

async def process_msg(c: Client, u: Client, m: Message, d: int, lt: str, uid: int, i: Any):
    """
    Processes a single message (downloads, renames, uploads).
    
    Args:
        c (Client): The bot client.
        u (Client): The user client (for downloads, if applicable).
        m (Message): The message object to process.
        d (int): User's chat ID (destination for initial messages/progress).
        lt (str): Link type ('public' or 'private').
        uid (int): User ID (same as d).
        i (Any): Channel ID or username of the source.
        
    Returns:
        str: Status message (e.g., 'Done.', 'Failed.', 'Sent directly.').
    """
    try:
        # Determine target chat ID and reply message ID
        cfg_chat = await get_user_data_key(uid, 'chat_id', None)
        tcid = d # Default target chat ID is the user's chat ID
        rp = ReplyParameters(message_id=m.id) # Default reply to original message for direct sends
        
        if cfg_chat:
            try:
                if '/' in cfg_chat:
                    parts = cfg_chat.split('/', 1)
                    tcid = int(parts[0])
                    # Update reply_parameters for the configured target chat
                    rp = ReplyParameters(message_id=int(parts[1]) if len(parts) > 1 else None)
                else:
                    tcid = int(cfg_chat)
                    rp = ReplyParameters(message_id=m.id) # If no reply_id, reply to current
            except ValueError:
                logger.warning(f"Invalid chat_id format '{cfg_chat}' for user {uid}, falling back to user chat_id.")
                tcid = d
                rp = ReplyParameters(message_id=m.id)
        
        # Prepare caption text
        ft = ""
        if m.media:
            orig_text = m.caption.markdown if m.caption else ''
            proc_text = await process_text_with_rules(uid, orig_text)
            user_cap = await get_user_data_key(uid, 'caption', '')
            ft = f'{proc_text}\n\n{user_cap}' if proc_text and user_cap else (user_cap if user_cap else proc_text)
            ft = ft or None # Ensure it's None if empty to avoid empty caption in Telegram
            
            # Attempt direct send for public links (if not forced download or self-hosted)
            # This is tricky because get_msg might return a message object that the bot itself can't forward.
            # Best to try copy_message, which handles forwarding if possible, then fallback to download.
            if lt == 'public':
                try:
                    copied_message = await c.copy_message(chat_id=tcid, from_chat_id=i, message_id=m.id, caption=ft, reply_parameters=rp)
                    if copied_message:
                        logger.info(f"Message {m.id} from public {i} copied directly to {tcid}.")
                        return 'Copied directly.'
                except Exception as e:
                    logger.warning(f"Failed to copy message {m.id} from public {i} directly: {e}. Attempting download.")
            
            st = time.time()
            p = await c.send_message(d, 'Downloading...') # 'p' is the progress message for the user

            # Determine file_name for download
            original_filename = m.document.file_name if m.document else \
                                m.video.file_name if m.video else \
                                m.audio.file_name if m.audio else None
            
            # Use original filename base if available, else generic timestamp
            file_name_base = os.path.splitext(original_filename)[0] if original_filename else f"file_{int(time.time())}"
            
            ext = ""
            if m.video: ext = ".mp4"
            elif m.audio: ext = ".mp3"
            elif m.document: ext = os.path.splitext(original_filename)[1] if original_filename else ".bin"
            elif m.photo: ext = ".jpg"
            elif m.video_note: ext = ".mp4"
            elif m.voice: ext = ".ogg"
            elif m.sticker: ext = ".webp" # Stickers can be downloaded as webp

            # Sanitize and ensure file_name is not empty
            c_name = sanitize(file_name_base) + ext
            if not c_name: # Fallback if sanitize makes it empty
                c_name = f"downloaded_file_{int(time.time())}{ext or '.bin'}"

            # Download using the user client (u)
            f = None
            try:
                f = await u.download_media(m, file_name=c_name, progress=prog, progress_args=(c, d, p.id, st))
            except Exception as e:
                logger.error(f"Error downloading media for message {m.id}: {e}")
                await c.edit_message_text(d, p.id, 'Failed to download file. (Download Error)')
                return 'Failed to download.'
            
            if not f or not os.path.exists(f): 
                await c.edit_message_text(d, p.id, 'Failed to download file. (File missing)')
                return 'Failed to download.'
            
            await c.edit_message_text(d, p.id, 'Renaming...')
            renamed_f = await rename_file(f, uid, p) # Pass user_id for get_user_data_key
            if renamed_f and os.path.exists(renamed_f):
                f = renamed_f
            else:
                logger.warning(f"Renaming failed or returned invalid path for {f}. Continuing with original path.")

            fsize = os.path.getsize(f) / (1024 * 1024 * 1024) # Size in GB
            user_thumb_path = thumbnail(uid) # This could be user's custom thumb (d.jpg) or None

            # Handling large files (over 2GB) with global userbot Y
            if fsize > 2 and Y and Y.is_connected:
                logger.info(f"File {f} is larger than 2GB ({fsize:.2f}GB). Using global userbot Y for upload.")
                upload_start_time = time.time()
                await c.edit_message_text(d, p.id, 'File is larger than 2GB. Using alternative method (Userbot)...')
                
                # Check userbot dialogs before large upload
                try:
                    await upd_dlg(Y) 
                except Exception as e:
                    logger.error(f"Global Userbot dialog update failed: {e}. Large file upload might fail.")

                dur, h, w = None, None, None
                thumb_to_use = user_thumb_path # Default to user's custom thumb if set

                if m.video or (isinstance(f, str) and os.path.splitext(f)[1].lower() in ['.mp4', '.mkv', '.avi', '.webm']):
                    mtd = await get_video_metadata(f)
                    dur, h, w = mtd.get('duration', 1), mtd.get('height', 1), mtd.get('width', 1) # Provide defaults
                    if not user_thumb_path: # Create screenshot if no custom thumb
                        temp_screenshot = await screenshot(f, dur, uid)
                        if temp_screenshot and os.path.exists(temp_screenshot):
                            thumb_to_use = temp_screenshot
                        else:
                            logger.warning(f"Failed to create screenshot for {f}.")

                sent = None
                try:
                    # Upload to LOG_GROUP via global userbot
                    reply_params_log = ReplyParameters(message_id=m.id) # Reply in log group to original message (if applicable)

                    if m.video or (isinstance(f, str) and os.path.splitext(f)[1].lower() in ['.mp4', '.mkv', '.avi', '.webm']):
                        sent = await Y.send_video(LOG_GROUP, f, thumb=thumb_to_use, caption=ft,
                                                duration=dur, height=h, width=w,
                                                reply_parameters=reply_params_log, progress=prog, progress_args=(c, d, p.id, upload_start_time))
                    elif m.audio:
                        sent = await Y.send_audio(LOG_GROUP, f, thumb=thumb_to_use, caption=ft,
                                                duration=m.audio.duration, performer=m.audio.performer, title=m.audio.title,
                                                reply_parameters=reply_params_log, progress=prog, progress_args=(c, d, p.id, upload_start_time))
                    elif m.photo:
                        sent = await Y.send_photo(LOG_GROUP, f, caption=ft,
                                                reply_parameters=reply_params_log, progress=prog, progress_args=(c, d, p.id, upload_start_time))
                    elif m.document:
                        sent = await Y.send_document(LOG_GROUP, f, thumb=thumb_to_use, caption=ft,
                                                    file_name=os.path.basename(f), # Ensure filename for document
                                                    reply_parameters=reply_params_log, progress=prog, progress_args=(c, d, p.id, upload_start_time))
                    elif m.video_note:
                        sent = await Y.send_video_note(LOG_GROUP, f, reply_parameters=reply_params_log, progress=prog, progress_args=(c, d, p.id, upload_start_time))
                    elif m.voice:
                        sent = await Y.send_voice(LOG_GROUP, f, reply_parameters=reply_params_log, progress=prog, progress_args=(c, d, p.id, upload_start_time))
                    elif m.sticker:
                         # Stickers are sent by file_id, not path for download from file
                        await c.edit_message_text(d, p.id, "Stickers over 2GB cannot be handled directly.")
                        sent = False # Indicate not sent via this path
                    else:
                        sent = await Y.send_document(LOG_GROUP, f, thumb=thumb_to_use, caption=ft, # Fallback to document
                                                    file_name=os.path.basename(f),
                                                    reply_parameters=reply_params_log, progress=prog, progress_args=(c, d, p.id, upload_start_time))
                    
                    if sent:
                        # Copy from LOG_GROUP to user's chat
                        copied_to_user = await c.copy_message(tcid, LOG_GROUP, sent.id, reply_parameters=rp)
                        if copied_to_user:
                            logger.info(f"Large file {f} uploaded to LOG_GROUP and copied to user {uid}.")
                            await c.delete_messages(d, p.id) # Delete progress message
                            return 'Done (Large file).'
                        else:
                            raise Exception(f"Failed to copy large file from LOG_GROUP to user {uid}.")
                    else:
                        raise Exception("Failed to send large file via global userbot.")

                except Exception as upload_e:
                    logger.error(f"Large file upload failed for {f}: {upload_e}")
                    await c.edit_message_text(d, p.id, f'Large file upload failed: {str(upload_e)}')
                    return 'Large file upload failed.'
                finally:
                    # Clean up temp screenshot if created and it's not the user's custom thumbnail
                    if thumb_to_use and os.path.exists(thumb_to_use) and thumb_to_use != user_thumb_path:
                        os.remove(thumb_to_use)
                    if os.path.exists(f): os.remove(f) # Clean up downloaded file
            
            # Handling smaller files or if global userbot Y is not available
            logger.info(f"File {f} is {fsize:.2f}GB. Uploading via bot client (smaller file or no userbot Y).")
            upload_start_time = time.time()
            await c.edit_message_text(d, p.id, 'Uploading...')

            thumb_to_use = user_thumb_path # Default to user's custom thumb if set

            try:
                # Use ReplyParameters for all send calls
                if m.video or (isinstance(f, str) and os.path.splitext(f)[1].lower() in ['.mp4', '.mkv', '.avi', '.webm']):
                    mtd = await get_video_metadata(f)
                    dur, h, w = mtd.get('duration', 1), mtd.get('height', 1), mtd.get('width', 1) # Provide defaults
                    if not user_thumb_path: # Create screenshot if no custom thumb
                        temp_screenshot = await screenshot(f, dur, uid)
                        if temp_screenshot and os.path.exists(temp_screenshot):
                            thumb_to_use = temp_screenshot
                        else:
                            logger.warning(f"Failed to create screenshot for {f}.")
                    
                    await c.send_video(tcid, video=f, caption=ft, 
                                    thumb=thumb_to_use, width=w, height=h, duration=dur, 
                                    progress=prog, progress_args=(c, d, p.id, upload_start_time), 
                                    reply_parameters=rp)
                elif m.video_note:
                    await c.send_video_note(tcid, video_note=f, progress=prog, 
                                        progress_args=(c, d, p.id, upload_start_time), reply_parameters=rp)
                elif m.voice:
                    await c.send_voice(tcid, f, progress=prog, progress_args=(c, d, p.id, upload_start_time), 
                                    reply_parameters=rp)
                elif m.sticker:
                    # Stickers are always sent by file_id if they already exist on Telegram
                    # If it was downloaded, it was a file, but can only be uploaded as sticker IF it's a webp image valid as sticker
                    # For general case, if source was sticker, send it as a sticker. If downloaded, treat as document.
                    if m.sticker: # If the original message was a sticker
                        await c.send_sticker(tcid, m.sticker.file_id, reply_parameters=rp)
                    else: # If it's a downloaded sticker file, send as document
                        await c.send_document(tcid, document=f, caption=ft, thumb=thumb_to_use,
                                            progress=prog, progress_args=(c, d, p.id, upload_start_time), 
                                            reply_parameters=rp)
                elif m.audio:
                    # Ensure thumb_to_use is valid (None or a path)
                    await c.send_audio(tcid, audio=f, caption=ft, 
                                    thumb=thumb_to_use, progress=prog, progress_args=(c, d, p.id, upload_start_time), 
                                    reply_parameters=rp)
                elif m.photo:
                    # Ensure thumb_to_use is valid
                    await c.send_photo(tcid, photo=f, caption=ft, 
                                    progress=prog, progress_args=(c, d, p.id, upload_start_time), 
                                    reply_parameters=rp)
                else: # Default to document for other types or fallback
                    await c.send_document(tcid, document=f, caption=ft, 
                                        thumb=thumb_to_use, progress=prog, progress_args=(c, d, p.id, upload_start_time), 
                                        reply_parameters=rp)
            except Exception as e:
                logger.error(f"Small file upload failed for {f}: {e}")
                await c.edit_message_text(d, p.id, f'Upload failed: {str(e)}')
                return 'Failed.'
            finally:
                # Clean up temp screenshot if created and it's not the user's custom thumbnail
                if thumb_to_use and os.path.exists(thumb_to_use) and thumb_to_use != user_thumb_path:
                    os.remove(thumb_to_use)
                if os.path.exists(f): os.remove(f) # Clean up downloaded file

            await c.delete_messages(d, p.id) # Delete progress message
            return 'Done.'
            
        elif m.text:
            if m.text.markdown:
                await c.send_message(tcid, text=m.text.markdown, reply_parameters=rp) # Use reply_parameters
                return 'Sent.'
            else:
                return 'No text found in message.'
        else:
            return 'Unsupported message type.'
    except Exception as e:
        logger.error(f"Critical error in process_msg for user {uid}, message {m.id}: {e}")
        # Attempt to delete progress message if it exists, to avoid leaving it hanging
        if 'p' in locals() and p:
            try:
                await c.delete_messages(d, p.id)
            except Exception as delete_e:
                logger.warning(f"Failed to delete progress message {p.id}: {delete_e}")
        
        # Clean up downloaded file if it exists
        if 'f' in locals() and f and os.path.exists(f):
            try:
                os.remove(f)
            except Exception as rm_e:
                logger.warning(f"Failed to remove downloaded file {f}: {rm_e}")
        
        return f'Fatal Error: {str(e)}'


@X.on_message(filters.command(['batch', 'single']) & filters.private & ~login_in_progress)
async def process_cmd(c: Client, m: Message):
    uid = m.from_user.id
    cmd = m.command[0]
    
    if FREEMIUM_LIMIT == 0 and not await is_premium_user(uid):
        await m.reply_text("This bot does not provide free services. Please get a subscription from the OWNER.")
        return
    
    if await sub(c, m) == 1: # Assumes sub handles force subscribe and returns 1 if not subscribed
        return
    
    pro = await m.reply_text('Doing some checks, please wait...')
    
    if is_user_active(uid):
        await pro.edit('You have an active task. Use /stop to cancel it.')
        return
    
    ubot = await get_ubot(uid)
    if not ubot:
        await pro.edit('Your userbot is not configured. Please set it up with /setbot first.')
        return
    
    Z[uid] = {'step': 'start' if cmd == 'batch' else 'start_single'}
    await pro.edit(f'Send the {"start link for batch processing..." if cmd == "batch" else "link you want to process..."}.')

@X.on_message(filters.command(['cancel', 'stop']) & filters.private)
async def cancel_cmd(c: Client, m: Message):
    uid = m.from_user.id
    if is_user_active(uid):
        if await request_batch_cancel(uid):
            await m.reply_text('Cancellation requested. The current batch will stop after the current message has finished processing.')
        else:
            await m.reply_text('Failed to request cancellation. Please try again or wait for the current process to finish.')
    else:
        await m.reply_text('No active batch process found for your account.')

@X.on_message(filters.text & filters.private & ~login_in_progress & ~filters.command([
    'start', 'batch', 'cancel', 'login', 'logout', 'stop', 'set', 
    'pay', 'redeem', 'gencode', 'single', 'generate', 'keyinfo', 'encrypt', 'decrypt', 'keys', 'setbot', 'rembot']))
async def text_handler(c: Client, m: Message):
    uid = m.from_user.id
    
    if uid not in Z: 
        # If user sends text not part of a command flow, ignore or send help message
        # await m.reply_text("Please use /start to begin or /help for more info.")
        return # Silently ignore for now if not part of flow
    
    s = Z[uid].get('step')

    if s == 'start': # For batch link input
        L = m.text
        i, d_id, lt = E(L) # d_id is start_message_id
        if not i or not d_id:
            await m.reply_text('Invalid link format. Please send a valid Telegram message link.')
            Z.pop(uid, None)
            return
        Z[uid].update({'step': 'count', 'cid': i, 'sid': d_id, 'lt': lt})
        await m.reply_text('How many messages do you want to process from this link, starting from the message ID in the link?')

    elif s == 'start_single': # For single link input
        L = m.text
        i, d_id, lt = E(L) # d_id is start_message_id
        if not i or not d_id:
            await m.reply_text('Invalid link format. Please send a valid Telegram message link.')
            Z.pop(uid, None)
            return

        Z[uid].update({'step': 'process_single', 'cid': i, 'sid': d_id, 'lt': lt})
        # Extract variables locally for clarity
        channel_id, message_id, link_type = Z[uid]['cid'], Z[uid]['sid'], Z[uid]['lt']
        
        pt = await m.reply_text('Processing single message...')
        
        ubot = await get_ubot(uid)
        if not ubot:
            await pt.edit('Your userbot is not configured. Please set it up with /setbot first.')
            Z.pop(uid, None)
            return
        
        uc = await get_uclient(uid) # Prefer user client for message fetching
        if not uc: # Fallback to global userbot if user's custom client isn't available
            uc = Y
            if not uc: # If neither user's client nor global userbot is available
                await pt.edit('Cannot proceed without a user client or global userbot. Please log in with /login or ensure global userbot is active.')
                Z.pop(uid, None)
                return
        
        if is_user_active(uid):
            await pt.edit('You have an active task. Please use /stop first before starting another.')
            Z.pop(uid, None)
            return

        try:
            # For single message, add to active_users temporarily to prevent double processing
            await add_active_batch(uid, {
                "total": 1,
                "current": 0,
                "success": 0,
                "cancel_requested": False,
                "progress_message_id": pt.id # Store progress message ID
            })
            
            msg = await get_msg(c, uc, channel_id, message_id, link_type) # Use bot client 'c' for public chats and user client 'uc' for private
            if msg:
                res = await process_msg(c, uc, msg, m.chat.id, link_type, uid, channel_id) # Pass m.chat.id as 'd' (destination)
                await pt.edit(f'Processed: {res}')
            else:
                await pt.edit('Message not found or could not be accessed. Ensure the link is correct and the bot/userbot has access.')
        except Exception as e:
            logger.error(f"Error processing single message for user {uid}: {e}")
            await pt.edit(f'An unexpected error occurred: {str(e)}')
        finally:
            await remove_active_batch(uid) # Always clean up
            Z.pop(uid, None)

    elif s == 'count': # For batch count input
        if not m.text.isdigit():
            await m.reply_text('Please enter a valid number for the message count.')
            return
        
        count = int(m.text)
        maxlimit = PREMIUM_LIMIT if await is_premium_user(uid) else FREEMIUM_LIMIT

        if count > maxlimit:
            await m.reply_text(f'The maximum limit for your account is {maxlimit} messages. Please enter a lower number.')
            return

        Z[uid].update({'step': 'process', 'did': m.chat.id, 'num': count}) # m.chat.id is int here
        # Extract variables locally
        channel_id, start_message_id, total_messages, link_type = Z[uid]['cid'], Z[uid]['sid'], Z[uid]['num'], Z[uid]['lt']
        success_count = 0

        pt = await m.reply_text('Starting batch processing...')
        uc = await get_uclient(uid) # User's personal client
        ubot = await get_ubot(uid) # User's personal bot client (for progress messages etc.)
        
        if not uc: # Fallback to global userbot if user's custom client isn't available
            uc = Y
            if not uc:
                await pt.edit('Cannot proceed without a user client or global userbot. Please log in with /login or ensure global userbot is active.')
                await remove_active_batch(uid) # Ensure cleanup if setup fails
                Z.pop(uid, None)
                return
        
        if not ubot: # If user's own bot is not available, use main bot for progress
             ubot = c # Use the main bot client `c` as the client for progress updates
        
        if is_user_active(uid):
            await pt.edit('You have an active task. Please use /stop first before starting another.')
            Z.pop(uid, None)
            return
        
        # Add to active users for cancellation and tracking
        await add_active_batch(uid, {
            "total": total_messages,
            "current": 0,
            "success": 0,
            "cancel_requested": False,
            "progress_message_id": pt.id # Store progress message ID
        })
        
        try:
            for j in range(total_messages):
                current_message_index = j + 1
                
                if should_cancel(uid):
                    await pt.edit(f'Batch process cancelled by user. Processed: {current_message_index-1}/{total_messages}, Success: {success_count}.')
                    break # Exit loop on cancel request
                
                # Update progress message periodically
                await update_batch_progress(uid, current_message_index, success_count, total_messages, pt.id, m.chat.id)
                
                current_message_id_in_channel = int(start_message_id) + j
                
                try:
                    # Use `ubot` for sending progress, `uc` for fetching messages
                    msg = await get_msg(ubot, uc, channel_id, current_message_id_in_channel, link_type)
                    if msg:
                        # Pass `ubot` as the client for `process_msg` to ensure progress messages are sent by the user's bot if available
                        res = await process_msg(ubot, uc, msg, m.chat.id, link_type, uid, channel_id) 
                        if 'Done' in res or 'Copied' in res or 'Sent' in res:
                            success_count += 1
                        else:
                            logger.warning(f"Message {current_message_id_in_channel} processing failed with result: {res}")
                    else:
                        logger.warning(f"Message {current_message_id_in_channel} not found or inaccessible.")
                        # Optionally, inform the user if many messages are missing
                        if (j + 1) % 10 == 0: # Every 10 messages
                            try: await pt.edit(f'Warning: Message {current_message_id_in_channel} not found. Continuing...')
                            except: pass # Suppress error if edit fails

                except Exception as e:
                    logger.error(f"Error processing message {current_message_id_in_channel} for user {uid}: {e}")
                    # Update progress message with error if possible
                    try: await pt.edit(f'{current_message_index}/{total_messages}: Error - {str(e)[:50]}')
                    except: pass
                
                await asyncio.sleep(5) # Small delay to avoid hitting flood limits

            # Final update of progress after loop
            await update_batch_progress(uid, total_messages, success_count, total_messages, pt.id, m.chat.id)
            await m.reply_text(f'Batch processing completed. Total: {total_messages}, Success: {success_count}.')
        
        except Exception as e:
            logger.error(f"Unhandled error in batch processing for user {uid}: {e}")
            await m.reply_text(f'An unexpected error occurred during batch processing: {str(e)}')
        finally:
            await remove_active_batch(uid) # Always clean up active task state
            Z.pop(uid, None) # Remove user from command flow