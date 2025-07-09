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
import logging # <--- Import logging

# Configure logging
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__) # Use logger instead of print

Y = None if not STRING else __import__('shared_client').userbot
Z, P, UB, UC, emp = {}, {}, {}, {}, {}

ACTIVE_USERS = {}
ACTIVE_USERS_FILE = "active_users.json"

# fixed directory file_name problems 
def sanitize(filename):
    # Ensure filename is not None or empty
    if not filename:
        return f"unnamed_file_{int(time.time())}" # Fallback to generic name
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
            json.dump(ACTIVE_USERS, f, indent=4)
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
                # Calculate percentage for progress bar
                percentage = (current / total * 100) if total > 0 else 0
                bar = '🟢' * int(percentage / 10) + '🔴' * (10 - int(percentage / 10))
                
                await X.edit_message_text( # X is the bot client
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

ACTIVE_USERS = load_active_users()

async def upd_dlg(c):
    try:
        async for _ in c.get_dialogs(limit=100): pass
        return True
    except Exception as e:
        logger.error(f'Failed to update dialogs: {e}')
        return False

async def get_msg(c, u, i, d, lt):
    try:
        if lt == 'public':
            try:
                xm = await c.get_messages(i, d)
                emp[i] = getattr(xm, "empty", False)
                if emp[i]:
                    try: await u.join_chat(i)
                    except: pass
                    xm = await u.get_messages((await u.get_chat(f"@{i}")).id, d)
                return xm
            except Exception as e:
                logger.error(f'Error fetching public message: {e}')
                return None
        else:
            if u:
                try:
                    async for _ in u.get_dialogs(limit=50): pass
                    chat_id = i if str(i).startswith('-100') else f'-100{i}' if i.isdigit() else i
                    try:
                        peer = await u.resolve_peer(chat_id)
                        if hasattr(peer, 'channel_id'): resolved_id = f'-100{peer.channel_id}'
                        elif hasattr(peer, 'chat_id'): resolved_id = f'-{peer.chat_id}'
                        elif hasattr(peer, 'user_id'): resolved_id = peer.user_id
                        else: resolved_id = chat_id
                        return await u.get_messages(resolved_id, d)
                    except Exception:
                        try:
                            chat = await u.get_chat(chat_id)
                            return await u.get_messages(chat.id, d)
                        except Exception:
                            async for _ in u.get_dialogs(limit=200): pass
                            return await u.get_messages(chat_id, d)
                except Exception as e:
                    logger.error(f'Private channel error: {e}')
                    return None
            return None
    except Exception as e:
        logger.error(f'Error fetching message: {e}')
        return None

async def get_ubot(uid):
    bt = await get_user_data_key(uid, "bot_token", None)
    if not bt: return None
    if uid in UB and UB.get(uid) and UB.get(uid).is_connected: # Check if connected
        return UB.get(uid)
    try:
        bot = Client(f"user_{uid}", bot_token=bt, api_id=API_ID, api_hash=API_HASH, no_updates=True) # Add no_updates
        await bot.start()
        UB[uid] = bot
        return bot
    except Exception as e:
        logger.error(f"Error starting bot for user {uid}: {e}")
        return None

async def get_uclient(uid):
    ud = await get_user_data(uid)
    ubot = UB.get(uid)
    cl = UC.get(uid)
    if cl and cl.is_connected: # Check if connected
        return cl
    if not ud: return ubot if ubot else None
    xxx = ud.get('session_string')
    if xxx:
        try:
            ss = dcs(xxx)
            gg = Client(f'{uid}_client', api_id=API_ID, api_hash=API_HASH, device_model="v3saver", session_string=ss, no_updates=True) # Add no_updates
            await gg.start()
            await upd_dlg(gg)
            UC[uid] = gg
            return gg
        except Exception as e:
            logger.error(f'User client error: {e}')
            return ubot if ubot else Y # Fallback if user client fails
    return Y

# prog function, now using Pyrogram's direct arguments
async def prog(current: int, total: int, client: Client, chat_id: int, message_id: int, start_time: float):
    global P
    if total == 0: # Avoid division by zero
        logger.warning("Total size is zero in progress callback.")
        return

    percentage = current * 100 / total
    
    interval = 10 if total >= 100 * 1024 * 1024 else 20 if total >= 50 * 1024 * 1024 else 30 if total >= 10 * 1024 * 1024 else 50
    step = int(percentage // interval) * interval
    
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

# send_direct function - menggunakan reply_parameters
async def send_direct(c: Client, m: Message, tcid: int, ft: Optional[str], rp: ReplyParameters):
    try:
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

# process_msg function - menggunakan reply_parameters
async def process_msg(c, u, m, d, lt, uid, i):
    try:
        cfg_chat = await get_user_data_key(d, 'chat_id', None)
        tcid = d # Default target chat ID is the user's chat ID
        
        # Default reply parameters, can be overridden by cfg_chat
        rp = ReplyParameters(message_id=m.id) # Reply to the message being processed

        if cfg_chat:
            try:
                if '/' in cfg_chat:
                    parts = cfg_chat.split('/', 1)
                    tcid = int(parts[0])
                    # If target chat has a specific reply_to_message_id
                    rp_msg_id = int(parts[1]) if len(parts) > 1 else None
                    if rp_msg_id:
                        rp = ReplyParameters(message_id=rp_msg_id)
                    else: # If only chat_id is provided, reply to current message in that chat
                        rp = ReplyParameters(message_id=m.id) # Or consider replying to nothing if not directly relevant
                else:
                    tcid = int(cfg_chat)
                    rp = ReplyParameters(message_id=m.id) # If only chat_id, reply to current message
            except ValueError:
                logger.warning(f"Invalid chat_id format '{cfg_chat}', falling back to user chat_id.")
                tcid = d # Fallback to user's chat if cfg_chat is invalid
                rp = ReplyParameters(message_id=m.id)
        
        if m.media:
            orig_text = m.caption.markdown if m.caption else ''
            proc_text = await process_text_with_rules(d, orig_text)
            user_cap = await get_user_data_key(d, 'caption', '')
            # Ensure ft is None if empty, so Telegram doesn't show "No caption"
            ft = f'{proc_text}\n\n{user_cap}' if proc_text and user_cap else (user_cap if user_cap else proc_text)
            ft = ft or None # Set to None if resulting string is empty

            # Try to copy message directly if public and bot has access
            if lt == 'public':
                try:
                    copied_message = await c.copy_message(chat_id=tcid, from_chat_id=i, message_id=m.id, caption=ft, reply_parameters=rp)
                    if copied_message:
                        return 'Copied directly.'
                except Exception as e:
                    logger.warning(f"Failed to copy message {m.id} from public {i} directly: {e}. Attempting download.")
            
            st = time.time()
            p = await c.send_message(d, 'Downloading...') # 'p' is the progress message

            # Determine file_name for download, ensure it's always set
            # Use original filename if available, else a generic one
            file_name_base = None
            if m.video and m.video.file_name:
                file_name_base = os.path.splitext(m.video.file_name)[0]
            elif m.audio and m.audio.file_name:
                file_name_base = os.path.splitext(m.audio.file_name)[0]
            elif m.document and m.document.file_name:
                file_name_base = os.path.splitext(m.document.file_name)[0]
            else:
                file_name_base = f"file_{int(time.time())}" # Fallback
            
            ext = ""
            if m.video: ext = ".mp4"
            elif m.audio: ext = ".mp3"
            elif m.document: ext = os.path.splitext(m.document.file_name)[1] if m.document.file_name else ".bin"
            elif m.photo: ext = ".jpg"
            
            # Sanitize and ensure file_name is not empty
            c_name = sanitize(file_name_base) + ext
            if not c_name: # Fallback if sanitize makes it empty (unlikely with new base logic)
                c_name = f"downloaded_file_{int(time.time())}{ext or '.bin'}"

            f = await u.download_media(m, file_name=c_name, progress=prog, progress_args=(c, d, p.id, st))
            
            if not f or not os.path.exists(f) or os.path.getsize(f) == 0: # Crucial check if download failed or file is empty
                await c.edit_message_text(d, p.id, 'Failed to download file or file is empty.')
                if f and os.path.exists(f): os.remove(f) # Clean up empty/corrupt file
                return 'Failed to download.'
            
            await c.edit_message_text(d, p.id, 'Renaming...')
            # Ensure 'f' is a string before passing to rename_file
            if isinstance(f, str):
                renamed_f = await rename_file(f, d, p) # Pass user_id (d) for get_user_data_key
                if renamed_f and os.path.exists(renamed_f):
                    f = renamed_f
                else:
                    logger.warning(f"Renaming failed or returned invalid path for {f}. Continuing with original path.")
            else:
                logger.warning(f"Downloaded file path is not a string: {f}. Skipping rename.")

            fsize = os.path.getsize(f) / (1024 * 1024 * 1024)
            th = thumbnail(d) # This can return None, handle it later in send_media calls

            # Handling large files (over 2GB) with userbot Y
            if fsize > 2 and Y:
                st = time.time()
                await c.edit_message_text(d, p.id, 'File is larger than 2GB. Using alternative method...')
                try:
                    await upd_dlg(Y) # Ensure userbot dialogs are updated
                except Exception as e:
                    logger.error(f"Userbot dialog update failed: {e}. Large file upload might fail.")

                dur, h, w = None, None, None
                if m.video or (isinstance(f, str) and os.path.splitext(f)[1].lower() in ['.mp4', '.mkv', '.avi', '.webm']): # Check for video extension
                    mtd = await get_video_metadata(f)
                    dur = mtd.get('duration', 0) if mtd else 0
                    h = mtd.get('height', 0) if mtd else 0
                    w = mtd.get('width', 0) if mtd else 0
                    if not th: # Only create screenshot if no custom thumb is set
                        th = await screenshot(f, dur or 1, d) # Pass default duration if None

                sent = None
                try:
                    # Generic way to send, try to match media type
                    # Using rp (reply parameters) for LOG_GROUP upload too
                    if m.video or (isinstance(f, str) and os.path.splitext(f)[1].lower() in ['.mp4', '.mkv', '.avi', '.webm']):
                        sent = await Y.send_video(LOG_GROUP, f, thumb=th, caption=ft,
                                                duration=dur, height=h, width=w,
                                                reply_parameters=rp, progress=prog, progress_args=(c, d, p.id, st))
                    elif m.audio:
                        sent = await Y.send_audio(LOG_GROUP, f, thumb=th, caption=ft,
                                                duration=m.audio.duration, performer=m.audio.performer, title=m.audio.title,
                                                reply_parameters=rp, progress=prog, progress_args=(c, d, p.id, st))
                    elif m.photo:
                        sent = await Y.send_photo(LOG_GROUP, f, caption=ft,
                                                reply_parameters=rp, progress=prog, progress_args=(c, d, p.id, st))
                    elif m.document:
                        sent = await Y.send_document(LOG_GROUP, f, thumb=th, caption=ft, file_name=os.path.basename(f),
                                                    reply_parameters=rp, progress=prog, progress_args=(c, d, p.id, st))
                    elif m.video_note:
                        sent = await Y.send_video_note(LOG_GROUP, f, reply_parameters=rp, progress=prog, progress_args=(c, d, p.id, st))
                    elif m.voice:
                        sent = await Y.send_voice(LOG_GROUP, f, reply_parameters=rp, progress=prog, progress_args=(c, d, p.id, st))
                    elif m.sticker: # Stickers are special, usually sent by file_id
                        await c.edit_message_text(d, p.id, "Stickers over 2GB are not directly supported for re-upload from file.")
                        sent = False # Indicate that it won't be sent via this path
                    else:
                        sent = await Y.send_document(LOG_GROUP, f, thumb=th, caption=ft, file_name=os.path.basename(f),
                                                    reply_parameters=rp, progress=prog, progress_args=(c, d, p.id, st))

                    if sent:
                        await c.copy_message(tcid, LOG_GROUP, sent.id, reply_parameters=rp) # Copy to user's chat from log group
                        # Ensure 'th' is removed only if it was created during screenshot and not user's custom thumb
                        if th and os.path.exists(th) and th != f'{d}.jpg': 
                            os.remove(th)
                        os.remove(f)
                        await c.delete_messages(d, p.id)
                        return 'Done (Large file).'
                    else:
                        raise Exception("Failed to send large file via userbot.")

                except Exception as upload_e:
                    logger.error(f"Large file upload failed for {f}: {upload_e}")
                    await c.edit_message_text(d, p.id, f'Large file upload failed: {str(upload_e)[:50]}')
                    if th and os.path.exists(th) and th != f'{d}.jpg':
                        os.remove(th)
                    if os.path.exists(f): os.remove(f)
                    return 'Large file upload failed.'

            # Handling smaller files or if userbot Y is not available
            await c.edit_message_text(d, p.id, 'Uploading...')
            st = time.time()

            try:
                if m.video or (isinstance(f, str) and os.path.splitext(f)[1].lower() in ['.mp4', '.mkv', '.avi', '.webm']):
                    mtd = await get_video_metadata(f)
                    dur = mtd.get('duration', 0) if mtd else 0
                    h = mtd.get('height', 0) if mtd else 0
                    w = mtd.get('width', 0) if mtd else 0
                    # Create screenshot only if no custom thumbnail is provided
                    th_for_upload = await screenshot(f, dur or 1, d) if not th else th 
                    await c.send_video(tcid, video=f, caption=ft, 
                                    thumb=th_for_upload, width=w, height=h, duration=dur, 
                                    progress=prog, progress_args=(c, d, p.id, st), 
                                    reply_parameters=rp) # <--- Perubahan di sini
                elif m.video_note:
                    await c.send_video_note(tcid, video_note=f, progress=prog, 
                                        progress_args=(c, d, p.id, st), reply_parameters=rp) # <--- Perubahan di sini
                elif m.voice:
                    await c.send_voice(tcid, f, progress=prog, progress_args=(c, d, p.id, st), 
                                    reply_parameters=rp) # <--- Perubahan di sini
                elif m.sticker:
                    # If the original message was a sticker, send by file_id if available
                    if m.sticker:
                        await c.send_sticker(tcid, m.sticker.file_id, reply_parameters=rp) # <--- Perubahan di sini
                    else: # Otherwise, if it's a downloaded file that was originally a sticker, send as document
                        await c.send_document(tcid, document=f, caption=ft, reply_parameters=rp) # <--- Perubahan di sini
                elif m.audio:
                    await c.send_audio(tcid, audio=f, caption=ft, 
                                    thumb=th, progress=prog, progress_args=(c, d, p.id, st), 
                                    reply_parameters=rp) # <--- Perubahan di sini
                elif m.photo:
                    await c.send_photo(tcid, photo=f, caption=ft, 
                                    progress=prog, progress_args=(c, d, p.id, st), 
                                    reply_parameters=rp) # <--- Perubahan di sini
                else: # Default to document for other types or fallback
                    await c.send_document(tcid, document=f, caption=ft, 
                                        thumb=th, progress=prog, progress_args=(c, d, p.id, st), 
                                        reply_parameters=rp) # <--- Perubahan di sini
            except Exception as e:
                logger.error(f"Small file upload failed for {f}: {e}")
                await c.edit_message_text(d, p.id, f'Upload failed: {str(e)[:50]}')
                # Clean up temp screenshot if created
                if th and os.path.exists(th) and th != f'{d}.jpg':
                    os.remove(th)
                if os.path.exists(f): os.remove(f)
                return 'Failed.'
            finally:
                # Clean up temp screenshot if created and it's not the user's custom thumbnail
                if th and os.path.exists(th) and th != f'{d}.jpg':
                    os.remove(th)
                if os.path.exists(f): os.remove(f) # Clean up downloaded file
            
            await c.delete_messages(d, p.id)
            
            return 'Done.'
            
        elif m.text:
            if m.text.markdown:
                await c.send_message(tcid, text=m.text.markdown, reply_parameters=rp) # <--- Perubahan di sini
                return 'Sent.'
            else:
                return 'No text found in message.'
        else: # Handle messages that are neither media nor text
            return 'Unsupported message type.'
    except Exception as e:
        # Catch-all for any unhandled errors within process_msg
        logger.error(f"Error in process_msg for user {d}: {e}", exc_info=True) # exc_info for full traceback
        # Attempt to clean up
        if 'p' in locals() and p:
            try: await c.delete_messages(d, p.id)
            except: pass
        if 'f' in locals() and f and os.path.exists(f):
            try: os.remove(f)
            except: pass
        if 'th' in locals() and th and os.path.exists(th) and th != f'{d}.jpg':
            try: os.remove(th)
            except: pass
        return f'Fatal Error: {str(e)[:50]}'


@X.on_message(filters.command(['batch', 'single']) & filters.private)
async def process_cmd(c, m):
    uid = m.from_user.id
    cmd = m.command[0]
    
    if FREEMIUM_LIMIT == 0 and not await is_premium_user(uid):
        await m.reply_text("This bot does not provide free servies, get subscription from OWNER")
        return
    
    if await sub(c, m) == 1: return
    pro = await m.reply_text('Doing some checks hold on...')
    
    if is_user_active(uid):
        await pro.edit('You have an active task. Use /stop to cancel it.')
        return
    
    ubot = await get_ubot(uid)
    if not ubot:
        await pro.edit('Add your bot with /setbot first')
        return
    
    Z[uid] = {'step': 'start' if cmd == 'batch' else 'start_single'}
    await pro.edit(f'Send {"start link..." if cmd == "batch" else "link you to process"}.')

@X.on_message(filters.command(['cancel', 'stop']) & filters.private)
async def cancel_cmd(c, m):
    uid = m.from_user.id
    if is_user_active(uid):
        if await request_batch_cancel(uid):
            await m.reply_text('Cancellation requested. The current batch will stop after the current download completes.')
        else:
            await m.reply_text('Failed to request cancellation. Please try again.')
    else:
        await m.reply_text('No active batch process found.')

@X.on_message(filters.text & filters.private & ~login_in_progress & ~filters.command([
    'start', 'batch', 'cancel', 'login', 'logout', 'stop', 'set', 
    'pay', 'redeem', 'gencode', 'single', 'generate', 'keyinfo', 'encrypt', 'decrypt', 'keys', 'setbot', 'rembot']))
async def text_handler(c, m):
    uid = m.from_user.id
    if uid not in Z: return
    s = Z[uid].get('step')

    if s == 'start':
        L = m.text
        i, d_id, lt = E(L) # d_id is start_message_id
        if not i or not d_id:
            await m.reply_text('Invalid link format.')
            Z.pop(uid, None)
            return
        Z[uid].update({'step': 'count', 'cid': i, 'sid': d_id, 'lt': lt})
        await m.reply_text('How many messages?')

    elif s == 'start_single':
        L = m.text
        i, d_id, lt = E(L) # d_id is start_message_id
        if not i or not d_id:
            await m.reply_text('Invalid link format.')
            Z.pop(uid, None)
            return

        Z[uid].update({'step': 'process_single', 'cid': i, 'sid': d_id, 'lt': lt})
        # Use local variables for clarity
        channel_id, message_id, link_type = Z[uid]['cid'], Z[uid]['sid'], Z[uid]['lt']
        
        pt = await m.reply_text('Processing...')
        
        ubot = await get_ubot(uid)
        if not ubot:
            await pt.edit('Add bot with /setbot first')
            Z.pop(uid, None)
            return
        
        uc = await get_uclient(uid)
        if not uc:
            uc = Y # Fallback to global userbot if user's client is not available
            if not uc: # If no user client and no global userbot
                await pt.edit('Cannot proceed without user client or global userbot.')
                Z.pop(uid, None)
                return
            
        if is_user_active(uid):
            await pt.edit('Active task exists. Use /stop first.')
            Z.pop(uid, None)
            return

        try:
            await add_active_batch(uid, { # Add to active users for proper state management
                "total": 1,
                "current": 0,
                "success": 0,
                "cancel_requested": False,
                "progress_message_id": pt.id
            })

            msg = await get_msg(ubot, uc, channel_id, message_id, link_type) # Use ubot for send_messages in get_msg, uc for get_messages itself
            if msg:
                res = await process_msg(ubot, uc, msg, m.chat.id, link_type, uid, channel_id) # Pass ubot as client `c`
                await pt.edit(f'1/1: {res}')
            else:
                await pt.edit('Message not found or could not be accessed.')
        except Exception as e:
            logger.error(f"Error processing single message for user {uid}: {e}")
            await pt.edit(f'Error: {str(e)[:50]}')
        finally:
            await remove_active_batch(uid) # Ensure cleanup
            Z.pop(uid, None)

    elif s == 'count':
        if not m.text.isdigit():
            await m.reply_text('Enter valid number.')
            return
        
        count = int(m.text)
        maxlimit = PREMIUM_LIMIT if await is_premium_user(uid) else FREEMIUM_LIMIT

        if count > maxlimit:
            await m.reply_text(f'Maximum limit is {maxlimit}.')
            return

        Z[uid].update({'step': 'process', 'did': m.chat.id, 'num': count})
        # Use local variables for clarity
        channel_id, start_message_id, total_messages, link_type = Z[uid]['cid'], Z[uid]['sid'], Z[uid]['num'], Z[uid]['lt']
        success_count = 0

        pt = await m.reply_text('Processing batch...')
        uc = await get_uclient(uid) # User's personal client for downloading
        ubot = await get_ubot(uid) # User's personal bot client for sending updates
        
        if not uc:
            uc = Y # Fallback to global userbot if user's client is not available
            if not uc: # If no user client and no global userbot
                await pt.edit('Cannot proceed without user client or global userbot.')
                await remove_active_batch(uid) # Clean up if setup fails
                Z.pop(uid, None)
                return
        
        if not ubot:
            ubot = c # Fallback to main bot if user's bot is not available, for progress
        
        if is_user_active(uid):
            await pt.edit('Active task exists. Use /stop first.')
            Z.pop(uid, None)
            return
        
        await add_active_batch(uid, {
            "total": total_messages,
            "current": 0,
            "success": 0,
            "cancel_requested": False,
            "progress_message_id": pt.id
            })
        
        try:
            for j in range(total_messages):
                current_message_index = j + 1
                
                if should_cancel(uid):
                    await pt.edit(f'Batch process cancelled. Processed: {current_message_index-1}/{total_messages}, Success: {success_count}.')
                    break
                
                # Update progress message periodically
                await update_batch_progress(uid, current_message_index, success_count, total_messages, pt.id, m.chat.id)
                
                message_id_to_fetch = int(start_message_id) + j
                
                try:
                    # Pass ubot as the 'bot client' (c) to get_msg for public channel checks
                    msg = await get_msg(ubot, uc, channel_id, message_id_to_fetch, link_type)
                    if msg:
                        # Pass ubot as the 'bot client' (c) to process_msg for sending updates/uploads
                        res = await process_msg(ubot, uc, msg, m.chat.id, link_type, uid, channel_id) 
                        if 'Done' in res or 'Copied' in res or 'Sent' in res:
                            success_count += 1
                        else:
                            logger.warning(f"Message {message_id_to_fetch} processing failed with result: {res}")
                    else:
                        logger.warning(f"Message {message_id_to_fetch} not found or inaccessible for user {uid}.")
                        # Optionally, inform the user if many messages are missing
                        if (j + 1) % 10 == 0: # Every 10 messages
                            try: await pt.edit(f'Warning: Message {message_id_to_fetch} not found. Continuing...')
                            except: pass # Suppress error if edit fails

                except Exception as e:
                    logger.error(f"Error processing message {message_id_to_fetch} for user {uid}: {e}")
                    try: await pt.edit(f'{current_message_index}/{total_messages}: Error - {str(e)[:50]}')
                    except: pass
                
                await asyncio.sleep(5) # Small delay to avoid hitting flood limits
            
            # Final message after loop finishes or breaks
            await update_batch_progress(uid, total_messages, success_count, total_messages, pt.id, m.chat.id)
            await m.reply_text(f'Batch completed ✅ Success: {success_count}/{total_messages}')
        
        except Exception as e:
            logger.error(f"Unhandled error in batch processing for user {uid}: {e}")
            await m.reply_text(f'An unexpected error occurred during batch processing: {str(e)}')
        finally:
            await remove_active_batch(uid)
            Z.pop(uid, None)