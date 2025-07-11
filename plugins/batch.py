import os, re, time, asyncio, json
from pyrogram import Client, filters
from pyrogram.types import Message, ReplyParameters
from pyrogram.errors import UserNotParticipant, MessageNotModified, FloodWait, RPCError
from config import API_ID, API_HASH, LOG_GROUP, STRING, FORCE_SUB, FREEMIUM_LIMIT, PREMIUM_LIMIT
from utils.func import get_user_data, screenshot, thumbnail, get_video_metadata
from utils.func import get_user_data_key, process_text_with_rules, is_premium_user, E
from shared_client import app as X
from plugins.settings import rename_file
from plugins.start import subscribe as sub
from utils.custom_filters import login_in_progress
from utils.encrypt import dcs
from typing import Dict, Any, Optional


Y = None if not STRING else __import__('shared_client').userbot
Z, P, UB, UC, emp = {}, {}, {}, {}, {}

ACTIVE_USERS = {}
ACTIVE_USERS_FILE = "active_users.json"

# fixed directory file_name problems
def sanitize(filename):
    # Pastikan filename tidak kosong sebelum diproses
    if not filename:
        return f"file_{int(time.time())}"
    return re.sub(r'[<>:"/\\|?*\']', '_', filename).strip(" .")[:255]

def load_active_users():
    try:
        if os.path.exists(ACTIVE_USERS_FILE):
            with open(ACTIVE_USERS_FILE, 'r') as f:
                return json.load(f)
        return {}
    except Exception:
        return {}

async def save_active_users_to_file():
    try:
        with open(ACTIVE_USERS_FILE, 'w') as f:
            json.dump(ACTIVE_USERS, f)
    except Exception as e:
        print(f"Error saving active users: {e}")

async def add_active_batch(user_id: int, batch_info: Dict[str, Any]):
    ACTIVE_USERS[str(user_id)] = batch_info
    await save_active_users_to_file()

def is_user_active(user_id: int) -> bool:
    return str(user_id) in ACTIVE_USERS

async def update_batch_progress(user_id: int, current: int, success: int):
    user_str = str(user_id)
    if user_str in ACTIVE_USERS:
        batch_info = ACTIVE_USERS[user_str]
        batch_info["current"] = current
        batch_info["success"] = success
        total = batch_info.get("total", 1) # Ambil total dari batch_info, default 1 untuk menghindari ZeroDivisionError
        progress_message_id = batch_info.get("progress_message_id")
        chat_id = batch_info.get("chat_id")

        if progress_message_id and chat_id:
            try:
                percentage = (current / total * 100) if total > 0 else 0
                bar = '🟢' * int(percentage / 10) + '🔴' * (10 - int(percentage / 10))

                await X.edit_message_text(
                    chat_id,
                    progress_message_id,
                    f"__**Batch Progress...**__\n\n{bar}\n\n✅ **Completed**: {current}/{total}\n⭐ **Success**: {success}/{total}\n\n**__Powered by Team SPY__**"
                )
            except MessageNotModified:
                # Ini adalah error yang Anda lihat, normal jika teks tidak berubah
                pass
            except Exception as e:
                print(f"Failed to update batch progress message for user {user_id}: {e}")
        await save_active_users_to_file()

async def request_batch_cancel(user_id: int):
    if str(user_id) in ACTIVE_USERS:
        ACTIVE_USERS[str(user_id)]["cancel_requested"] = True
        await save_active_users_to_file()
        return True
    return False

def should_cancel(user_id: int) -> bool:
    user_str = str(user_id)
    return user_str in ACTIVE_USERS and ACTIVE_USERS[user_str].get("cancel_requested", False)

async def remove_active_batch(user_id: int):
    if str(user_id) in ACTIVE_USERS:
        del ACTIVE_USERS[str(user_id)]
        await save_active_users_to_file()

def get_batch_info(user_id: int) -> Optional[Dict[str, Any]]:
    return ACTIVE_USERS.get(str(user_id))

ACTIVE_USERS = load_active_users()

async def upd_dlg(c):
    try:
        async for _ in c.get_dialogs(limit=100): pass
        return True
    except Exception as e:
        print(f'Failed to update dialogs: {e}')
        return False

async def get_msg(c, u, i, d, lt):
    try:
        if lt == 'public':
            try:
                xm = await c.get_messages(i, d)
                emp[i] = getattr(xm, "empty", False)
                if emp[i]:
                    try: await u.join_chat(i)
                    except Exception as join_e:
                        print(f"Could not join chat {i}: {join_e}")
                    xm = await u.get_messages((await u.get_chat(f"@{i}")).id, d)
                return xm
            except Exception as e:
                print(f'Error fetching public message: {e}')
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
                    print(f'Private channel error: {e}')
                    return None
            return None
    except Exception as e:
        print(f'Error fetching message: {e}')
        return None

async def get_ubot(uid):
    bt = await get_user_data_key(uid, "bot_token", None)
    if not bt: return None
    if uid in UB: return UB.get(uid)
    try:
        bot = Client(f"user_{uid}", bot_token=bt, api_id=API_ID, api_hash=API_HASH)
        await bot.start()
        UB[uid] = bot
        return bot
    except Exception as e:
        print(f"Error starting bot for user {uid}: {e}")
        return None

async def get_uclient(uid):
    ud = await get_user_data(uid)
    ubot = UB.get(uid)
    cl = UC.get(uid)
    if cl: return cl
    if not ud: return ubot if ubot else None
    xxx = ud.get('session_string')
    if xxx:
        try:
            ss = dcs(xxx)
            gg = Client(f'{uid}_client', api_id=API_ID, api_hash=API_HASH, device_model="v3saver", session_string=ss)
            await gg.start()
            await upd_dlg(gg)
            UC[uid] = gg
            return gg
        except Exception as e:
            print(f'User client error: {e}')
            return ubot if ubot else Y
    return Y

async def prog(c, t, C, h, m, st):
    global P
    p = c / t * 100
    interval = 10 if t >= 100 * 1024 * 1024 else 20 if t >= 50 * 1024 * 1024 else 30 if t >= 10 * 1024 * 1024 else 50
    step = int(p // interval) * interval
    if m not in P or P[m] != step or p >= 100:
        P[m] = step
        c_mb = c / (1024 * 1024)
        t_mb = t / (1024 * 1024)
        bar = '🟢' * int(p / 10) + '🔴' * (10 - int(p / 10))
        speed = c / (time.time() - st) / (1024 * 1024) if time.time() > st else 0
        eta = time.strftime('%M:%S', time.gmtime((t - c) / (speed * 1024 * 1024))) if speed > 0 else '00:00'
        try:
            await C.edit_message_text(h, m, f"__**Pyro Handler...**__\n\n{bar}\n\n⚡**__Completed__**: {c_mb:.2f} MB / {t_mb:.2f} MB\n📊 **__Done__**: {p:.2f}%\n🚀 **__Speed__**: {speed:.2f} MB/s\n⏳ **__ETA__**: {eta}\n\n**__Powered by Team SPY__**")
        except MessageNotModified:
            pass
        except Exception as e:
            print(f"Error editing progress message: {e}")
        if p >= 100: P.pop(m, None)

async def process_msg(c, u, m, d, lt, uid, i):
    try:
        cfg_chat = await get_user_data_key(d, 'chat_id', None)
        tcid = d

        rp = ReplyParameters(message_id=m.id)

        if cfg_chat:
            try:
                if '/' in cfg_chat:
                    parts = cfg_chat.split('/', 1)
                    tcid = int(parts[0])
                    rtmid_from_cfg = int(parts[1]) if len(parts) > 1 else None
                    if rtmid_from_cfg:
                        rp = ReplyParameters(message_id=rtmid_from_cfg)
                else:
                    tcid = int(cfg_chat)
                    rp = ReplyParameters(message_id=m.id)
            except ValueError:
                print(f"Invalid chat_id format '{cfg_chat}', falling back to user chat_id.")
                tcid = d
                rp = ReplyParameters(message_id=m.id)

        orig_text = m.caption.markdown if m.caption else m.text.markdown if m.text else ''
        proc_text = await process_text_with_rules(d, orig_text)
        user_cap = await get_user_data_key(d, 'caption', '')
        ft = f'{proc_text}\n\n{user_cap}' if proc_text and user_cap else user_cap if user_cap else proc_text

        # Try to copy message with reply_parameters
        if m.media or m.text:
            try:
                copied_message = await c.copy_message(
                    chat_id=tcid,
                    from_chat_id=m.chat.id,
                    message_id=m.id,
                    caption=ft if m.media else m.text.markdown,
                    reply_parameters=rp
                )
                if copied_message:
                    return 'Copied.'
            except TypeError as te: # Catch TypeError for unexpected keyword argument
                print(f"Pyrogram version might not support 'reply_parameters' in copy_message directly. Trying without it. Error: {te}")
                try:
                    # Retry without reply_parameters if TypeError occurs
                    copied_message = await c.copy_message(
                        chat_id=tcid,
                        from_chat_id=m.chat.id,
                        message_id=m.id,
                        caption=ft if m.media else m.text.markdown
                    )
                    if copied_message:
                        return 'Copied (no reply_parameters).'
                except (RPCError, FloodWait) as e:
                    print(f"Failed to copy message directly ({m.id}) due to {type(e).__name__}: {e}. Trying forwarding.")
                    if isinstance(e, FloodWait): await asyncio.sleep(e.value)
            except (RPCError, FloodWait) as e:
                print(f"Failed to copy message directly ({m.id}) due to {type(e).__name__}: {e}. Trying forwarding.")
                if isinstance(e, FloodWait): await asyncio.sleep(e.value)

            # Fallback to forwarding if copying fails
            try:
                forwarded_message = await c.forward_messages(
                    chat_id=tcid,
                    from_chat_id=m.chat.id,
                    message_ids=m.id,
                )
                if forwarded_message:
                    return 'Forwarded.'
            except (RPCError, FloodWait) as e_forward:
                print(f"Failed to forward message directly ({m.id}) due to {type(e_forward).__name__}: {e_forward}. Falling back to download/upload.")
                if isinstance(e_forward, FloodWait): await asyncio.sleep(e_forward.value)

        # Fallback to download and upload if copy/forward fails
        if m.media:
            st = time.time()
            p = await c.send_message(d, 'Downloading...')

            file_name_base = f"{time.time()}"
            ext = ""
            if m.video:
                ext = ".mp4"
                if m.video.file_name:
                    file_name_base = os.path.splitext(m.video.file_name)[0]
            elif m.audio:
                ext = ".mp3"
                if m.audio.file_name:
                    file_name_base = os.path.splitext(m.audio.file_name)[0]
            elif m.document:
                ext = os.path.splitext(m.document.file_name)[1] if m.document.file_name else ""
                if m.document.file_name:
                    file_name_base = os.path.splitext(m.document.file_name)[0]
            elif m.photo:
                ext = ".jpg"

            c_name = sanitize(file_name_base) + ext
            if not c_name:
                c_name = f"downloaded_file_{int(time.time())}{ext or '.bin'}"

            f = await u.download_media(m, file_name=c_name, progress=prog, progress_args=(c, d, p.id, st))

            if not f or not os.path.exists(f):
                await c.edit_message_text(d, p.id, 'Failed to download file.')
                return 'Failed to download.'

            await c.edit_message_text(d, p.id, 'Renaming...')
            if isinstance(f, str):
                renamed_f = await rename_file(f, d, p)
                if renamed_f and os.path.exists(renamed_f):
                    f = renamed_f
                else:
                    print(f"Renaming failed or returned invalid path for {f}. Continuing with original path.")
            else:
                print(f"Downloaded file path is not a string: {f}. Skipping rename.")

            fsize = os.path.getsize(f) / (1024 * 1024 * 1024)
            th = thumbnail(d)

            if fsize > 2 and Y:
                st = time.time()
                await c.edit_message_text(d, p.id, 'File is larger than 2GB. Using alternative method...')
                try:
                    await upd_dlg(Y)
                except Exception as e:
                    print(f"Userbot dialog update failed: {e}. Large file upload might fail.")

                dur, h, w = None, None, None
                if m.video or (isinstance(f, str) and os.path.splitext(f)[1].lower() in ['.mp4', '.mkv', '.avi']):
                    mtd = await get_video_metadata(f)
                    dur, h, w = mtd.get('duration'), mtd.get('height'), mtd.get('width')
                    th = await screenshot(f, dur or 1, d)

                sent = None
                try:
                    # Try sending with reply_parameters first
                    send_kwargs = {'caption': ft, 'progress': prog, 'progress_args': (c, d, p.id, st), 'reply_parameters': rp}
                    
                    if m.video or (isinstance(f, str) and os.path.splitext(f)[1].lower() in ['.mp4', '.mkv', '.avi']):
                        send_kwargs.update(thumb=th, duration=dur, height=h, width=w)
                        sent = await Y.send_video(tcid, f, **send_kwargs)
                    elif m.audio:
                        send_kwargs.update(thumb=th, duration=m.audio.duration, performer=m.audio.performer, title=m.audio.title)
                        sent = await Y.send_audio(tcid, f, **send_kwargs)
                    elif m.photo:
                        send_kwargs.update(thumb=th) # Photo usually doesn't need a specific thumb, but for consistency
                        sent = await Y.send_photo(tcid, f, **send_kwargs)
                    elif m.document:
                        send_kwargs.update(thumb=th)
                        sent = await Y.send_document(tcid, f, **send_kwargs)
                    else:
                        send_kwargs.update(thumb=th)
                        sent = await Y.send_document(tcid, f, **send_kwargs)

                    if sent:
                        if th and os.path.exists(th) and th != f'{d}.jpg':
                            os.remove(th)
                        os.remove(f)
                        await c.delete_messages(d, p.id)
                        return 'Done (Large file via userbot).'
                    else:
                        raise Exception("Failed to send large file via userbot.")

                except TypeError as te: # Catch TypeError for unexpected keyword argument (e.g., reply_parameters)
                    print(f"Pyrogram version might not support 'reply_parameters' in send_media. Retrying without it. Error: {te}")
                    try:
                        # Retry without reply_parameters
                        send_kwargs.pop('reply_parameters', None) # Remove it safely
                        if m.video or (isinstance(f, str) and os.path.splitext(f)[1].lower() in ['.mp4', '.mkv', '.avi']):
                            sent = await Y.send_video(tcid, f, **send_kwargs)
                        elif m.audio:
                            sent = await Y.send_audio(tcid, f, **send_kwargs)
                        elif m.photo:
                            sent = await Y.send_photo(tcid, f, **send_kwargs)
                        elif m.document:
                            sent = await Y.send_document(tcid, f, **send_kwargs)
                        else:
                            sent = await Y.send_document(tcid, f, **send_kwargs)

                        if sent:
                            if th and os.path.exists(th) and th != f'{d}.jpg':
                                os.remove(th)
                            os.remove(f)
                            await c.delete_messages(d, p.id)
                            return 'Done (Large file via userbot, no reply_parameters).'
                        else:
                            raise Exception("Failed to send large file via userbot after retry.")

                    except Exception as upload_e:
                        print(f"Large file upload failed for {f} after retry: {upload_e}")
                        await c.edit_message_text(d, p.id, f'Large file upload failed: {str(upload_e)[:50]}')
                        if th and os.path.exists(th) and th != f'{d}.jpg':
                            os.remove(th)
                        if os.path.exists(f): os.remove(f)
                        return 'Large file upload failed.'
                except Exception as upload_e:
                    print(f"Large file upload failed for {f}: {upload_e}")
                    await c.edit_message_text(d, p.id, f'Large file upload failed: {str(upload_e)[:50]}')
                    if th and os.path.exists(th) and th != f'{d}.jpg':
                        os.remove(th)
                    if os.path.exists(f): os.remove(f)
                    return 'Large file upload failed.'

            await c.edit_message_text(d, p.id, 'Uploading...')
            st = time.time()

            try:
                # Try sending with reply_parameters first
                send_kwargs = {'caption': ft, 'progress': prog, 'progress_args': (c, d, p.id, st), 'reply_parameters': rp}

                if m.video or (isinstance(f, str) and os.path.splitext(f)[1].lower() in ['.mp4', '.mkv', '.avi']):
                    mtd = await get_video_metadata(f)
                    dur, h, w = mtd.get('duration'), mtd.get('height'), mtd.get('width')
                    th_for_upload = await screenshot(f, dur or 1, d) if not th else th
                    send_kwargs.update(thumb=th_for_upload, width=w, height=h, duration=dur)
                    await c.send_video(tcid, video=f, **send_kwargs)
                elif m.video_note:
                    await c.send_video_note(tcid, video_note=f, **send_kwargs)
                elif m.voice:
                    await c.send_voice(tcid, f, **send_kwargs)
                elif m.sticker:
                    await c.send_sticker(tcid, m.sticker.file_id, reply_parameters=rp) # Stickers might be handled differently
                elif m.audio:
                    th_for_upload = th
                    send_kwargs.update(thumb=th_for_upload)
                    await c.send_audio(tcid, audio=f, **send_kwargs)
                elif m.photo:
                    th_for_upload = th
                    send_kwargs.update(thumb=th_for_upload)
                    await c.send_photo(tcid, photo=f, **send_kwargs)
                else:
                    th_for_upload = th
                    send_kwargs.update(thumb=th_for_upload)
                    await c.send_document(tcid, document=f, **send_kwargs)

            except TypeError as te: # Catch TypeError for unexpected keyword argument (e.g., reply_parameters)
                print(f"Pyrogram version might not support 'reply_parameters' in send_media. Retrying without it. Error: {te}")
                try:
                    # Retry without reply_parameters
                    send_kwargs.pop('reply_parameters', None)
                    if m.video or (isinstance(f, str) and os.path.splitext(f)[1].lower() in ['.mp4', '.mkv', '.avi']):
                        mtd = await get_video_metadata(f)
                        dur, h, w = mtd.get('duration'), mtd.get('height'), mtd.get('width')
                        th_for_upload = await screenshot(f, dur or 1, d) if not th else th
                        send_kwargs.update(thumb=th_for_upload, width=w, height=h, duration=dur)
                        await c.send_video(tcid, video=f, **send_kwargs)
                    elif m.video_note:
                        await c.send_video_note(tcid, video_note=f, **send_kwargs)
                    elif m.voice:
                        await c.send_voice(tcid, f, **send_kwargs)
                    elif m.sticker:
                        await c.send_sticker(tcid, m.sticker.file_id) # Stickers might be handled differently without RP
                    elif m.audio:
                        th_for_upload = th
                        send_kwargs.update(thumb=th_for_upload)
                        await c.send_audio(tcid, audio=f, **send_kwargs)
                    elif m.photo:
                        th_for_upload = th
                        send_kwargs.update(thumb=th_for_upload)
                        await c.send_photo(tcid, photo=f, **send_kwargs)
                    else:
                        th_for_upload = th
                        send_kwargs.update(thumb=th_for_upload)
                        await c.send_document(tcid, document=f, **send_kwargs)

                except Exception as e:
                    await c.edit_message_text(d, p.id, f'Upload failed: {str(e)[:50]}')
                    if th and os.path.exists(th) and th != f'{d}.jpg':
                        os.remove(th)
                    if os.path.exists(f): os.remove(f)
                    return 'Failed.'
            except Exception as e:
                await c.edit_message_text(d, p.id, f'Upload failed: {str(e)[:50]}')
                if th and os.path.exists(th) and th != f'{d}.jpg':
                    os.remove(th)
                if os.path.exists(f): os.remove(f)
                return 'Failed.'

            if th and os.path.exists(th) and th != f'{d}.jpg':
                os.remove(th)
            os.remove(f)
            await c.delete_messages(d, p.id)

            return 'Done (Download & Upload).'

        elif m.text:
            try:
                await c.send_message(tcid, text=ft, reply_parameters=rp)
                return 'Sent.'
            except TypeError as te: # Catch TypeError for unexpected keyword argument
                print(f"Pyrogram version might not support 'reply_parameters' in send_message. Retrying without it. Error: {te}")
                await c.send_message(tcid, text=ft)
                return 'Sent (no reply_parameters).'
            except Exception as e:
                print(f"Error sending text message: {e}")
                return f'Failed to send text: {str(e)[:50]}'
        else:
            return 'Unsupported message type.'
    except Exception as e:
        print(f"Error in process_msg for user {d}: {e}")
        return f'Fatal Error: {str(e)[:50]}'


@X.on_message(filters.command(['batch', 'single']))
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

@X.on_message(filters.command(['cancel', 'stop']))
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
        i, d, lt = E(L)
        if not i or not d:
            await m.reply_text('Invalid link format.')
            Z.pop(uid, None)
            return
        Z[uid].update({'step': 'count', 'cid': i, 'sid': d, 'lt': lt})
        await m.reply_text('How many messages?')

    elif s == 'start_single':
        L = m.text
        i, d, lt = E(L)
        if not i or not d:
            await m.reply_text('Invalid link format.')
            Z.pop(uid, None)
            return

        Z[uid].update({'step': 'process_single', 'cid': i, 'sid': d, 'lt': lt})
        channel_id, message_id, link_type = Z[uid]['cid'], Z[uid]['sid'], Z[uid]['lt']
        pt = await m.reply_text('Processing...')

        ubot = UB.get(uid)
        if not ubot:
            await pt.edit('Add bot with /setbot first')
            Z.pop(uid, None)
            return

        uc = await get_uclient(uid)
        if not uc:
            await pt.edit('Cannot proceed without user client.')
            Z.pop(uid, None)
            return

        if is_user_active(uid):
            await pt.edit('Active task exists. Use /stop first.')
            Z.pop(uid, None)
            return

        await add_active_batch(uid, {
            "total": 1,
            "current": 0,
            "success": 0,
            "cancel_requested": False,
            "progress_message_id": pt.id,
            "chat_id": m.chat.id
        })

        try:
            msg = await get_msg(ubot, uc, channel_id, message_id, link_type)
            if msg:
                res = await process_msg(ubot, uc, msg, str(m.chat.id), link_type, uid, channel_id)
                await update_batch_progress(uid, 1, (1 if any(s in res for s in ['Done', 'Copied', 'Sent', 'Forwarded']) else 0))
                await pt.edit(f'1/1: {res}')
            else:
                await pt.edit('Message not found')
                await update_batch_progress(uid, 1, 0)
        except Exception as e:
            await pt.edit(f'Error: {str(e)[:50]}')
            await update_batch_progress(uid, 1, 0)
        finally:
            await remove_active_batch(uid)
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

        Z[uid].update({'step': 'process', 'did': str(m.chat.id), 'num': count})
        i, s_id, total_messages, lt = Z[uid]['cid'], Z[uid]['sid'], Z[uid]['num'], Z[uid]['lt']
        success = 0

        pt = await m.reply_text('Processing batch...')
        uc = await get_uclient(uid)
        ubot = UB.get(uid)

        if not uc or not ubot:
            await pt.edit('Missing client setup')
            Z.pop(uid, None)
            return

        if is_user_active(uid):
            await pt.edit('Active task exists')
            Z.pop(uid, None)
            return

        await add_active_batch(uid, {
            "total": total_messages,
            "current": 0,
            "success": 0,
            "cancel_requested": False,
            "progress_message_id": pt.id,
            "chat_id": m.chat.id
            })

        try:
            for j in range(total_messages):
                current_message_index = j + 1

                if should_cancel(uid):
                    await pt.edit(f'Cancellation requested. Stopping batch after current message. Success: {success}/{current_message_index-1}')
                    break

                await update_batch_progress(uid, current_message_index - 1, success)

                mid = int(s_id) + j

                try:
                    msg = await get_msg(ubot, uc, i, mid, lt)
                    if msg:
                        res = await process_msg(ubot, uc, msg, str(m.chat.id), lt, uid, i)
                        if any(s_res in res for s_res in ['Done', 'Copied', 'Sent', 'Forwarded']):
                            success += 1
                        else:
                            print(f"Message {mid} processing failed with result: {res}")
                    else:
                        print(f"Message {mid} not found or inaccessible for user {uid}.")
                except FloodWait as fw:
                    print(f"FloodWait encountered. Sleeping for {fw.value} seconds.")
                    await pt.edit(f"FloodWait encountered. Sleeping for {fw.value} seconds before continuing.")
                    await asyncio.sleep(fw.value)
                    # Try processing the same message again after FloodWait
                    msg = await get_msg(ubot, uc, i, mid, lt)
                    if msg:
                        res = await process_msg(ubot, uc, msg, str(m.chat.id), lt, uid, i)
                        if any(s_res in res for s_res in ['Done', 'Copied', 'Sent', 'Forwarded']):
                            success += 1
                        else:
                            print(f"Message {mid} processing failed with result after FloodWait: {res}")
                    else:
                        print(f"Message {mid} still not found or inaccessible after FloodWait.")
                except Exception as e:
                    print(f"Error processing message {mid} for user {uid}: {e}")
                    try: await pt.edit(f'{current_message_index}/{total_messages}: Error - {str(e)[:30]}')
                    except MessageNotModified: pass
                    except Exception as ex: print(f"Error editing error message: {ex}")

                # Small delay between messages to prevent hitting rate limits
                await asyncio.sleep(2) # Reduced from 5 to 2 for potentially faster processing, adjust as needed

            await update_batch_progress(uid, total_messages, success)

            if not should_cancel(uid): # Only send final message if not cancelled mid-way
                await m.reply_text(f'Batch Completed ✅ Success: {success}/{total_messages}')

        except Exception as e:
            print(f"Unhandled error in batch processing for user {uid}: {e}")
            await m.reply_text(f'An unexpected error occurred during batch processing: {str(e)}')
            current_batch_info = get_batch_info(uid)
            if current_batch_info:
                await update_batch_progress(uid, current_batch_info["current"], current_batch_info["success"])
        finally:
            await remove_active_batch(uid)
            Z.pop(uid, None)