import os, re, time, asyncio, json
from pyrogram import Client, filters
from pyrogram.types import Message, ReplyParameters
from pyrogram.errors import UserNotParticipant, FileReferenceExpired # Tambahkan FileReferenceExpired di sini
from config import API_ID, API_HASH, LOG_GROUP, STRING, FORCE_SUB, FREEMIUM_LIMIT, PREMIUM_LIMIT
from utils.func import get_user_data, screenshot, thumbnail, get_video_metadata
from utils.func import get_user_data_key, process_text_with_rules, is_premium_user, E
from shared_client import app as X
from plugins.settings import rename_file
from plugins.start import subscribe as sub
from utils.custom_filters import login_in_progress
from utils.encrypt import dcs
from typing import Dict, Any, Optional
import logging # Tambahkan ini jika belum ada

# Konfigurasi logging (ini lebih baik daripada print)
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

Y = None if not STRING else __import__('shared_client').userbot
Z, P, UB, UC, emp = {}, {}, {}, {}, {}

ACTIVE_USERS = {}
ACTIVE_USERS_FILE = "active_users.json"

def sanitize(filename):
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
            json.dump(ACTIVE_USERS, f, indent=4)
    except Exception as e:
        logger.error(f"Error saving active users: {e}")

async def add_active_batch(user_id: int, batch_info: Dict[str, Any]):
    ACTIVE_USERS[str(user_id)] = batch_info
    await save_active_users_to_file()

def is_user_active(user_id: int) -> bool:
    return str(user_id) in ACTIVE_USERS

# =============================================================
# Pastikan fungsi ini sudah seperti ini
async def update_batch_progress(user_id: int, current: int, success: int, total: int, progress_message_id: int, chat_id: int):
    user_str = str(user_id)
    if user_str in ACTIVE_USERS:
        ACTIVE_USERS[user_str]["current"] = current
        ACTIVE_USERS[user_str]["success"] = success
        if progress_message_id and chat_id:
            try:
                percentage = (current / total * 100) if total > 0 else 0
                bar = '🟢' * int(percentage / 10) + '🔴' * (10 - int(percentage / 10))

                await X.edit_message_text( # Ini adalah kunci pembaruan progres
                    chat_id,
                    progress_message_id,
                    f"__**Batch Progress...**__\n\n{bar}\n\n✅ **Completed**: {current}/{total}\n⭐ **Success**: {success}/{total}\n\n**__Powered by Team SPY__**"
                )
            except Exception as e:
                logger.warning(f"Failed to update batch progress message for user {user_id}: {e}")
        await save_active_users_to_file()
# =============================================================

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
                    except Exception as e: logger.warning(f"Failed to join chat {i}: {e}") # Pakai logger
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
                    except Exception: # Ini mungkin terjadi jika chat_id tidak bisa diresolve langsung
                        try:
                            chat = await u.get_chat(chat_id)
                            return await u.get_messages(chat.id, d)
                        except Exception as e:
                            logger.warning(f"Could not get chat {chat_id}, trying again with more dialogs: {e}")
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
    if uid in UB and UB.get(uid) and UB.get(uid).is_connected:
        return UB.get(uid)
    try:
        bot = Client(f"user_{uid}", bot_token=bt, api_id=API_ID, api_hash=API_HASH, no_updates=True)
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
    if cl and cl.is_connected:
        return cl
    if not ud: return ubot if ubot else None
    xxx = ud.get('session_string')
    if xxx:
        try:
            ss = dcs(xxx)
            gg = Client(f'{uid}_client', api_id=API_ID, api_hash=API_HASH, device_model="v3saver", session_string=ss, no_updates=True)
            await gg.start()
            await upd_dlg(gg)
            UC[uid] = gg
            return gg
        except Exception as e:
            logger.error(f'User client error: {e}')
            return ubot if ubot else Y
    return Y

# Ini adalah fungsi prog untuk progress download/upload, TIDAK terkait langsung dengan batch progress bar
async def prog(current: int, total: int, client: Client, chat_id: int, message_id: int, start_time: float):
    global P
    if total == 0:
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

# Tidak ada perubahan pada send_direct atau process_msg
# Pastikan fungsi process_msg Anda memiliki penanganan error yang baik
# Contoh penanganan FileReferenceExpired di process_msg (jika ingin ditambahkan, tapi ini di luar cakupan "hanya progress bar")
# try:
#     f = await u.download_media(m, file_name=c_name, progress=prog, progress_args=(c, d, p.id, st))
# except FileReferenceExpired:
#     await c.edit_message_text(d, p.id, "File reference expired. Please try again with a fresh link or wait a bit.")
#     return 'Failed to download (expired reference).'
# ... sisanya dari process_msg

async def send_direct(c: Client, m: Message, tcid: int, ft: Optional[str], rp: ReplyParameters):
    # ... (kode send_direct Anda yang sudah ada)
    pass # Ini hanya placeholder

async def process_msg(c, u, m, d, lt, uid, i):
    # ... (kode process_msg Anda yang sudah ada)
    # PASTIKAN DI SINI ANDA MENGEMBALIKAN STRING SEPERTI 'Done.', 'Copied directly.', 'Sent.'
    # UNTUK KASUS BERHASIL, DAN 'Failed to download.' atau 'Failed.' UNTUK KASUS GAGAL
    pass # Ini hanya placeholder

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
        i, d_id, lt = E(L)
        if not i or not d_id:
            await m.reply_text('Invalid link format.')
            Z.pop(uid, None)
            return
        Z[uid].update({'step': 'count', 'cid': i, 'sid': d_id, 'lt': lt})
        await m.reply_text('How many messages?')

    elif s == 'start_single':
        L = m.text
        i, d_id, lt = E(L)
        if not i or not d_id:
            await m.reply_text('Invalid link format.')
            Z.pop(uid, None)
            return

        Z[uid].update({'step': 'process_single', 'cid': i, 'sid': d_id, 'lt': lt})
        channel_id, message_id, link_type = Z[uid]['cid'], Z[uid]['sid'], Z[uid]['lt']

        pt = await m.reply_text('Processing...') # Pesan progres awal

        ubot = await get_ubot(uid)
        if not ubot:
            await pt.edit('Add bot with /setbot first')
            Z.pop(uid, None)
            return

        uc = await get_uclient(uid)
        if not uc:
            uc = Y
            if not uc:
                await pt.edit('Cannot proceed without user client or global userbot.')
                Z.pop(uid, None)
                return

        if is_user_active(uid):
            await pt.edit('Active task exists. Use /stop first.')
            Z.pop(uid, None)
            return

        # =============================================================
        # PASTIKAN BAGIAN INI SUDAH BENAR
        await add_active_batch(uid, {
            "total": 1,
            "current": 0,
            "success": 0,
            "cancel_requested": False,
            "progress_message_id": pt.id, # Penting: Simpan ID pesan progres
            "chat_id": m.chat.id # Penting: Simpan chat ID
        })
        # =============================================================

        try:
            msg = await get_msg(ubot, uc, channel_id, message_id, link_type)
            if msg:
                res = await process_msg(ubot, uc, msg, m.chat.id, link_type, uid, channel_id)
                # =============================================================
                # PASTIKAN BAGIAN INI SUDAH BENAR
                # Memperbarui progres setelah proses single selesai
                await update_batch_progress(uid, 1, (1 if 'Done' in res or 'Copied' in res or 'Sent' in res else 0), 1, pt.id, m.chat.id)
                # =============================================================
                await pt.edit(f'1/1: {res}') # Ini mungkin akan diganti oleh update_batch_progress, atau tetap ada jika hanya ada 1 pesan
            else:
                await pt.edit('Message not found or could not be accessed.')
                # =============================================================
                # Pastikan progres diperbarui meskipun gagal ditemukan
                await update_batch_progress(uid, 1, 0, 1, pt.id, m.chat.id)
                # =============================================================
        except Exception as e:
            logger.error(f"Error processing single message for user {uid}: {e}")
            await pt.edit(f'Error: {str(e)[:50]}')
            # =============================================================
            # Pastikan progres diperbarui meskipun ada error
            await update_batch_progress(uid, 1, 0, 1, pt.id, m.chat.id)
            # =============================================================
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

        Z[uid].update({'step': 'process', 'did': m.chat.id, 'num': count})
        channel_id, start_message_id, total_messages, link_type = Z[uid]['cid'], Z[uid]['sid'], Z[uid]['num'], Z[uid]['lt']
        success_count = 0

        pt = await m.reply_text('Processing batch...') # Pesan progres awal untuk batch

        uc = await get_uclient(uid)
        ubot = await get_ubot(uid)

        if not uc:
            uc = Y
            if not uc:
                await pt.edit('Cannot proceed without user client or global userbot.')
                await remove_active_batch(uid)
                Z.pop(uid, None)
                return

        if not ubot:
            ubot = c

        if is_user_active(uid):
            await pt.edit('Active task exists.')
            Z.pop(uid, None)
            return

        # =============================================================
        # PASTIKAN BAGIAN INI SUDAH BENAR
        await add_active_batch(uid, {
            "total": total_messages,
            "current": 0,
            "success": 0,
            "cancel_requested": False,
            "progress_message_id": pt.id, # Penting: Simpan ID pesan progres
            "chat_id": m.chat.id # Penting: Simpan chat ID
            })
        # =============================================================

        try:
            for j in range(total_messages):
                current_message_index = j + 1

                if should_cancel(uid):
                    await pt.edit(f'Batch process cancelled. Processed: {current_message_index-1}/{total_messages}, Success: {success_count}.')
                    break

                message_id_to_fetch = int(start_message_id) + j

                try:
                    msg = await get_msg(ubot, uc, channel_id, message_id_to_fetch, link_type)
                    if msg:
                        res = await process_msg(ubot, uc, msg, m.chat.id, link_type, uid, channel_id)
                        if 'Done' in res or 'Copied' in res or 'Sent' in res:
                            success_count += 1
                        else:
                            logger.warning(f"Message {message_id_to_fetch} processing failed with result: {res}")
                    else:
                        logger.warning(f"Message {message_id_to_fetch} not found or inaccessible for user {uid}.")
                        # Anda bisa menambahkan pesan khusus jika perlu, atau biarkan update_batch_progress menanganinya
                        if (j + 1) % 10 == 0:
                            try: await pt.edit(f'Warning: Message {message_id_to_fetch} not found. Continuing...')
                            except Exception as e: logger.warning(f"Failed to update warning message: {e}") # Pakai logger

                except Exception as e:
                    logger.error(f"Error processing message {message_id_to_fetch} for user {uid}: {e}")
                    try: await pt.edit(f'{current_message_index}/{total_messages}: Error - {str(e)[:30]}')
                    except Exception as e: logger.warning(f"Failed to update error message: {e}") # Pakai logger

                # =============================================================
                # PASTIKAN BAGIAN INI SUDAH BENAR
                # Panggil update_batch_progress SETELAH setiap pesan diproses
                await update_batch_progress(uid, current_message_index, success_count, total_messages, pt.id, m.chat.id)
                # =============================================================

                await asyncio.sleep(5) # Jeda untuk menghindari flood
            # =============================================================
            # Pembaruan terakhir jika loop selesai
            await update_batch_progress(uid, total_messages, success_count, total_messages, pt.id, m.chat.id)
            # =============================================================
            await m.reply_text(f'Batch completed ✅ Success: {success_count}/{total_messages}')

        except Exception as e:
            logger.error(f"Unhandled error in batch processing for user {uid}: {e}")
            await m.reply_text(f'An unexpected error occurred during batch processing: {str(e)}')
            # =============================================================
            # Pastikan progres diperbarui meskipun ada unhandled error
            await update_batch_progress(uid, current_message_index, success_count, total_messages, pt.id, m.chat.id)
            # =============================================================
        finally:
            await remove_active_batch(uid)
            Z.pop(uid, None)