import logging
import psycopg2
from psycopg2.extras import execute_values
from psycopg2.pool import ThreadedConnectionPool  # Active pooling engine
import random
import json
import time
import os
import io
from threading import Thread
from flask import Flask
from datetime import date, datetime, timedelta, time as dt_time
from dotenv import load_dotenv
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    ApplicationBuilder,
    ContextTypes,
    CommandHandler,
    CallbackQueryHandler,
    PollAnswerHandler,
    MessageHandler,
    filters,
)

load_dotenv()

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)

# ─────────────────────────────────────────────
# CONFIGURATION CONSTANTS
# ─────────────────────────────────────────────
TOKEN = os.getenv("BOT_TOKEN")
ADMIN_ID = int(os.getenv("ADMIN_ID", "0"))
DATABASE_URL = os.getenv("DATABASE_URL") # Supabase Connection String
FREE_DAILY_LIMIT = 3
DAILY_TARGET = 10
PREMIUM_GROUP_LINK = os.getenv("PREMIUM_GROUP_LINK", "https://t.me/+fRTOlcBm97U1MjFl")

# Streak milestones that trigger a congratulation message
STREAK_MILESTONES = {
    3:   ("🌱", "3-Day Streak!", "You're building a habit. Don't break the chain."),
    7:   ("🔥", "7-Day Streak!", "One full week of consistent practice. Most students quit before this."),
    14:  ("⚡", "14-Day Streak!", "Two weeks straight. You're in the top tier of dedication."),
    30:  ("🏆", "30-Day Streak!", "A full month of daily practice. This is how toppers are made."),
    60:  ("👑", "60-Day Streak!", "Two months. Your consistency is extraordinary."),
    100: ("💎", "100-Day Streak!", "100 days. You are the standard other students measure themselves against."),
}


# ─────────────────────────────────────────────
# DATABASE POOL ENGINE & PROXY MECHANISM
# ─────────────────────────────────────────────

try:
    # Initializes 15 temporary connections kept active in memory
    db_pool = ThreadedConnectionPool(
        minconn=2,
        maxconn=15,
        dsn=DATABASE_URL
    )
    logging.info("Database connection pool initialized successfully.")
except Exception as e:
    logging.error(f"Failed to initialize connection pool: {e}")
    db_pool = None


class ConnectionProxy:
    """
    Intercepts the traditional conn.close() calls from the old architecture
    and recycles the connection back to the active thread pool.
    """
    def __init__(self, conn, pool):
        self._conn = conn
        self._pool = pool

    def cursor(self, *args, **kwargs):
        return self._conn.cursor(*args, **kwargs)

    def commit(self):
        return self._conn.commit()

    def rollback(self):
        return self._conn.rollback()

    def close(self):
        if self._pool and self._conn:
            self._pool.putconn(self._conn)  # Returned cleanly to the pool
            self._conn = None
        elif self._conn:
            self._conn.close()
            self._conn = None

    def __getattr__(self, name):
        return getattr(self._conn, name)


def get_conn():
    if db_pool:
        return ConnectionProxy(db_pool.getconn(), db_pool)
    return psycopg2.connect(DATABASE_URL)


def init_db():
    conn = get_conn()
    cursor = conn.cursor()
    try:
        cursor.execute("""CREATE TABLE IF NOT EXISTS users (
            user_id                 BIGINT PRIMARY KEY,
            status                  TEXT    DEFAULT 'free',
            last_date               TEXT,
            count_today             INTEGER DEFAULT 0,
            expiry_date             TEXT,
            streak                  INTEGER DEFAULT 0,
            last_milestone_notified INTEGER DEFAULT 0,
            referral_count          INTEGER DEFAULT 0
        )""")

        cursor.execute("""CREATE TABLE IF NOT EXISTS questions (
            id          SERIAL PRIMARY KEY,
            subject     TEXT,
            topic       TEXT,
            question    TEXT,
            opt1        TEXT,
            opt2        TEXT,
            opt3        TEXT,
            opt4        TEXT,
            correct     INTEGER,
            explanation TEXT
        )""")

        cursor.execute("""CREATE TABLE IF NOT EXISTS user_sessions (
            user_id       BIGINT PRIMARY KEY,
            mode          TEXT,
            topic         TEXT,
            current_index INTEGER,
            score         INTEGER,
            question_ids  TEXT,
            start_time    INTEGER
        )""")

        cursor.execute("""CREATE TABLE IF NOT EXISTS stats (
            user_id        BIGINT PRIMARY KEY,
            name           TEXT,
            total_score    INTEGER DEFAULT 0,
            total_attempted INTEGER DEFAULT 0
        )""")

        cursor.execute("""CREATE TABLE IF NOT EXISTS user_history (
            user_id     BIGINT,
            question_id INTEGER,
            is_correct  INTEGER DEFAULT 0,
            answered    INTEGER DEFAULT 0,
            PRIMARY KEY (user_id, question_id)
        )""")

        cursor.execute("""CREATE TABLE IF NOT EXISTS user_bookmarks (
            user_id     BIGINT,
            question_id INTEGER,
            PRIMARY KEY (user_id, question_id)
        )""")

        cursor.execute("""CREATE TABLE IF NOT EXISTS poll_tracker (
            poll_id        TEXT PRIMARY KEY,
            user_id        BIGINT,
            question_id    INTEGER,
            correct_option INTEGER
        )""")

        conn.commit()

        migrations = [
            "ALTER TABLE users ADD COLUMN last_milestone_notified INTEGER DEFAULT 0",
            "ALTER TABLE users ADD COLUMN referral_count INTEGER DEFAULT 0",
        ]
        for sql in migrations:
            try:
                cursor.execute(sql)
                conn.commit()
            except Exception:
                conn.rollback()

        cursor.execute("SELECT COUNT(*) FROM questions")
        if cursor.fetchone()[0] == 0:
            try:
                with open("questions.json", "r", encoding="utf-8") as f:
                    q_list = json.load(f)
                for q in q_list:
                    cursor.execute(
                        """INSERT INTO questions
                           (subject, topic, question, opt1, opt2, opt3, opt4, correct, explanation)
                           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                        (q["subject"], q["topic"], q["question"],
                         q["opt1"], q["opt2"], q["opt3"], q["opt4"],
                         q["correct"], q["explanation"]),
                    )
                conn.commit()
                print(f"Loaded {len(q_list)} questions successfully into Supabase.")
            except FileNotFoundError:
                print("Warning: questions.json file not found.")
    finally:
        conn.close()


# ─────────────────────────────────────────────
# CORE HELPERS
# ─────────────────────────────────────────────

def make_bar(pct, width=10):
    filled = int(min(pct, 100) / 100 * width)
    return "█" * filled + "░" * (width - filled)


def get_user_status(user_id, name="User"):
    conn = get_conn()
    cursor = conn.cursor()
    try:
        cursor.execute(
            "SELECT status, count_today, last_date, expiry_date, streak, last_milestone_notified FROM users WHERE user_id = %s",
            (user_id,)
        )
        row = cursor.fetchone()
        today_str = str(date.today())
        yesterday_str = str(date.today() - timedelta(days=1))

        if not row:
            cursor.execute(
                """INSERT INTO users (user_id, status, last_date, count_today, expiry_date, streak, last_milestone_notified, referral_count)
                   VALUES (%s, 'free', %s, 0, NULL, 1, 0, 0)""",
                (user_id, today_str)
            )
            cursor.execute(
                "INSERT INTO stats (user_id, name, total_score, total_attempted) VALUES (%s, %s, 0, 0) ON CONFLICT (user_id) DO NOTHING",
                (user_id, name)
            )
            conn.commit()
            return "free", 0, None, 1, None

        status, count, last_date, expiry_date, streak, last_notified = row
        new_count = count
        new_streak = streak or 0
        last_notified = last_notified or 0
        milestone_reached = None

        if last_date != today_str:
            new_count = 0
            new_streak = (new_streak + 1) if last_date == yesterday_str else 1
            cursor.execute(
                "UPDATE users SET last_date = %s, count_today = 0, streak = %s WHERE user_id = %s",
                (today_str, new_streak, user_id)
            )
            conn.commit()

        achieved = [m for m in STREAK_MILESTONES if new_streak >= m and m > last_notified]
        if achieved:
            top_milestone = max(achieved)
            cursor.execute(
                "UPDATE users SET last_milestone_notified = %s WHERE user_id = %s",
                (top_milestone, user_id)
            )
            conn.commit()
            milestone_reached = top_milestone

        return status, new_count, expiry_date, new_streak, milestone_reached
    finally:
        conn.close()


def get_topic_keyboard(subject):
    conn = get_conn()
    cursor = conn.cursor()
    try:
        cursor.execute(
            "SELECT DISTINCT topic FROM questions WHERE subject = %s ORDER BY topic",
            (subject,)
        )
        topics = [r[0] for r in cursor.fetchall()]
    finally:
        conn.close()

    buttons = [
        [InlineKeyboardButton("📚 All Topics", callback_data=f"setmode_{subject}_all")]
    ]
    row = []
    for topic in topics:
        cb = f"setmode_{subject}_{topic}"[:64]
        row.append(InlineKeyboardButton(f"• {topic}", callback_data=cb))
        if len(row) == 2:
            buttons.append(row)
            row = []
    if row:
        buttons.append(row)
    buttons.append([InlineKeyboardButton("⬅️ Back to Menu", callback_data="quit_test")])
    return InlineKeyboardMarkup(buttons)


def get_main_menu_keyboard():
    keyboard = [
        [
            InlineKeyboardButton("📁 Physics", callback_data="menu_physics"),
            InlineKeyboardButton("📁 Chemistry", callback_data="menu_chemistry"),
        ],
        [
            InlineKeyboardButton("📁 Biology", callback_data="menu_biology"),
            InlineKeyboardButton("🎲 Mixed Quiz", callback_data="menu_mixed"),
        ],
        [
            InlineKeyboardButton("🏆 Custom Mock", callback_data="menu_mock"),
            InlineKeyboardButton("⭐ Bookmarks", callback_data="menu_viewbooks"),
        ],
        [
            InlineKeyboardButton("👑 Leaderboard", callback_data="menu_leader"),
            InlineKeyboardButton("📊 Dashboard", callback_data="menu_dash"),
        ],
        [
            InlineKeyboardButton("💬 Support & Feedback", callback_data="menu_support")
        ],
        [InlineKeyboardButton("💎 Unlock Premium", callback_data="menu_premium")],
    ]
    return InlineKeyboardMarkup(keyboard)


async def export_users(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if user_id != ADMIN_ID:
        await update.message.reply_text("⛔ Unauthorized access!")
        return

    conn = get_conn()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT u.user_id, s.name, u.status, u.streak
            FROM users u
            LEFT JOIN stats s ON u.user_id = s.user_id
        """)
        rows = cursor.fetchall()
    finally:
        conn.close()

    if not rows:
        await update.message.reply_text("No students registered yet.")
        return

    report = f"==== NEET PREP ENGINE: LIVE USER REPORT (Total: {len(rows)}) ====\n\n"
    for idx, row in enumerate(rows, 1):
        tg_id = row[0]
        name = row[1] if row[1] else "Unknown Student"
        status = row[2].upper()
        streak = row[3] if row[3] else 0
        report += f"{idx}. ID: {tg_id} | Name: {name} | Plan: {status} | Streak: {streak} days\n"

    file_data = io.BytesIO(report.encode('utf-8'))
    file_data.name = "neet_students_report.txt"

    await update.message.reply_document(
        document=file_data, 
        caption=f"📊 Here is the detailed report of your {len(rows)} students."
    )


# ─────────────────────────────────────────────
# TELEGRAM COMMAND & NAVIGATION HANDLERS
# ─────────────────────────────────────────────

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if context.args and context.args[0].startswith("ref_"):
        try:
            referrer_id = int(context.args[0].split("_")[1])
            if referrer_id != user.id:
                conn = get_conn()
                cursor = conn.cursor()
                try:
                    cursor.execute("SELECT status, expiry_date FROM users WHERE user_id = %s", (referrer_id,))
                    ref_row = cursor.fetchone()
                    if ref_row:
                        ref_status, ref_expiry = ref_row
                        today_dt = date.today()
                        if ref_status == "premium" and ref_expiry:
                            current_expiry = datetime.strptime(ref_expiry, "%Y-%m-%d").date()
                            new_expiry = max(current_expiry, today_dt) + timedelta(days=3)
                        else:
                            new_expiry = today_dt + timedelta(days=3)
                        
                        new_expiry_str = new_expiry.strftime("%Y-%m-%d")
                        cursor.execute(
                            "UPDATE users SET status = 'premium', expiry_date = %s WHERE user_id = %s",
                            (new_expiry_str, referrer_id)
                        )
                        cursor.execute(
                            "UPDATE users SET referral_count = referral_count + 1 WHERE user_id = %s",
                            (referrer_id,)
                        )
                        conn.commit()
                        try:
                            await context.bot.send_message(
                                chat_id=referrer_id,
                                text=f"🎁 **Referral Earned!** A student joined via your link. Premium plan added/extended 3 days until `{new_expiry_str}`!",
                                parse_mode="Markdown"
                            )
                        except Exception:
                            pass
                finally:
                    conn.close()
        except Exception as e:
            logging.error(f"Referral deep linking system error: {e}")

    _, _, _, streak, milestone = get_user_status(user.id, user.full_name)
    context.user_data.pop("awaiting_count_mode", None)
    context.user_data.pop("awaiting_topic", None)
    context.user_data.pop("awaiting_support_msg", None)

    if milestone and milestone in STREAK_MILESTONES:
        icon, title, body = STREAK_MILESTONES[milestone]
        await update.message.reply_text(
            f"{icon} **{title}**\n\n{body}",
            parse_mode="Markdown"
        )

    streak_line = f"🔥 Streak: **{streak} day{'s' if streak != 1 else ''}** — keep it going!\n\n" if streak > 1 else ""

    welcome = (
        f"┌──────────────────────────┐\n"
        f"    **NEET PREP ENGINE v4.5**\n"
        f"└──────────────────────────┘\n\n"
        f"Hello, **{user.first_name}**!\n\n"
        f"{streak_line}"
        f"**What's available:**\n"
        f"• NEET marking (+4 / -1)\n"
        f"• Topic-wise practice\n"
        f"• Weakness detection\n"
        f"• Non-repeating questions\n\n"
        f"Choose a subject or mode:"
    )
    await update.message.reply_text(
        welcome, parse_mode="Markdown", reply_markup=get_main_menu_keyboard()
    )


async def handle_menu_clicks(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    data = query.data

    status, today_count, expiry_date, streak, _ = get_user_status(user_id)

    exempt = {
        "menu_dash", "menu_premium", "menu_leader", "menu_support",
        "menu_viewbooks", "quit_test", "reset_history", "mistake_vault",
        "next_question", "next_bookmark", "weak_retry",
        "menu_physics", "menu_chemistry", "menu_biology",
        "session_continue", "session_restart",
    }
    is_browsing = (
        data in exempt
        or data.startswith("book_")
        or data.startswith("delbk_")
        or data.startswith("setmode_")
        or data.startswith("report_")
    )

    if status == "free" and today_count >= FREE_DAILY_LIMIT and not is_browsing:
        await context.bot.send_message(
            chat_id=user_id,
            text=(
                f"⚠️ **Daily Limit Reached**\n\n"
                f"You've used your {FREE_DAILY_LIMIT} free questions for today.\n\n"
                f"👑 **Premium gives you:**\n"
                f"• Unlimited questions\n"
                f"• Full topic-wise drills\n"
                f"• Complete analytics\n\n"
                f"Only ₹49/month."
            ),
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("💎 Upgrade Now", callback_data="menu_premium")]
            ])
        )
        return

    conn = get_conn()
    cursor = conn.cursor()
    try:
        if data == "session_continue":
            await send_next_session_question(context, user_id)
            return

        if data == "session_restart":
            pending = context.user_data.pop("pending_session", None)
            if not pending:
                await context.bot.send_message(chat_id=user_id, text="Session data lost. Please start again.", reply_markup=get_main_menu_keyboard())
                return
            ids_str = pending["ids_str"]
            mode = pending["mode"]
            topic = pending["topic"]
            cursor.execute(
                """INSERT INTO user_sessions (user_id, mode, topic, current_index, score, question_ids, start_time)
                   VALUES (%s, %s, %s, 0, 0, %s, %s)
                   ON CONFLICT (user_id) DO UPDATE SET
                   mode=EXCLUDED.mode, topic=EXCLUDED.topic, current_index=0, score=0, question_ids=EXCLUDED.question_ids, start_time=EXCLUDED.start_time""",
                (user_id, mode, topic, ids_str, int(time.time()))
            )
            conn.commit()
            topic_label = f" — {topic}" if topic != "all" else ""
            q_count = len(ids_str.split(","))
            await context.bot.send_message(
                chat_id=user_id,
                text=f"🚀 **New session started!** {q_count} questions loaded ({mode.upper()}{topic_label}). Good luck!",
                parse_mode="Markdown"
            )
            await send_next_session_question(context, user_id)
            return

        if data == "menu_support":
            context.user_data['awaiting_support_msg'] = True
            await context.bot.send_message(
                chat_id=user_id,
                text="Please type your problem, bug report, or improvement suggestion below and send it as a text message.",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="quit_test")]])
            )
            return

        if data.startswith("report_"):
            q_id = int(data.split("_")[1])
            user = query.from_user
            try:
                await context.bot.send_message(
                    chat_id=ADMIN_ID,
                    text=f"🚨 **Bug Report**\n\nUser: {user.full_name} (@{user.username if user.username else 'N/A'}, ID: `{user.id}`)\nhas flagged **Question ID #{q_id}** for review.",
                    parse_mode="Markdown"
                )
            except Exception as ex:
                logging.error(f"Failed to transmit report data trace to admin context: {ex}")
            await context.bot.send_message(chat_id=user_id, text="📥 Thank you! Your report has been submitted.")
            return

        if data == "mistake_vault":
            cursor.execute(
                "SELECT question_id FROM user_history WHERE user_id = %s AND is_correct = 0 AND answered = 1",
                (user_id,)
            )
            rows = cursor.fetchall()
            if not rows:
                await context.bot.send_message(chat_id=user_id, text="Excellent! Your mistake vault is empty.", reply_markup=get_main_menu_keyboard())
                return
            
            q_ids_str = ",".join(str(r[0]) for r in rows)
            cursor.execute(
                """INSERT INTO user_sessions (user_id, mode, topic, current_index, score, question_ids, start_time)
                   VALUES (%s, 'mistake_review', 'all', 0, 0, %s, %s)
                   ON CONFLICT (user_id) DO UPDATE SET 
                   mode=EXCLUDED.mode, topic=EXCLUDED.topic, current_index=0, score=0, question_ids=EXCLUDED.question_ids, start_time=EXCLUDED.start_time""",
                (user_id, q_ids_str, int(time.time()))
            )
            conn.commit()
            await context.bot.send_message(chat_id=user_id, text="❌ **Entering Mistake Vault...**\nReviewing previously failed items.")
            await send_next_session_question(context, user_id)
            return

        if data in ["menu_physics", "menu_chemistry", "menu_biology"]:
            subject = data.split("_")[1]
            await context.bot.send_message(
                chat_id=user_id, text=f"📂 **{subject.upper()} — Choose Topic**\n\nPractice a specific chapter or all at once:",
                parse_mode="Markdown", reply_markup=get_topic_keyboard(subject)
            )
            return

        if data.startswith("setmode_"):
            parts = data.split("_", 2)
            subject = parts[1]
            topic = parts[2] if len(parts) > 2 else "all"
            context.user_data["awaiting_count_mode"] = subject
            context.user_data["awaiting_topic"] = topic
            label = "All Topics" if topic == "all" else topic
            await context.bot.send_message(
                chat_id=user_id, text=f"📂 **{subject.upper()} — {label}**\n\nHow many questions? (e.g. 5, 10, 30):",
                parse_mode="Markdown", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="quit_test")]])
            )
            return

        if data in ["menu_mixed", "menu_mock"]:
            mode = data.split("_")[1]
            context.user_data["awaiting_count_mode"] = mode
            context.user_data["awaiting_topic"] = "all"
            label = "Mixed Quiz" if mode == "mixed" else "Custom Mock Test"
            await context.bot.send_message(
                chat_id=user_id, text=f"🎯 **{label}**\n\nHow many questions? (e.g. 10, 45, 90):",
                parse_mode="Markdown", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="quit_test")]])
            )
            return

        if data == "menu_viewbooks":
            cursor.execute("SELECT question_id FROM user_bookmarks WHERE user_id = %s", (user_id,))
            bk_rows = cursor.fetchall()
            if not bk_rows:
                await context.bot.send_message(
                    chat_id=user_id, text="📝 **No Bookmarks Yet**\n\nSave questions during practice using the ⭐ button.",
                    parse_mode="Markdown", reply_markup=get_main_menu_keyboard()
                )
                return
            bk_ids = ",".join(str(r[0]) for r in bk_rows)
            cursor.execute(
                """INSERT INTO user_sessions (user_id, mode, topic, current_index, score, question_ids, start_time)
                   VALUES (%s, 'bookmark_review', 'all', 0, 0, %s, %s)
                   ON CONFLICT (user_id) DO UPDATE SET 
                   mode=EXCLUDED.mode, topic=EXCLUDED.topic, current_index=0, score=0, question_ids=EXCLUDED.question_ids, start_time=EXCLUDED.start_time""",
                (user_id, bk_ids, int(time.time()))
            )
            conn.commit()
            await send_next_bookmark_question(context, user_id)
            return

        if data == "menu_leader":
            cursor.execute("SELECT name, total_score FROM stats ORDER BY total_score DESC LIMIT 10")
            leaders = cursor.fetchall()
            text = "🏆 **GLOBAL LEADERBOARD**\n\n"
            if not leaders:
                text += "No scores yet."
            else:
                medals = ["🥇", "🥈", "🥉"]
                for i, (name, score) in enumerate(leaders):
                    medal = medals[i] if i < 3 else f"{i+1}."
                    text += f"{medal} **{name}** —  `{score} pts`\n"
            await context.bot.send_message(chat_id=user_id, text=text, parse_mode="Markdown", reply_markup=get_main_menu_keyboard())
            return

        if data == "menu_dash":
            cursor.execute(
                """SELECT q.subject, q.topic, COUNT(h.question_id), SUM(h.is_correct), SUM(h.answered)
                   FROM user_history h
                   JOIN questions q ON h.question_id = q.id
                   WHERE h.user_id = %s
                   GROUP BY q.subject, q.topic""",
                (user_id,)
            )
            rows = cursor.fetchall()

            subject_data = {}
            for subject, topic, total, correct, answered in rows:
                if subject not in subject_data:
                    subject_data[subject] = {"total": 0, "correct": 0, "answered": 0, "topics": []}
                subject_data[subject]["total"] += total
                subject_data[subject]["correct"] += (correct or 0)
                subject_data[subject]["answered"] += (answered or 0)
                if (answered or 0) >= 3:
                    acc = (correct or 0) / answered * 100
                    subject_data[subject]["topics"].append((topic, answered, acc))

            analytics_str = ""
            weak_topic = None
            weak_acc = 101.0

            for subj, d in subject_data.items():
                sub_acc = (d["correct"] / d["answered"] * 100) if d["answered"] > 0 else 0.0
                bar = make_bar(sub_acc)
                analytics_str += f"\n🔹 **{subj.upper()}** {bar} `{sub_acc:.0f}%`\n"
                for topic, t_ans, t_acc in sorted(d["topics"], key=lambda x: x[2]):
                    analytics_str += f"    ↳ {topic}: `{t_acc:.0f}%` ({t_ans} done)\n"
                    if t_acc < weak_acc and t_ans >= 3:
                        weak_acc = t_acc
                        weak_topic = (subj, topic)

            cursor.execute("SELECT COUNT(*) FROM user_bookmarks WHERE user_id = %s", (user_id,))
            bk_count = cursor.fetchone()[0]

            cursor.execute("SELECT total_score FROM stats WHERE user_id = %s", (user_id,))
            pts_row = cursor.fetchone()
            global_pts = pts_row[0] if pts_row else 0

            goal_pct = (today_count / DAILY_TARGET) * 100
            goal_bar = make_bar(goal_pct)
            goal_str = f"Daily Goal Progress: {today_count}/{DAILY_TARGET} Questions [{goal_bar}] {min(int(goal_pct), 100)}%"

            dash_buttons = []
            if weak_topic:
                context.user_data["weak_topic"] = weak_topic
                dash_buttons.append([InlineKeyboardButton(
                    f"⚡ Drill Weakest: {weak_topic[1]}",
                    callback_data="weak_retry"
                )])
            else:
                dash_buttons.append([InlineKeyboardButton("🎯 Retry Weakest Topic", callback_data="weak_retry")])
            dash_buttons.append([InlineKeyboardButton("❌ Mistake Vault", callback_data="mistake_vault")])
            dash_buttons.append([InlineKeyboardButton("🔄 Reset Progress", callback_data="reset_history")])
            dash_buttons.append([InlineKeyboardButton("⬅️ Main Menu", callback_data="quit_test")])

            dash_text = (
                f"📊 **DASHBOARD**\n\n"
                f"• Plan: `{status.upper()}`\n"
                f"• Score: `{global_pts} pts`\n"
                f"• Streak: `{streak} day{'s' if streak != 1 else ''} 🔥`\n"
                f"• Bookmarks: `{bk_count}`\n\n"
                f"📈 **DAILY TARGET METRIC:**\n"
                f"• {goal_str}\n\n"
                f"**ACCURACY BY TOPIC:**\n"
                f"{analytics_str}\n"
                f"_Solved questions are hidden. Reset to recycle them._"
            )
            await context.bot.send_message(chat_id=user_id, text=dash_text, parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(dash_buttons))
            return

        if data == "weak_retry":
            cursor.execute(
                """SELECT q.subject, q.topic, SUM(h.is_correct), SUM(h.answered)
                   FROM user_history h
                   JOIN questions q ON h.question_id = q.id
                   WHERE h.user_id = %s AND h.answered = 1
                   GROUP BY q.subject, q.topic""",
                (user_id,)
            )
            hist_rows = cursor.fetchall()
            if not hist_rows:
                await context.bot.send_message(chat_id=user_id, text="⚠️ No practice history found yet!", reply_markup=get_main_menu_keyboard())
                return
            
            weak_topic = None
            weak_acc = 101.0
            for subj, topic, correct, answered in hist_rows:
                acc = (correct / answered) * 100 if answered > 0 else 0.0
                if acc < weak_acc:
                    weak_acc = acc
                    weak_topic = (subj, topic)
            
            if weak_topic:
                subj, topic = weak_topic
                cursor.execute(
                    """SELECT id FROM questions WHERE subject = %s AND topic = %s
                       AND id NOT IN (SELECT question_id FROM user_history WHERE user_id = %s AND is_correct = 1)
                       ORDER BY RANDOM() LIMIT 10""",
                    (subj, topic, user_id)
                )
                q_ids = [r[0] for r in cursor.fetchall()]
                if not q_ids:
                    cursor.execute("SELECT id FROM questions WHERE subject = %s AND topic = %s ORDER BY RANDOM() LIMIT 10", (subj, topic))
                    q_ids = [r[0] for r in cursor.fetchall()]
                
                ids_str = ",".join(map(str, q_ids))
                cursor.execute(
                    """INSERT INTO user_sessions (user_id, mode, topic, current_index, score, question_ids, start_time)
                       VALUES (%s, %s, %s, 0, 0, %s, %s)
                       ON CONFLICT (user_id) DO UPDATE SET 
                       mode=EXCLUDED.mode, topic=EXCLUDED.topic, current_index=0, score=0, question_ids=EXCLUDED.question_ids, start_time=EXCLUDED.start_time""",
                    (user_id, subj, topic, ids_str, int(time.time()))
                )
                conn.commit()
                await context.bot.send_message(
                    chat_id=user_id, text=f"🎯 **Weakest Topic Session Configured!**\nTarget: `{topic}` ({subj.upper()})", parse_mode="Markdown"
                )
                await send_next_session_question(context, user_id)
            return

        if data == "menu_premium":
            if status == "premium" and expiry_date:
                exp_dt = datetime.strptime(expiry_date, "%Y-%m-%d").date()
                days_left = (exp_dt - date.today()).days
                text = f"👑 **PREMIUM ACTIVE**\n\n• Days remaining: **{days_left}**\n• Expires: `{expiry_date}`"
            else:
                text = (
                    f"💎 **UPGRADE TO PREMIUM**\n\n• Unlimited questions daily\n• Chapter-wise topic drills\n• Full analytics\n\n"
                    f"💳 **Price:** ₹49 / month\n👉 **UPI:** `karan87@jio`\n\nSend payment screenshot here."
                )
            await context.bot.send_message(chat_id=user_id, text=text, parse_mode="Markdown", reply_markup=get_main_menu_keyboard())
            return

        if data.startswith("book_"):
            q_id = int(data.split("_")[1])
            cursor.execute("INSERT INTO user_bookmarks (user_id, question_id) VALUES (%s, %s) ON CONFLICT DO NOTHING", (user_id, q_id))
            conn.commit()
            await context.bot.send_message(chat_id=user_id, text="⭐ Saved to bookmarks.")
            return

        if data.startswith("delbk_"):
            q_id = int(data.split("_")[1])
            cursor.execute("DELETE FROM user_bookmarks WHERE user_id = %s AND question_id = %s", (user_id, q_id))
            conn.commit()
            await context.bot.send_message(chat_id=user_id, text="🗑️ Removed from bookmarks.")
            await send_next_bookmark_question(context, user_id)
            return

        if data == "next_question":
            await send_next_session_question(context, user_id)
            return

        if data == "next_bookmark":
            await send_next_bookmark_question(context, user_id)
            return

        if data == "quit_test":
            context.user_data.pop("awaiting_count_mode", None)
            context.user_data.pop("awaiting_topic", None)
            context.user_data.pop("awaiting_support_msg", None)
            cursor.execute("DELETE FROM user_sessions WHERE user_id = %s", (user_id,))
            conn.commit()
            await context.bot.send_message(chat_id=user_id, text="Session ended.", reply_markup=get_main_menu_keyboard())
            return

        if data == "reset_history":
            cursor.execute("DELETE FROM user_history WHERE user_id = %s", (user_id,))
            conn.commit()
            await context.bot.send_message(chat_id=user_id, text="🔄 **Progress reset!** Questions recycled.", parse_mode="Markdown", reply_markup=get_main_menu_keyboard())
            return
    finally:
        conn.close()


async def handle_text_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    user = update.effective_user
    
    if context.user_data.get('awaiting_support_msg'):
        support_text = update.message.text
        header = f"💬 **SUPPORT SUBMISSION**\n\n• User: {user.full_name} (`{user.id}`)\n\n**Message:**\n{support_text}"
        try:
            await context.bot.send_message(chat_id=ADMIN_ID, text=header, parse_mode="Markdown")
        except Exception as e:
            logging.error(f"Failed to forward support packet: {e}")
        context.user_data.pop('awaiting_support_msg', None)
        await update.message.reply_text("📥 Your feedback has been transmitted. Thank you!")
        return

    mode = context.user_data.get("awaiting_count_mode")
    if not mode:
        return

    text_input = update.message.text.strip()
    try:
        requested_count = int(text_input)
        if requested_count <= 0:
            await update.message.reply_text("⚠️ Please enter a number greater than 0.")
            return
    except ValueError:
        await update.message.reply_text("⚠️ That doesn't look like a number. Try again:")
        return

    topic = context.user_data.pop("awaiting_topic", "all")
    context.user_data.pop("awaiting_count_mode", None)

    conn = get_conn()
    cursor = conn.cursor()
    try:
        cursor.execute(
            "SELECT current_index, question_ids FROM user_sessions WHERE user_id = %s",
            (user_id,)
        )
        active = cursor.fetchone()
        if active:
            curr_idx, ids_str_active = active
            total = len(ids_str_active.split(","))
            if curr_idx < total:
                if mode in ["physics", "chemistry", "biology"]:
                    if topic == "all":
                        cursor.execute(
                            "SELECT id FROM questions WHERE subject = %s AND id NOT IN (SELECT question_id FROM user_history WHERE user_id = %s) ORDER BY RANDOM()",
                            (mode, user_id)
                        )
                    else:
                        cursor.execute(
                            "SELECT id FROM questions WHERE subject = %s AND topic = %s AND id NOT IN (SELECT question_id FROM user_history WHERE user_id = %s) ORDER BY RANDOM()",
                            (mode, topic, user_id)
                        )
                    new_ids = [r[0] for r in cursor.fetchall()][:requested_count]
                elif mode == "mixed":
                    cursor.execute("SELECT id FROM questions WHERE id NOT IN (SELECT question_id FROM user_history WHERE user_id = %s) ORDER BY RANDOM()", (user_id,))
                    new_ids = [r[0] for r in cursor.fetchall()][:requested_count]
                elif mode == "mock":
                    p_count = max(1, int(requested_count * 0.25))
                    c_count = max(1, int(requested_count * 0.25))
                    b_count = max(1, requested_count - p_count - c_count)
                    def fetch_ids_mock(subject, limit):
                        cursor.execute("SELECT id FROM questions WHERE subject = %s AND id NOT IN (SELECT question_id FROM user_history WHERE user_id = %s) ORDER BY RANDOM() LIMIT %s", (subject, user_id, limit))
                        return [r[0] for r in cursor.fetchall()]
                    new_ids = fetch_ids_mock("physics", p_count) + fetch_ids_mock("chemistry", c_count) + fetch_ids_mock("biology", b_count)
                    random.shuffle(new_ids)
                else:
                    new_ids = []

                if not new_ids:
                    await update.message.reply_text("⚠️ No unseen questions available. Reset progress to recycle.", reply_markup=get_main_menu_keyboard())
                    return

                context.user_data["pending_session"] = {
                    "mode": mode,
                    "topic": topic,
                    "ids_str": ",".join(map(str, new_ids)),
                }
                remaining = total - curr_idx
                await update.message.reply_text(
                    f"⚠️ **Active Session Detected**\n\n"
                    f"You have **{remaining}** questions remaining in your current session.\n\n"
                    f"What would you like to do?",
                    parse_mode="Markdown",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("▶️ Continue Current Session", callback_data="session_continue")],
                        [InlineKeyboardButton("🔄 Discard & Start New", callback_data="session_restart")],
                        [InlineKeyboardButton("❌ Cancel", callback_data="quit_test")],
                    ])
                )
                return

        if mode in ["physics", "chemistry", "biology"]:
            if topic == "all":
                cursor.execute(
                    "SELECT id FROM questions WHERE subject = %s AND id NOT IN (SELECT question_id FROM user_history WHERE user_id = %s) ORDER BY RANDOM()",
                    (mode, user_id)
                )
            else:
                cursor.execute(
                    "SELECT id FROM questions WHERE subject = %s AND topic = %s AND id NOT IN (SELECT question_id FROM user_history WHERE user_id = %s) ORDER BY RANDOM()",
                    (mode, topic, user_id)
                )
            q_ids = [r[0] for r in cursor.fetchall()][:requested_count]

        elif mode == "mixed":
            cursor.execute("SELECT id FROM questions WHERE id NOT IN (SELECT question_id FROM user_history WHERE user_id = %s) ORDER BY RANDOM()", (user_id,))
            q_ids = [r[0] for r in cursor.fetchall()][:requested_count]

        elif mode == "mock":
            p_count = max(1, int(requested_count * 0.25))
            c_count = max(1, int(requested_count * 0.25))
            b_count = max(1, requested_count - p_count - c_count)

            def fetch_ids(subject, limit):
                cursor.execute("SELECT id FROM questions WHERE subject = %s AND id NOT IN (SELECT question_id FROM user_history WHERE user_id = %s) ORDER BY RANDOM() LIMIT %s", (subject, user_id, limit))
                return [r[0] for r in cursor.fetchall()]

            q_ids = fetch_ids("physics", p_count) + fetch_ids("chemistry", c_count) + fetch_ids("biology", b_count)
            random.shuffle(q_ids)
        else:
            q_ids = []

        if not q_ids:
            await update.message.reply_text("⚠️ No unseen questions available. Reset progress to recycle.", reply_markup=get_main_menu_keyboard())
            return

        ids_str = ",".join(map(str, q_ids))
        cursor.execute(
            """INSERT INTO user_sessions (user_id, mode, topic, current_index, score, question_ids, start_time)
               VALUES (%s, %s, %s, 0, 0, %s, %s)
               ON CONFLICT (user_id) DO UPDATE SET 
               mode=EXCLUDED.mode, topic=EXCLUDED.topic, current_index=0, score=0, question_ids=EXCLUDED.question_ids, start_time=EXCLUDED.start_time""",
            (user_id, mode, topic, ids_str, int(time.time()))
        )
        conn.commit()

        await update.message.reply_text(f"🚀 **Session started!** {len(q_ids)} questions loaded. Good luck!", parse_mode="Markdown")
        await send_next_session_question(context, user_id)
    finally:
        conn.close()


async def send_next_session_question(context, user_id):
    conn = get_conn()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT mode, topic, current_index, question_ids, score, start_time FROM user_sessions WHERE user_id = %s", (user_id,))
        row = cursor.fetchone()
        if not row:
            await context.bot.send_message(chat_id=user_id, text="Session not found.", reply_markup=get_main_menu_keyboard())
            return

        mode, topic, curr_idx, ids_str, score, start_time = row
        q_ids = list(map(int, ids_str.split(",")))

        if curr_idx >= len(q_ids):
            duration = int(time.time()) - start_time
            mins, secs = duration // 60, duration % 60
            max_score = len(q_ids) * 4
            pct = (score / max_score * 100) if max_score > 0 else 0.0

            cursor.execute("DELETE FROM user_sessions WHERE user_id = %s", (user_id,))
            conn.commit()

            mode_labels = {
                "physics":        "Physics",
                "chemistry":      "Chemistry",
                "biology":        "Biology",
                "mixed":          "Mixed Quiz",
                "mock":           "Custom Mock",
                "mistake_review": "Mistake Vault",
                "bookmark_review":"Bookmark Review",
            }
            mode_display = mode_labels.get(mode, mode.replace("_", " ").title())

            if pct >= 80:
                verdict = "🌟 Excellent! You're well prepared on this topic."
            elif pct >= 50:
                verdict = "👍 Good effort. A few more rounds will sharpen this."
            else:
                verdict = "📚 Needs more work — revisit this topic with the Weak Drill."

            vault_note = ""
            if mode == "mistake_review":
                vault_note = (
                    "\n\n🧹 Great — correctly answered mistakes are now cleared from your vault."
                    if pct >= 80 else
                    "\n\n🔁 Keep revisiting the vault. Repetition is how wrong answers become automatic."
                )

            summary = (
                f"🏁 **SESSION COMPLETE**\n\n"
                f"• Mode: `{mode_display}`\n"
                f"• Score: **{score} / {max_score}** ({pct:.0f}%)\n"
                f"• Time: **{mins}m {secs}s**\n\n"
                f"{verdict}{vault_note}"
            )
            await context.bot.send_message(
                chat_id=user_id, text=summary, parse_mode="Markdown",
                reply_markup=get_main_menu_keyboard()
            )
            return

        status, today_count, _, _, _ = get_user_status(user_id)
        if status == "free" and today_count >= FREE_DAILY_LIMIT:
            cursor.execute("DELETE FROM user_sessions WHERE user_id = %s", (user_id,))
            conn.commit()
            await context.bot.send_message(chat_id=user_id, text="⚠️ **Daily Limit Reached**", reply_markup=get_main_menu_keyboard())
            return

        target_q_id = q_ids[curr_idx]
        cursor.execute("SELECT question, opt1, opt2, opt3, opt4, correct, explanation FROM questions WHERE id = %s", (target_q_id,))
        q = cursor.fetchone()

        nav_kb = InlineKeyboardMarkup([[
            InlineKeyboardButton("Next ➡️", callback_data="next_question"),
            InlineKeyboardButton("⭐ Bookmark", callback_data=f"book_{target_q_id}"),
            InlineKeyboardButton("🚩 Report", callback_data=f"report_{target_q_id}")
        ], [InlineKeyboardButton("❌ Quit", callback_data="quit_test")]])

        cursor.execute("UPDATE users SET count_today = count_today + 1 WHERE user_id = %s", (user_id,))
        cursor.execute("UPDATE user_sessions SET current_index = current_index + 1 WHERE user_id = %s", (user_id,))
        cursor.execute("INSERT INTO user_history (user_id, question_id, is_correct, answered) VALUES (%s, %s, 0, 0) ON CONFLICT DO NOTHING", (user_id, target_q_id))
        conn.commit()

        状况_count = updated_count = cursor.execute("SELECT count_today FROM users WHERE user_id = %s", (user_id,))
        updated_count = cursor.fetchone()[0]
        if updated_count == DAILY_TARGET:
            await context.bot.send_message(
                chat_id=user_id,
                text=(
                    f"🎯 **Daily Goal Hit!**\n\n"
                    f"You've completed {DAILY_TARGET} questions today. "
                    f"That's exactly what separates consistent students from the rest.\n\n"
                    f"Keep going — every extra question is bonus territory. 🔥"
                ),
                parse_mode="Markdown"
            )

        msg = await context.bot.send_poll(
            chat_id=user_id, question=f"[{mode.replace('_', ' ').upper()} — Q{curr_idx + 1}/{len(q_ids)}] {q[0]}",
            options=[q[1], q[2], q[3], q[4]], type="quiz", correct_option_id=q[5], explanation=q[6],
            is_anonymous=False, open_period=72, reply_markup=nav_kb,
        )
        cursor.execute("INSERT INTO poll_tracker (poll_id, user_id, question_id, correct_option) VALUES (%s, %s, %s, %s) ON CONFLICT (poll_id) DO UPDATE SET user_id=EXCLUDED.user_id, question_id=EXCLUDED.question_id, correct_option=EXCLUDED.correct_option", (msg.poll.id, user_id, target_q_id, q[5]))
        conn.commit()
    finally:
        conn.close()


async def send_next_bookmark_question(context, user_id):
    conn = get_conn()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT current_index, question_ids FROM user_sessions WHERE user_id = %s", (user_id,))
        row = cursor.fetchone()
        if not row:
            await context.bot.send_message(chat_id=user_id, text="Bookmark session lost.", reply_markup=get_main_menu_keyboard())
            return

        curr_idx, ids_str = row
        q_ids = list(map(int, ids_str.split(",")))

        if curr_idx >= len(q_ids):
            cursor.execute("DELETE FROM user_sessions WHERE user_id = %s", (user_id,))
            conn.commit()
            await context.bot.send_message(chat_id=user_id, text="🏁 **Bookmark Review Done!**", reply_markup=get_main_menu_keyboard())
            return

        target_q_id = q_ids[curr_idx]
        cursor.execute("SELECT question, opt1, opt2, opt3, opt4, correct, explanation FROM questions WHERE id = %s", (target_q_id,))
        q = cursor.fetchone()

        nav_kb = InlineKeyboardMarkup([[
            InlineKeyboardButton("Next ➡️", callback_data="next_bookmark"),
            InlineKeyboardButton("🗑️ Remove", callback_data=f"delbk_{target_q_id}"),
        ], [InlineKeyboardButton("❌ Exit", callback_data="quit_test")]])

        cursor.execute("UPDATE user_sessions SET current_index = current_index + 1 WHERE user_id = %s", (user_id,))
        conn.commit()

        await context.bot.send_poll(
            chat_id=user_id, question=f"[BOOKMARKS — {curr_idx + 1}/{len(q_ids)}] {q[0]}",
            options=[q[1], q[2], q[3], q[4]], type="quiz", correct_option_id=q[5], explanation=q[6],
            is_anonymous=False, reply_markup=nav_kb,
        )
    finally:
        conn.close()


async def receive_poll_answer(update: Update, context: ContextTypes.DEFAULT_TYPE):
    answer = update.poll_answer
    poll_id = answer.poll_id
    user_id = answer.user.id
    selected = answer.option_ids[0] if answer.option_ids else None

    conn = get_conn()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT question_id, correct_option FROM poll_tracker WHERE poll_id = %s", (poll_id,))
        row = cursor.fetchone()
        if not row:
            return

        q_id, correct_option = row
        is_correct = 1 if selected == correct_option else 0
        neet_delta = 4 if is_correct else -1

        cursor.execute("UPDATE user_sessions SET score = score + %s WHERE user_id = %s", (neet_delta, user_id))
        cursor.execute("UPDATE user_history SET is_correct = %s, answered = 1 WHERE user_id = %s AND question_id = %s", (is_correct, user_id, q_id))
        cursor.execute("UPDATE stats SET total_score = total_score + %s, total_attempted = total_attempted + 1 WHERE user_id = %s", (neet_delta, user_id))
        conn.commit()
    finally:
        conn.close()


async def handle_payment_screenshot(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    photo_file = update.message.photo[-1].file_id
    await update.message.reply_text("📥 **Received!** Forwarded to admin.", parse_mode="Markdown")
    await context.bot.send_photo(
        chat_id=ADMIN_ID, photo=photo_file,
        caption=f"💳 **NEW PREMIUM REQUEST**\n\n• Name: {user.full_name}\n• ID: `{user.id}`\n\nTo approve: `/approve {user.id}`",
    )


# ─────────────────────────────────────────────
# ADMINISTRATOR EXCLUSIVE PRIVILEGES
# ─────────────────────────────────────────────

async def approve_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    if not context.args:
        await update.message.reply_text("Usage: `/approve <user_id>`")
        return

    target_id = int(context.args[0])
    expiry = (date.today() + timedelta(days=30)).strftime("%Y-%m-%d")

    conn = get_conn()
    cursor = conn.cursor()
    try:
        cursor.execute("UPDATE users SET status = 'premium', expiry_date = %s WHERE user_id = %s", (expiry, target_id))
        conn.commit()
    finally:
        conn.close()

    await update.message.reply_text(
        f"✅ User `{target_id}` activated until `{expiry}`", parse_mode="Markdown"
    )
    try:
        await context.bot.send_message(
            chat_id=target_id,
            text=(
                f"🎉 **Premium Activated!**\n\n"
                f"Your account is now upgraded. Valid for 30 days until `{expiry}`.\n"
                f"Enjoy unlimited practice — go crush NEET! 💪"
            ),
            parse_mode="Markdown"
        )
        if PREMIUM_GROUP_LINK:
            await context.bot.send_message(
                chat_id=target_id,
                text=(
                    f"👑 **Welcome to the VIP Rankers Circle!**\n\n"
                    f"As a Premium member you now have access to our exclusive discussion channel "
                    f"where top NEET aspirants share strategies, notes, and support each other.\n\n"
                    f"🔗 **Join here:** {PREMIUM_GROUP_LINK}"
                ),
                parse_mode="Markdown"
            )
    except Exception:
        pass


async def revoke_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    if not context.args:
        await update.message.reply_text("Usage: `/revoke <user_id>`")
        return

    target_id = int(context.args[0])
    conn = get_conn()
    cursor = conn.cursor()
    try:
        cursor.execute("UPDATE users SET status = 'free', expiry_date = NULL WHERE user_id = %s", (target_id,))
        conn.commit()
    finally:
        conn.close()

    await update.message.reply_text(f"✅ User `{target_id}` revoked.")


async def stats_all(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return

    conn = get_conn()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT COUNT(*) FROM users")
        total_users = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM users WHERE status = 'premium'")
        premium_users = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM users WHERE last_date = %s", (str(date.today()),))
        active_today = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM questions")
        total_q = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM user_history WHERE answered = 1")
        total_answers = cursor.fetchone()[0]
        cursor.execute("SELECT MAX(streak) FROM users")
        top_streak = cursor.fetchone()[0] or 0
    finally:
        conn.close()

    await update.message.reply_text(
        f"📊 **BOT STATS (SUPABASE)**\n\n• Total Users: `{total_users}`\n• Premium: `{premium_users}`\n• Active Today: `{active_today}`\n• DB Questions: `{total_q}`\n• Answers Logged: `{total_answers}`\n• Top Streak: `{top_streak} days`",
        parse_mode="Markdown"
    )


async def broadcast_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    if not context.args:
        await update.message.reply_text("Usage: `/broadcast <message>`")
        return

    message = " ".join(context.args)
    conn = get_conn()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT user_id FROM users")
        all_users = cursor.fetchall()
    finally:
        conn.close()

    sent, failed = 0, 0
    for (uid,) in all_users:
        try:
            await context.bot.send_message(chat_id=uid, text=f"📢 **Announcement**\n\n{message}", parse_mode="Markdown")
            sent += 1
        except Exception:
            failed += 1
    await update.message.reply_text(f"✅ Broadcast done. Sent: {sent}, Failed: {failed}")


async def check_expiry(context: ContextTypes.DEFAULT_TYPE):
    today_str = str(date.today())
    conn = get_conn()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT user_id FROM users WHERE status = 'premium' AND expiry_date < %s", (today_str,))
        expired = cursor.fetchall()
        for (uid,) in expired:
            cursor.execute("UPDATE users SET status = 'free', expiry_date = NULL WHERE user_id = %s", (uid,))
            conn.commit()
            try:
                await context.bot.send_message(
                    chat_id=uid,
                    text=(
                        "⚠️ **Premium Expired**\n\n"
                        "Your subscription has ended. Renew for ₹49/month to get back unlimited access.\n\n"
                        "Use /start → 💎 Unlock Premium to resubscribe."
                    ),
                    parse_mode="Markdown"
                )
            except Exception:
                pass
    finally:
        conn.close()


async def my_link(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    get_user_status(user.id, user.full_name)

    bot_info = await context.bot.get_me()
    ref_link = f"https://t.me/{bot_info.username}?start=ref_{user.id}"

    conn = get_conn()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT referral_count FROM users WHERE user_id = %s", (user.id,))
        row = cursor.fetchone()
        ref_count = row[0] if row else 0
    finally:
        conn.close()

    await update.message.reply_text(
        f"🔗 **Your Referral Link**\n\n"
        f"`{ref_link}`\n\n"
        f"Share this with friends. Every student who joins via your link gives you **+3 days Premium** automatically.\n\n"
        f"• Students referred so far: **{ref_count}**",
        parse_mode="Markdown"
    )


async def user_rank(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    get_user_status(user_id, update.effective_user.full_name)

    conn = get_conn()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT COUNT(*) FROM stats WHERE total_score > 0")
        total_ranked = cursor.fetchone()[0]

        cursor.execute("SELECT total_score FROM stats WHERE user_id = %s", (user_id,))
        row = cursor.fetchone()
        my_score = row[0] if row else 0

        cursor.execute("SELECT COUNT(*) FROM stats WHERE total_score > %s", (my_score,))
        ahead_of_me = cursor.fetchone()[0]
        my_rank = ahead_of_me + 1
    finally:
        conn.close()

    if total_ranked == 0 or my_score == 0:
        await update.message.reply_text(
            "📊 You haven't answered any questions yet. Complete a session to appear on the leaderboard.",
            reply_markup=get_main_menu_keyboard()
        )
        return

    if my_rank == 1:
        rank_label = "🥇 You are #1 — the top student on this platform!"
    elif my_rank == 2:
        rank_label = "🥈 You are #2 — one push away from the top."
    elif my_rank == 3:
        rank_label = "🥉 You are #3 — podium position."
    else:
        rank_label = f"📊 You are **#{my_rank}** out of **{total_ranked}** ranked students."

    top_pct = round((1 - (my_rank - 1) / total_ranked) * 100)
    await update.message.reply_text(
        f"🏆 **Your Global Rank**\n\n"
        f"{rank_label}\n\n"
        f"• Your score: `{my_score} pts`\n"
        f"• Top `{top_pct}%` of all ranked students",
        parse_mode="Markdown"
    )


async def extend_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    if len(context.args) < 2:
        await update.message.reply_text("Usage: `/extend <user_id> <days>`", parse_mode="Markdown")
        return

    try:
        target_id = int(context.args[0])
        extra_days = int(context.args[1])
        if extra_days <= 0:
            await update.message.reply_text("Days must be a positive number.")
            return
    except ValueError:
        await update.message.reply_text("⚠️ Invalid arguments. Both user_id and days must be integers.")
        return

    conn = get_conn()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT status, expiry_date FROM users WHERE user_id = %s", (target_id,))
        row = cursor.fetchone()
        if not row:
            await update.message.reply_text(f"⚠️ User `{target_id}` not found in database.", parse_mode="Markdown")
            return

        current_status, current_expiry = row
        today = date.today()

        if current_expiry:
            base_date = max(datetime.strptime(current_expiry, "%Y-%m-%d").date(), today)
        else:
            base_date = today

        new_expiry = (base_date + timedelta(days=extra_days)).strftime("%Y-%m-%d")
        cursor.execute(
            "UPDATE users SET status = 'premium', expiry_date = %s WHERE user_id = %s",
            (new_expiry, target_id)
        )
        conn.commit()
    finally:
        conn.close()

    await update.message.reply_text(
        f"✅ Extended `{target_id}` by **{extra_days} days**. New expiry: `{new_expiry}`",
        parse_mode="Markdown"
    )
    try:
        await context.bot.send_message(
            chat_id=target_id,
            text=(
                f"🎁 **Premium Extended!**\n\n"
                f"Your subscription has been extended by **{extra_days} days**.\n"
                f"New expiry: `{new_expiry}`\n\n"
                f"Keep up the great work! 💪"
            ),
            parse_mode="Markdown"
        )
    except Exception:
        pass


# ─────────────────────────────────────────────
# BACKGROUND FLASK WEB SERVER
# ─────────────────────────────────────────────
flask_app = Flask('')

@flask_app.route('/')
def home():
    return "NEET Bot (Supabase Engine) is running 24/7!"

def run_flask():
    port = int(os.getenv("PORT", 8080))
    flask_app.run(host='0.0.0.0', port=port)


if __name__ == "__main__":
    init_db()
    Thread(target=run_flask).start()
    
    app = ApplicationBuilder().token(TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("approve", approve_user))
    app.add_handler(CommandHandler("revoke", revoke_user))
    app.add_handler(CommandHandler("extend", extend_user))
    app.add_handler(CommandHandler("stats", stats_all))
    app.add_handler(CommandHandler("broadcast", broadcast_message))
    app.add_handler(CommandHandler("get_users", export_users))
    app.add_handler(CommandHandler("mylink", my_link))
    app.add_handler(CommandHandler("rank", user_rank))
    app.add_handler(CallbackQueryHandler(handle_menu_clicks))
    app.add_handler(PollAnswerHandler(receive_poll_answer))
    app.add_handler(MessageHandler(filters.PHOTO, handle_payment_screenshot))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_input))

    app.job_queue.run_daily(check_expiry, time=dt_time(0, 0))

    print("NEET Bot v5.0 (Supabase Backend with Active Pooling) running...")
    app.run_polling()
