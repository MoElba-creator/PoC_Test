import streamlit as st
import bcrypt
import os
import time
import hashlib
import base64
from dotenv import load_dotenv

load_dotenv()

def get_secret(key):
    return os.getenv(key) or st.secrets.get(key)

COOKIE_NAME = "auth_token"
COOKIE_SECRET = get_secret("COOKIE_SECRET") or "fallback_key_change_me"

def _generate_token(username):
    timestamp = str(int(time.time()))
    raw = f"{username}|{timestamp}|{COOKIE_SECRET}"
    token = f"{username}|{timestamp}|{hashlib.sha256(raw.encode()).hexdigest()}"
    return base64.b64encode(token.encode()).decode()

def _validate_token(token):
    try:
        decoded = base64.b64decode(token).decode()
        username, timestamp, signature = decoded.split("|")
        raw = f"{username}|{timestamp}|{COOKIE_SECRET}"
        valid_signature = hashlib.sha256(raw.encode()).hexdigest()
        if valid_signature == signature:
            return username
    except:
        pass
    return None

def check_login():
    correct_username = get_secret("LOGIN_USER")
    correct_password_hash = get_secret("LOGIN_PASS_HASH").encode("utf-8")

    token = st.experimental_get_cookie(COOKIE_NAME)
    user_from_token = _validate_token(token) if token else None
    if user_from_token == correct_username:
        st.session_state.authenticated = True
        st.sidebar.success(f"🔓 Logged in as {user_from_token}")
        if st.sidebar.button("🔒 Logout"):
            st.session_state.authenticated = False
            st.experimental_set_cookie(COOKIE_NAME, "", expires=0)
            st.rerun()
        return

    st.title("🔐 Login required")
    with st.form("login_form"):
        username = st.text_input("Username")
        password = st.text_input("Password", type="password")
        submit = st.form_submit_button("Login")

        if submit:
            if username == correct_username and bcrypt.checkpw(password.encode("utf-8"), correct_password_hash):
                token = _generate_token(username)
                st.experimental_set_cookie(COOKIE_NAME, token, max_age=86400)
                st.success("✔️ Logged in successfully")
                st.rerun()
            else:
                st.error("❌ Incorrect username or password")
    st.stop()
