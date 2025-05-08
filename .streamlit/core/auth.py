import streamlit as st
import os
import bcrypt
import time
from dotenv import load_dotenv

load_dotenv()

def check_login():
    correct_username = os.getenv("LOGIN_USER")
    raw_hash = os.getenv("LOGIN_PASS_HASH")
    if not raw_hash:
        st.error("LOGIN_PASS_HASH environment variable is not set.")
        st.stop()
    correct_password_hash = raw_hash.encode("utf-8")

    if "authenticated" not in st.session_state:
        st.session_state.authenticated = False
    if "login_attempts" not in st.session_state:
        st.session_state.login_attempts = 0
    if "last_attempt_time" not in st.session_state:
        st.session_state.last_attempt_time = 0

    if st.session_state.authenticated:
        st.sidebar.success(f"Logged in as {correct_username}")
        if st.sidebar.button("Logout"):
            st.session_state.authenticated = False
            st.rerun()
        return

    with st.form("Login"):
        username = st.text_input("Username")
        password = st.text_input("Password", type="password")
        submitted = st.form_submit_button("Login")

        if submitted:
            now = time.time()
            if now - st.session_state.last_attempt_time < 5:
                st.error("Please wait before trying again.")
                st.stop()

            if username == correct_username and bcrypt.checkpw(password.encode("utf-8"), correct_password_hash):
                st.session_state.authenticated = True
                st.session_state.login_attempts = 0
                st.rerun()
            else:
                st.session_state.last_attempt_time = now
                st.session_state.login_attempts += 1
                st.error("Invalid credentials")
                st.stop()

    st.stop()
