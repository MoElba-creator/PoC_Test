import streamlit as st
import bcrypt
import os
from dotenv import load_dotenv

load_dotenv()

def check_login():
    correct_username = os.getenv("LOGIN_USER")
    correct_password_hash = os.getenv("LOGIN_PASS_HASH").encode("utf-8")

    # ✅ Already authenticated? Continue with app
    if st.session_state.get("authenticated"):
        return

    # 🧱 Login form
    st.sidebar.subheader("🔐 Login")
    username = st.sidebar.text_input("Username")
    password = st.sidebar.text_input("Password", type="password")
    submit = st.sidebar.button("Login")

    if submit:
        if username == correct_username and bcrypt.checkpw(password.encode("utf-8"), correct_password_hash):
            st.session_state.authenticated = True
            st.success("✔️ Logged in successfully")
            st.rerun()  # 🔁 Force page refresh to show app content
        else:
            st.error("❌ Incorrect username or password")

    # 🚫 Stop the app if not logged in
    st.stop()
