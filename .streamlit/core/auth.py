import streamlit as st
import bcrypt
import os
from dotenv import load_dotenv

load_dotenv()

def check_login():
    if "authenticated" not in st.session_state:
        st.session_state["authenticated"] = False

    if st.session_state["authenticated"]:
        return

    correct_username = os.getenv("LOGIN_USER")
    correct_password_hash_raw = os.getenv("LOGIN_PASS_HASH")

    if not correct_username or not correct_password_hash_raw:
        st.error("Login is misconfigured.")
        st.stop()

    correct_password_hash = correct_password_hash_raw.encode("utf-8")

    # Login form in main screen
    st.title("Welcome! Please log in:")
    with st.form("login_form"):
        username = st.text_input("Username", key="login_username")
        password = st.text_input("Password", type="password", key="login_password")
        submitted = st.form_submit_button("Login")

    if submitted:
        if username == correct_username and bcrypt.checkpw(password.encode("utf-8"), correct_password_hash):
            st.session_state["authenticated"] = True
            st.rerun()
        else:
            st.error("Incorrect username or password. Please try again.")

    st.stop()
