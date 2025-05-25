"""
Script: auth.py
Authors: Moussa El Bazioui and Laurens Rasschaert
Project: Bachelor thesis — data-driven anomaly detection

Purpose:
This script provides Streamlit-based login functionality using credentials stored in a .env file.
The user's session state is used to persist authentication status.
Only after successful login is the main interface accessible.
"""

import streamlit as st
import bcrypt
import os
from dotenv import load_dotenv
from pathlib import Path
from PIL import Image
import base64

# Load environment variables from .env file
load_dotenv()

# Login guard used by all Streamlit pages
def check_login():
    if "authenticated" not in st.session_state:
        st.session_state["authenticated"] = False

    if st.session_state["authenticated"]:
        return

    # Read login credentials from env
    correct_username = os.getenv("LOGIN_USER")
    correct_password_hash_raw = os.getenv("LOGIN_PASS_HASH")

    if not correct_username or not correct_password_hash_raw:
        st.error("Login is misconfigured.")
        st.stop()

    # Prepare hashed password
    correct_password_hash = correct_password_hash_raw.encode("utf-8")

    # Configure login page
    st.set_page_config(page_title="Login | Anomaly Detection", layout="centered")

    # Load and encode logo for inline display
    logo_path = Path(__file__).resolve().parent.parent.parent / "images" / "logo_vives.png"
    logo_base64 = ""
    if logo_path.exists():
        logo = Image.open(logo_path).convert("RGBA")
        white_bg = Image.new("RGBA", logo.size, (255, 255, 255, 255))
        combined = Image.alpha_composite(white_bg, logo)
        tmp_path = Path(__file__).parent / "temp_logo.png"
        combined.save(tmp_path)
        with open(tmp_path, "rb") as f:
            logo_base64 = base64.b64encode(f.read()).decode()
        tmp_path.unlink(missing_ok=True)

    # Custom CSS and login form styling
    st.markdown(f"""
        <style>
        body {{
            background-color: #111827;
        }}
        @keyframes fadeInUp {{
            0% {{
                opacity: 0;
                transform: translateY(30px);
            }}
            100% {{
                opacity: 1;
                transform: translateY(0);
            }}
        }}
        .login-header {{
            text-align: center;
            margin-bottom: 2rem;
            animation: fadeInUp 0.8s ease-out;
        }}
        .login-header img {{
            width: 110px;
            margin-bottom: 0.7rem;
            border-radius: 8px;
        }}
        .login-header h1 {{
            font-size: 1.7rem;
            margin: 0;
            color: #f9fafb;
        }}
        .login-header p {{
            font-size: 0.9rem;
            color: #9ca3af;
            margin: 0;
        }}
        .login-container {{
            max-width: 400px;
            margin: 3rem auto;
            padding: 2.5rem;
            background: rgba(255, 255, 255, 0.05);
            backdrop-filter: blur(10px);
            border-radius: 12px;
            box-shadow: 0 0 20px rgba(0,0,0,0.25);
            color: #f9fafb;
            animation: fadeInUp 1s ease-out;
        }}
        .stTextInput input {{
            background-color: #1f2937;
            color: white;
        }}
        .stPasswordInput input {{
            background-color: #1f2937;
            color: white;
        }}
        .stButton>button {{
            background-color: #f97316;
            color: white;
            font-weight: bold;
            border: none;
            padding: 0.5rem 1.2rem;
            border-radius: 8px;
        }}
        .stButton>button:hover {{
            background-color: #fb923c;
        }}
        </style>
    """, unsafe_allow_html=True)

    st.markdown(f"""
        <div class="login-header">
            <div style="display: flex; align-items: center; justify-content: center; gap: 1.2rem; flex-wrap: wrap;">
                <img src="data:image/png;base64,{logo_base64}" alt="VIVES logo" style="height: 48px; border-radius: 6px;">
                <h1 style="font-size: 2.1rem; color: #f9fafb; margin: 0;">Anomaly Detection</h1>
            </div>
            <p style="font-size: 1.1rem; font-weight: 600; color: #d1d5db; margin-top: 0.8rem;">
                Review and label suspicious network logs
            </p>
        </div>
    """, unsafe_allow_html=True)

    # Streamlit login form
    with st.form("login_form"):
        username = st.text_input("Username", key="username")
        password = st.text_input("Password", type="password", key="password")
        submitted = st.form_submit_button("Login")

    st.markdown('</div>', unsafe_allow_html=True)

    # When submitted validate credentials
    if submitted:
        if username == correct_username and bcrypt.checkpw(password.encode("utf-8"), correct_password_hash):
            st.session_state["authenticated"] = True
            with st.spinner("Initializing interface..."):
                import time
                time.sleep(1.5)
            st.rerun()
        else:
            st.error("Incorrect username or password.")

    st.stop()

def logout():
    st.session_state["authenticated"] = False
    st.rerun()