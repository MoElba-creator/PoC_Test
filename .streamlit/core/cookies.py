import streamlit as st
from streamlit_javascript import st_javascript

class CookieManager:
    def get(self, key):
        return st_javascript(f"document.cookie.split('; ').find(row => row.startsWith('{key}='))?.split('=')[1];")

    def set(self, key, value, max_age=86400):
        st_javascript(f"document.cookie = '{key}={value}; max-age={max_age}; path=/'")

    def delete(self, key):
        st_javascript(f"document.cookie = '{key}=; max-age=0; path=/';")
