import bcrypt

password = b"<Le1Z%-J8|~j"  # <- replace this with your actual password
hashed = bcrypt.hashpw(password, bcrypt.gensalt())
print("Your bcrypt hash:", hashed.decode())
