from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives import serialization

# Tạo khóa riêng RSA với kích thước 2048 bit
private_key = rsa.generate_private_key(
    public_exponent=65537,
    key_size=2048,
    backend=default_backend()
)

# Chuyển khóa riêng sang định dạng PEM
pem_private = private_key.private_bytes(
    encoding=serialization.Encoding.PEM,
    format=serialization.PrivateFormat.PKCS8,
    encryption_algorithm=serialization.NoEncryption()  # Không mã hóa khóa riêng
)

# Lưu khóa riêng vào file "private_key.pem"
with open("private_key.pem", "wb") as f:
    f.write(pem_private)

# Sinh khóa công khai từ khóa riêng
public_key = private_key.public_key()

# Chuyển khóa công khai sang định dạng PEM
pem_public = public_key.public_bytes(
    encoding=serialization.Encoding.PEM,
    format=serialization.PublicFormat.SubjectPublicKeyInfo
)

# Lưu khóa công khai vào file "public_key.pem"
with open("public_key.pem", "wb") as f:
    f.write(pem_public)

print("Đã tạo cặp khóa thành công. File private_key.pem và public_key.pem được lưu trong thư mục hiện hành.")
