import base64
import os

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import padding
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes


class AESCryptor:
    @staticmethod
    def keygen():
        seed = os.urandom(16)  # 128 bits = 16 bytes
        key = base64.b64encode(seed).decode("utf-8")
        return key

    @staticmethod
    def pad(data):
        padder = padding.PKCS7(algorithms.AES.block_size).padder()
        padded_data = padder.update(data.encode("utf-8")) + padder.finalize()
        return padded_data

    @staticmethod
    def unpad(data):
        unpadder = padding.PKCS7(algorithms.AES.block_size).unpadder()
        unpadded_data = unpadder.update(data) + unpadder.finalize()
        return unpadded_data

    @staticmethod
    def decrypt(encrypted_text, aes_key):
        key = base64.b64decode(aes_key)
        iv = bytes(
            16
        )  # Using a zeroed IV for demonstration; replace with your IV if needed
        encrypted_data = base64.b64decode(encrypted_text)
        cipher = Cipher(algorithms.AES(key), modes.CBC(iv), backend=default_backend())
        decrypter = cipher.decryptor()
        decrypted_data = decrypter.update(encrypted_data) + decrypter.finalize()
        return AESCryptor.unpad(decrypted_data).decode("utf-8")

    @staticmethod
    def encrypt(plain_text, aes_key):
        key = base64.b64decode(aes_key)
        iv = bytes(
            16
        )  # Using a zeroed IV for demonstration; replace with your IV if needed
        padded_data = AESCryptor.pad(plain_text)
        cipher = Cipher(algorithms.AES(key), modes.CBC(iv), backend=default_backend())
        encrypter = cipher.encryptor()
        encrypted_data = encrypter.update(padded_data) + encrypter.finalize()
        return base64.b64encode(encrypted_data).decode("utf-8")
