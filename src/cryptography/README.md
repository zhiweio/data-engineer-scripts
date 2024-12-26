Python code snippets for encryption and decryption.

## Usage Example

```python
from .cryptor import AESCryptor

# Generate AES key
aes_key = AESCryptor.keygen()
print(f"Generated Key: {aes_key}")

# Plain text
plain_text = "Hello, World!"

# Encrypt
encrypted_text = AESCryptor.encrypt(plain_text, aes_key)
print(f"Encrypted Text: {encrypted_text}")

# Decrypt
decrypted_text = AESCryptor.decrypt(encrypted_text, aes_key)
print(f"Decrypted Text: {decrypted_text}")
```
