import unittest
from cryptor import AESCryptor


class TestAESCryptor(unittest.TestCase):
    def setUp(self):
        """Initialize the AESCryptor and generate a key for testing."""
        self.cryptor = AESCryptor()
        self.key = self.cryptor.keygen()
        self.plain_text = "Hello, World!"

    def test_encrypt_decrypt(self):
        """Test encryption decryption."""
        # Encrypt the plain text
        encrypted_text = self.cryptor.encrypt(self.plain_text, self.key)

        # Decrypt the encrypted text
        decrypted_text = self.cryptor.decrypt(encrypted_text, self.key)

        # Assert that the decrypted text is the same as the original plain text
        self.assertEqual(self.plain_text, decrypted_text)

    def test_keygen(self):
        """Test key generation."""
        key1 = self.cryptor.keygen()
        key2 = self.cryptor.keygen()

        # Assert that two generated keys are not the same
        self.assertNotEqual(key1, key2)


if __name__ == "__main__":
    unittest.main()
