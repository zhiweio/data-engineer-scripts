import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import javax.crypto.spec.IvParameterSpec;
import java.util.Base64;

public class AESEncryptionExample {

    private static final String ALGORITHM = "AES";
    private static final String TRANSFORMATION = "AES/CBC/PKCS5Padding"; // Use CBC mode with PKCS5Padding
    private static final byte[] IV = new byte[16]; // Initialization vector (16 bytes for AES)

    public static void main(String[] args) {
        try {
            // Example plain text to encrypt
            String plainText = "Hello, World!";

            // Your Base64 encoded AES key
            String base64EncodedKey = "k6P9sc18inSF//A4Gwsruw=="; // Replace with your actual Base64 key
            byte[] decodedKey = Base64.getDecoder().decode(base64EncodedKey); // Decode Base64 key
            SecretKey secretKey = new SecretKeySpec(decodedKey, ALGORITHM);

            // Encrypt the data
            String encryptedData = encrypt(plainText, secretKey);
            System.out.println("Encrypted Data: " + encryptedData);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static String encrypt(String plainText, SecretKey secretKey) throws Exception {
        Cipher cipher = Cipher.getInstance(TRANSFORMATION);
        cipher.init(Cipher.ENCRYPT_MODE, secretKey, new IvParameterSpec(IV)); // Initialize with the key and IV

        byte[] encryptedBytes = cipher.doFinal(plainText.getBytes()); // Encrypt the data
        return Base64.getEncoder().encodeToString(encryptedBytes); // Encode to Base64 and return
    }
}