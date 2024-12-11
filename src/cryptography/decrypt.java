import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import javax.crypto.spec.IvParameterSpec;
import java.util.Base64;

public class AESDecryptionExample {

    private static final String ALGORITHM = "AES";
    private static final String TRANSFORMATION = "AES/CBC/PKCS5Padding"; // Use CBC mode with PKCS5Padding
    private static final byte[] IV = new byte[16]; // Initialization vector (16 bytes for AES)

    public static void main(String[] args) {
        try {
            // Example encrypted data (Base64 encoded)
            String encryptedData = "8gZ9PLu4q1sw5BeB4ueQ6g==";

            // Your Base64 encoded AES key
            String base64EncodedKey = "k6P9sc18inSF//A4Gwsruw=="; // Replace with your actual Base64 key
            byte[] decodedKey = Base64.getDecoder().decode(base64EncodedKey); // Decode Base64 key
            SecretKey secretKey = new SecretKeySpec(decodedKey, ALGORITHM);

            // Decrypt the data
            String decryptedData = decrypt(encryptedData, secretKey);
            System.out.println("Decrypted Data: " + decryptedData);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static String decrypt(String encryptedData, SecretKey secretKey) throws Exception {
        Cipher cipher = Cipher.getInstance(TRANSFORMATION);
        cipher.init(Cipher.DECRYPT_MODE, secretKey, new IvParameterSpec(IV)); // Initialize with the key and IV

        byte[] decodedData = Base64.getDecoder().decode(encryptedData); // Decode Base64
        byte[] decryptedBytes = cipher.doFinal(decodedData); // Decrypt the data

        return new String(decryptedBytes); // Convert bytes to string
    }
}