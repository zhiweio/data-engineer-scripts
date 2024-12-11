import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import java.util.Base64;

public class AESKeyGeneratorExample {

    private static final String ALGORITHM = "AES";

    public static void main(String[] args) {
        try {
            SecretKey secretKey = generateKey();
            String encodedKey = Base64.getEncoder().encodeToString(secretKey.getEncoded());
            System.out.println("AES Secret Key (Base64): " + encodedKey);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static SecretKey generateKey() throws Exception {
        KeyGenerator keyGenerator = KeyGenerator.getInstance(ALGORITHM);
        keyGenerator.init(128); // 128位密钥
        return keyGenerator.generateKey();
    }
}
