package shared.messages;

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Base64;
import org.apache.log4j.Logger;
 
import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;
 
public class AESEncryption {
    private static Logger logger = Logger.getLogger("AESEncryp");
 
 
    public static String encrypt(String strToEncrypt, String secret) 
    {
        SecretKeySpec secretKey;
        byte[] key;
        MessageDigest sha = null;
        try {
            key = secret.getBytes("UTF-8");
            sha = MessageDigest.getInstance("SHA-1");
            key = sha.digest(key);
            key = Arrays.copyOf(key, 16); 
            secretKey = new SecretKeySpec(key, "AES");
        } 
        catch (NoSuchAlgorithmException e) {
            logger.error("NoSuchAlgorithmException");
            return null;
        } 
        catch (UnsupportedEncodingException e) {
            logger.error("UnsupportedEncodingException");
            return null;
        }
        
        try
        {   
            // logger.debug("String to encrypt ={" + strToEncrypt + "}, secretKey = {" + secret + "}");
            Cipher cipher = Cipher.getInstance("AES/ECB/PKCS5Padding");
            cipher.init(Cipher.ENCRYPT_MODE, secretKey);
            return Base64.getEncoder().encodeToString(cipher.doFinal(strToEncrypt.getBytes("UTF-8")));
        } 
        catch (Exception e) 
        {
            ;
            // logger.error("Error while encrypting: " + e.toString());
            // logger.error("msg causing error ={" + strToEncrypt + "}, secretKey = {" + secret + "}");
        }
        return null;
    }
 
    public static String decrypt(String strToDecrypt, String secret) 
    {
        SecretKeySpec secretKey;
        byte[] key;
        MessageDigest sha = null;
        try {
            key = secret.getBytes("UTF-8");
            sha = MessageDigest.getInstance("SHA-1");
            key = sha.digest(key);
            key = Arrays.copyOf(key, 16); 
            secretKey = new SecretKeySpec(key, "AES");
        } 
        catch (NoSuchAlgorithmException e) {
            logger.error("NoSuchAlgorithmException");
            return null;
        } 
        catch (UnsupportedEncodingException e) {
            logger.error("UnsupportedEncodingException");
            return null;
        }

        try
        {
            // logger.debug("String to decrypt ={" + strToDecrypt + "}, secretKey = {" + secret + "}");
            Cipher cipher = Cipher.getInstance("AES/ECB/PKCS5PADDING");
            cipher.init(Cipher.DECRYPT_MODE, secretKey);
            return new String(cipher.doFinal(Base64.getDecoder().decode(strToDecrypt)));
        } 
        catch (Exception e) 
        {
            ;
            // logger.error("Error while decrypting: " + e.toString());
            // logger.error("Encrypted msg causing error ={" + strToDecrypt + "}, secretKey = {" + secret + "}");
        }
        return null;
    }
}