package shared.messages;

import com.google.gson.Gson;
import org.apache.log4j.Logger;




public class KVMessageImple implements KVMessage, EncodingDecoding {
    public static final String DEFAULT_SECRET ="MY_DEFAULT_SECRET";
    private static Logger logger = Logger.getLogger("KVMessage");

    private String key;
    private String value;
    private StatusType status;
    private boolean isReplicated = false;
    private String secret = DEFAULT_SECRET;

    public KVMessageImple(String secret) {
        this.secret = secret;
    }

    public KVMessageImple(String key, String value, StatusType status, String secret) {
        this.key = key;
        this.value = value;
        this.status = status;
        this.secret = secret;
    }

    public KVMessageImple(String key, String value, StatusType status, boolean isReplicated, String secret) {
        this.key = key;
        this.value = value;
        this.status = status;
        this.isReplicated = isReplicated;
        this.secret = secret;
    }

    @Override
    public String getKey(){
        return key;
    }

    @Override
    public String getValue(){
        return value;
    }

    @Override
    public StatusType getStatus() {
        return status;
    }
    
    @Override
    public boolean getIsReplicated() {
        return isReplicated;
    }

    @Override
    public void setKey(String key){
        this.key = key;
    }

    @Override
    public void setValue(String value){
        this.value = value;
    }

    @Override
    public void setStatus(StatusType status) {
        this.status = status;
    }
    
    @Override
    public void setIsReplicated(boolean isReplicated) {
        this.isReplicated = isReplicated;
    }

    public void setSecretKey(String secretKey){
        secret = secretKey;
    }

    public String encryptString(String text){
        // logger.debug("Before encrption = {" + text + "} with key = {" + secret + "}" );
        return AESEncryption.encrypt(text, secret) ;
        // return text;
    }

    public String decryptString(String text){
        // logger.debug("Before decrption = {" + text + "} with key = {" + secret + "}" );
        return AESEncryption.decrypt(text, secret);
        // return text;
    }
    
    @Override
    public String encode(){
        String text = new Gson().toJson(this);
        logger.debug("Encrpting: {" + text + "} with key = {" + secret + "}..." );
        String encryptedText = encryptString(text);
        logger.debug("Encrypted: {" + encryptedText + "}");
        return encryptedText;
    }

    @Override
    public void decode(String data){
        logger.debug("Decrpting: {" + data + "} with key = {" + secret + "}..." );
        String decryptedData = decryptString(data);
        logger.debug("Decrypted: {" + decryptedData + "}");
        if (decryptedData == null){
            return;
        }
        KVMessageImple obj_mess = null;
        try {
            obj_mess = new Gson().fromJson(decryptedData, this.getClass());
            this.setKey(obj_mess.key);
            this.setValue(obj_mess.value);
            this.setStatus(obj_mess.status);
            this.setIsReplicated(obj_mess.isReplicated);
        } catch(com.google.gson.JsonSyntaxException ex) { 
            logger.error("Received invalid message format. Ignoring message");
        } catch(Exception e) {
            // logger.error("Cannot decode msg: {" + decryptedData + "}");
        }
    }
    
}
