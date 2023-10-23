package shared.messages;

public interface EncodingDecoding {
    /**
     * Encode a specific message object into a single string
     * 
     * @return encoded string
     */
    public String encode();

    /**
     * Decode an input string into message object and save 
     * the info into the message object calling this method.
     */
    public void decode(String data);
}
