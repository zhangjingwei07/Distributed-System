package shared.messages;

public interface KVMessage {
	
	public enum StatusType {
		// Status related to true data
		GET, 			/* Get - request */
		GET_ERROR, 		/* requested tuple (i.e. value) not found */
		GET_SUCCESS, 	/* requested tuple (i.e. value) found */
		PUT, 			/* Put - request */
		PUT_SUCCESS, 	/* Put - request successful, tuple inserted */
		PUT_UPDATE, 	/* Put - request successful, i.e. value updated */
		PUT_ERROR, 		/* Put - request not successful */
		DELETE_SUCCESS, /* Delete - request successful */
		DELETE_ERROR, 	/* Delete - request successful */
		UNKNOWN_STATUS_ERROR, /* The status code is unrecognizable * */
		INPUT_ERROR, /* Error related input parameters */
		MERGE, /* Merge data - request*/
		MERGE_SUCCESS, /* Merge data - file merging successful */
		MERGE_ERROR, /* Merge data - Error related to file merging */
		
		// Status related to metadata
		METADATA_REQUEST, /* This status means the client is requesting new METADATA */
		METADATA_UPDATE, /* This status is sent along with the latest metadata from server to client*/
		METADATA_ERROR, /* This status is sent when the server failed to send metadata() */

		// Status related to server
		SERVER_STOPPED,         /* Server is stopped, no requests are processed */
        SERVER_WRITE_LOCK,      /* Server locked for write, only get possible */
        SERVER_NOT_RESPONSIBLE  /* Request not successful, server not responsible for key */
	}

	/**
	 * @return the key that is associated with this message, 
	 * 		null if not key is associated.
	 */
	public String getKey();
	
	/**
	 * @return the value that is associated with this message, 
	 * 		null if not value is associated.
	 */
	public String getValue();
	
	/**
	 * @return a status string that is used to identify request types, 
	 * response types and error types associated to the message.
	 */
	public StatusType getStatus();

	/**
	 * Setter of key
	 */
	public void setKey(String key);

	/**
	 * Setter of value
	 */
	public void setValue(String value);

	/**
	 * Setter of status
	 */
	public void setStatus(StatusType status);
	
}


