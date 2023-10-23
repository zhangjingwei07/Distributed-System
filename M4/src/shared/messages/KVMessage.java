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
		DELETE_ERROR, 	/* Delete - request not successful */
		UNKNOWN_STATUS_ERROR, /* The status code is unrecognizable */
		INPUT_ERROR, /* Error related input parameters */
		MERGE, /* Merge data - request*/
		MERGE_SUCCESS, /* Merge data - file merging successful */
		MERGE_ERROR, /* Merge data - Error related to file merging */
		DIDSUB, /* Check whether the client has subscribbed a given key */
		DIDSUB_SUCCESS, /* Check whether the client has subscribbed a given key - request successful */
		DIDSUB_ERROR, /* Check whether the client has subscribbed a given key - request not successful */
		SUB, /* Subscribe - request */
		SUB_SUCCESS, /* Subscribe - request successful*/
		SUB_ERROR, /* Subscribe - request not successful */
		UNSUB, /* Unscribe - request */
		UNSUB_SUCCESS, /* Unsubscribe - request successful */
		UNSUB_ERROR, /* Unsubscribe - request not successful */
		SUBUPDATETOSERVER, /* Update PUT value change to the responsible server - request */
		SUBUPDATETOSERVER_SUCCESS, /* Update PUT value change to the responsible server - request sent successfully */
		SUBUPDATETOSERVER_ERROR, /* Update PUT value change to the responsible server - request sent unsuccessfully */
		SUBUPDATETOCLIENT, /* Update PUT value change to the client (subscriber) - request */	
		
		// Status related to first autentication setup
		AUTHENTICATION_SETUP, /* Request secretkey for authentication setup */

		// Status related to metadata
		METADATA_REQUEST, /* This status means the client is requesting new METADATA */
		METADATA_UPDATE, /* This status is sent along with the latest metadata from server to client*/
		METADATA_ERROR, /* This status is sent when the server failed to send metadata() */

		// Status related to subConnectionTable
		SUBCONTABLEINSERTTOSERVER_SUCCESS, /* This status is sent from server back to client, meaning that server successfully receives the new client connection
									and will insert client-server pair into subConnectionTable as part of metadata. */
		BROADCASTSUBCONTABLETOSERVER, /* Broadcast the up-to-date subConnectionTable - Request */
		BROADCASTSUBCONTABLETOSERVER_FINISH, /*  Broadcast the up-to-date subConnectionTable - Request finished */
		UNICASTSUBCONTABLETOSERVER, /* Unicast the up-to-date subConnectionTable to targeted server - Request */
		UNICASTSUBCONTABLETOSERVER_FINISH, /* Unicast the up-to-date subConnectionTable to targeted server - Request finished */
		GETSUBCONNECTIONTABLE, /* Helper request: To get the current subConnectionTable */
		GETSUBCONNECTIONTABLE_SUCCESS, /* Successful GET of subconnectionTable */
		GETSUBCONNECTIONTABLE_ERROR, /* Failed GET of subconnectionTable */

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
	 * @return whether the KVMessage is for replication or not.
	 */
	public boolean getIsReplicated();

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
	
	/**
	 * Setter of isReplicated
	 */
	public void setIsReplicated(boolean isReplicated);
}


