package ding;

import MessageForMap.Message;

public class DataTransferMessage extends Message {
	private String filename;
	private Object payload;
	public DataTransferMessage(String filename, Object payload) {
		this.filename = filename;
		this.payload = payload;
	}
	
	public String getFilename() {
		return this.filename;
	}
	
	public Object getPayload() {
		return this.payload;
	}
	
	public String getMessageType() {
		return "DataTransferMessage";
	}
}
