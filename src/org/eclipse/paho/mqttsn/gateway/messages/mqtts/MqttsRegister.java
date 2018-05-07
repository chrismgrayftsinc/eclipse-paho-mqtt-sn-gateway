/*******************************************************************************
 * Copyright (c) 2008, 2014 IBM Corp.
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * and Eclipse Distribution License v1.0 which accompany this distribution. 
 *
 * The Eclipse Public License is available at 
 *    http://www.eclipse.org/legal/epl-v10.html
 * and the Eclipse Distribution License is available at 
 *   http://www.eclipse.org/org/documents/edl-v10.php.
 *
 * Contributors:
 *    Ian Craggs - initial API and implementation and/or initial documentation
 *******************************************************************************/

package org.eclipse.paho.mqttsn.gateway.messages.mqtts;

import java.io.UnsupportedEncodingException;
import org.eclipse.paho.mqttsn.gateway.utils.Utils;

/**
 * This object represents a Mqtts REGISTER message.
 * 
 *
 */
public class MqttsRegister extends MqttsMessage {
	
	//Mqtts REGISTER fields
	private int topicId;
	private int msgId;
	private String topicName;
		
	/**
	 * MqttsRegister constructor.Sets the appropriate message type. 
	 */
	public MqttsRegister() {
		msgType = MqttsMessage.REGISTER;
	}
	
	/**
	 * MqttsRegister constructor.Sets the appropriate message type and constructs 
	 * a Mqtts REGISTER message from a received byte array.
	 * @param data: The buffer that contains the REGISTER message.
	 */	
	public MqttsRegister(byte[] data) {
		int length = getLength(data);
		int headerLength = data[0] == 0x01 ? 8 : 6;
		msgType = MqttsMessage.REGISTER;
		topicId = 0;//send by the client  hlt: ???
		//hlt 6.3.08
		topicId= ((data[headerLength - 4] & 0xFF) << 8) + (data[headerLength - 3] & 0xFF);
		msgId = ((data[headerLength - 2] & 0xFF) << 8) + (data[headerLength - 1] & 0xFF);
		byte[] byteTopicName = new byte[length - headerLength];
		System.arraycopy(data, headerLength, byteTopicName, 0, length - headerLength);
		try {
			topicName = new String(byteTopicName, Utils.STRING_ENCODING);
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Method to convert this message to a byte array for transmission.
	 * @return A byte array containing the REGISTER message as it should appear on the wire.
	 */
	public byte[] toBytes() {
		int headerLength = 6;
		if (headerLength + topicName.length() > 255) {
			headerLength += 2;
		}
		int length = headerLength + topicName.length();
		byte[] data = setLength(new byte[length], length);
		data[headerLength - 5] = (byte)msgType;
		data[headerLength - 4] = (byte)((topicId >> 8) & 0xFF);
		data[headerLength - 3] = (byte)(topicId & 0xFF);
		data[headerLength - 2] = (byte)((msgId >> 8) & 0xFF);
		data[headerLength - 1] = (byte)(msgId & 0xFF);
		System.arraycopy(topicName.getBytes(), 0, data, headerLength, topicName.length());
		return data;
	}
			
	public int getMsgId() {
		return msgId;
	}

	public void setMsgId(int msgId) {
		this.msgId = msgId;
	}

	public int getTopicId() {
		return topicId;
	}

	public void setTopicId(int topicId) {
		this.topicId = topicId;
	}

	public String getTopicName() {
		return topicName;
	}

	public void setTopicName(String topicName) {
		this.topicName = topicName;
	}
}
