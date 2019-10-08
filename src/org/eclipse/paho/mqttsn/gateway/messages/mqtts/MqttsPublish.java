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
 * This object represents a Mqtts PUBLISH message.
 * 
 *
 */
public class MqttsPublish extends MqttsMessage {
		
	//Mqtts PUBLISH fields
	private boolean  dup;
	private int      qos;
	private	boolean retain;
	private int topicIdType;
	
	private byte[] byteTopicId;
	private int msgId;
	private byte[] pubData = null;
	
	//The form of TopicId (or short topic name) that depends on topicIdType.
	//Maybe either an int or a String.
	private int topicId = 0;
	private String shortTopicName = "";
	
	
	/**
	 * MqttsPublish constructor.Sets the appropriate message type. 
	 */
	public MqttsPublish() {
		msgType = MqttsMessage.PUBLISH;
	}
	
	/**
	 * MqttsPublish constructor.Sets the appropriate message type and constructs 
	 * a Mqtts PUBLISH message from a received byte array.
	 * @param data: The buffer that contains the PUBLISH message.
	 */
	public MqttsPublish(byte[] data) {
		int headerLength = data[0] == 0x01 ? 9 : 7;
		int length = getLength(data);
		msgType = MqttsMessage.PUBLISH;
		dup = ((data[headerLength - 5] & 0x80) >> 7 != 0);
		qos = (data[headerLength - 5] & 0x60) >> 5;
		if(qos == 3) qos = -1;
		retain = ((data[headerLength - 5] & 0x10) >> 4 != 0);
		topicIdType = (data[headerLength - 5] & 0x03);

		byteTopicId = new byte[2];
		byteTopicId[0] = data[headerLength - 4];
		byteTopicId[1] = data[headerLength - 3];

		try {
			if (topicIdType == MqttsMessage.SHORT_TOPIC_NAME)
				shortTopicName = new String(byteTopicId,Utils.STRING_ENCODING);
			else if(topicIdType == MqttsMessage.NORMAL_TOPIC_ID || topicIdType == MqttsMessage.PREDIFINED_TOPIC_ID){
				topicId = ((byteTopicId[0] & 0xFF) << 8) + (byteTopicId[1] & 0xFF);
			}
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}

		msgId   = ((data[headerLength - 2] & 0xFF) << 8) + (data[headerLength - 1] & 0xFF);
		pubData = new byte[length - headerLength];
		System.arraycopy(data, headerLength, pubData, 0, length - headerLength);
	}
	
	/**
	 * Method to convert this message to a byte array for transmission.
	 * @return A byte array containing the PUBLISH message as it should appear on the wire.
	 */
	public byte[] toBytes() {
		int flags = 0;
		if(dup) {
			flags |= 0x80;
		}		
		if(qos == -1) {
			flags |= 0x60;
		} else if(qos == 0) {
		
		} else if(qos == 1) {
			flags |= 0x20;
		} else if(qos == 2) {
			flags |= 0x40;
		} else {
			throw new IllegalArgumentException("Unknown QoS value: " + qos);
		}

		if(retain) {
			flags |= 0x10;
		}

		if(topicIdType == MqttsMessage.NORMAL_TOPIC_ID){
			//do nothing
		}else if (topicIdType == MqttsMessage.PREDIFINED_TOPIC_ID){
			flags |= 0x01;
		}else if (topicIdType == MqttsMessage.SHORT_TOPIC_NAME){
			flags |= 0x02;
		}else {
			throw new IllegalArgumentException("Unknown topic id type: " + topicIdType);
		}

		int headerLength = pubData.length + 7 > 255 ? 9 : 7;
		int length = pubData.length + headerLength;
		byte[] data = setLength(new byte[length], length);
		data[headerLength - 6] = (byte)msgType;
		data[headerLength - 5] = (byte)flags;
		
		byteTopicId = new byte[2];
		if (topicIdType == MqttsMessage.SHORT_TOPIC_NAME)
			byteTopicId = shortTopicName.getBytes();
		else if(topicIdType == MqttsMessage.NORMAL_TOPIC_ID || topicIdType == MqttsMessage.PREDIFINED_TOPIC_ID){
			byteTopicId[0] = (byte)((topicId >> 8) & 0xFF);
			byteTopicId[1] = (byte) (topicId & 0xFF);
		}else 
				throw new IllegalArgumentException("Unknown topic id type: " + topicIdType);
		System.arraycopy(byteTopicId, 0, data, headerLength - 4, byteTopicId.length);
		data[headerLength - 2] = (byte)((msgId >> 8) & 0xFF);
		data[headerLength - 1] = (byte) (msgId & 0xFF);
		System.arraycopy(pubData, 0, data, headerLength, pubData.length);
		return data;
	}

	public boolean isDup() {
		return dup;
	}

	public void setDup(boolean dup) {
		this.dup = dup;
	}

	public int getQos() {
		return qos;
	}

	public void setQos(int qos) {
		this.qos = qos;
	}

	public boolean isRetain() {
		return retain;
	}

	public void setRetain(boolean retain) {
		this.retain = retain;
	}

	public int getTopicIdType() {
		return topicIdType;
	}

	public void setTopicIdType(int topicIdType) {
		this.topicIdType = topicIdType;
	}

	public byte[] getData() {
		return pubData;
	}

	public void setData(byte[] data) {
		this.pubData = data;
	}

	public byte[] getByteTopicId() {
		return byteTopicId;
	}

	public void setByteTopicId(byte[] byteTopicId) {
		this.byteTopicId = byteTopicId;
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

	public String getShortTopicName() {
		return shortTopicName;
	}

	public void setShortTopicName(String shortTopicName) {
		this.shortTopicName = shortTopicName;
	}
}