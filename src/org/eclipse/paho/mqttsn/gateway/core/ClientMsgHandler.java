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

package org.eclipse.paho.mqttsn.gateway.core;

import java.util.ArrayList;
import org.eclipse.paho.mqttsn.gateway.broker.tcp.TCPBrokerInterface;
import org.eclipse.paho.mqttsn.gateway.client.ClientInterface;
import org.eclipse.paho.mqttsn.gateway.exceptions.MqttsException;
import org.eclipse.paho.mqttsn.gateway.messages.Message;
import org.eclipse.paho.mqttsn.gateway.messages.control.ControlMessage;
import org.eclipse.paho.mqttsn.gateway.messages.mqtt.MqttConnack;
import org.eclipse.paho.mqttsn.gateway.messages.mqtt.MqttConnect;
import org.eclipse.paho.mqttsn.gateway.messages.mqtt.MqttDisconnect;
import org.eclipse.paho.mqttsn.gateway.messages.mqtt.MqttMessage;
import org.eclipse.paho.mqttsn.gateway.messages.mqtt.MqttPingReq;
import org.eclipse.paho.mqttsn.gateway.messages.mqtt.MqttPingResp;
import org.eclipse.paho.mqttsn.gateway.messages.mqtt.MqttPubComp;
import org.eclipse.paho.mqttsn.gateway.messages.mqtt.MqttPubRec;
import org.eclipse.paho.mqttsn.gateway.messages.mqtt.MqttPubRel;
import org.eclipse.paho.mqttsn.gateway.messages.mqtt.MqttPuback;
import org.eclipse.paho.mqttsn.gateway.messages.mqtt.MqttPublish;
import org.eclipse.paho.mqttsn.gateway.messages.mqtt.MqttSuback;
import org.eclipse.paho.mqttsn.gateway.messages.mqtt.MqttSubscribe;
import org.eclipse.paho.mqttsn.gateway.messages.mqtt.MqttUnsuback;
import org.eclipse.paho.mqttsn.gateway.messages.mqtt.MqttUnsubscribe;
import org.eclipse.paho.mqttsn.gateway.messages.mqtts.MqttsConnack;
import org.eclipse.paho.mqttsn.gateway.messages.mqtts.MqttsConnect;
import org.eclipse.paho.mqttsn.gateway.messages.mqtts.MqttsDisconnect;
import org.eclipse.paho.mqttsn.gateway.messages.mqtts.MqttsMessage;
import org.eclipse.paho.mqttsn.gateway.messages.mqtts.MqttsPingReq;
import org.eclipse.paho.mqttsn.gateway.messages.mqtts.MqttsPingResp;
import org.eclipse.paho.mqttsn.gateway.messages.mqtts.MqttsPubComp;
import org.eclipse.paho.mqttsn.gateway.messages.mqtts.MqttsPubRec;
import org.eclipse.paho.mqttsn.gateway.messages.mqtts.MqttsPubRel;
import org.eclipse.paho.mqttsn.gateway.messages.mqtts.MqttsPuback;
import org.eclipse.paho.mqttsn.gateway.messages.mqtts.MqttsPublish;
import org.eclipse.paho.mqttsn.gateway.messages.mqtts.MqttsRegack;
import org.eclipse.paho.mqttsn.gateway.messages.mqtts.MqttsRegister;
import org.eclipse.paho.mqttsn.gateway.messages.mqtts.MqttsSearchGW;
import org.eclipse.paho.mqttsn.gateway.messages.mqtts.MqttsSuback;
import org.eclipse.paho.mqttsn.gateway.messages.mqtts.MqttsSubscribe;
import org.eclipse.paho.mqttsn.gateway.messages.mqtts.MqttsUnsuback;
import org.eclipse.paho.mqttsn.gateway.messages.mqtts.MqttsUnsubscribe;
import org.eclipse.paho.mqttsn.gateway.messages.mqtts.MqttsWillMsg;
import org.eclipse.paho.mqttsn.gateway.messages.mqtts.MqttsWillMsgReq;
import org.eclipse.paho.mqttsn.gateway.messages.mqtts.MqttsWillTopic;
import org.eclipse.paho.mqttsn.gateway.messages.mqtts.MqttsWillTopicReq;
import org.eclipse.paho.mqttsn.gateway.timer.TimerService;
import org.eclipse.paho.mqttsn.gateway.utils.ClientAddress;
import org.eclipse.paho.mqttsn.gateway.utils.GWParameters;
import org.eclipse.paho.mqttsn.gateway.utils.GatewayAddress;
import org.eclipse.paho.mqttsn.gateway.utils.GatewayLogger;
import org.eclipse.paho.mqttsn.gateway.utils.UniqueID;
import org.eclipse.paho.mqttsn.gateway.utils.Utils;

/**
 * This object implements the core functions of the protocol translation. For each client there is
 * one instance of this object.Every message (Mqtt, Mqtts or Control) that corresponds to a certain
 * client, is handled by this object.
 */
@SuppressWarnings("Duplicates")
public class ClientMsgHandler extends MsgHandler {

  //the unique address of the client that distinguishes this object
  private ClientAddress clientAddress;

  //the clientId of the client(the one that is sent in Mqtts CONNECT message)
  private String clientId = "...";

  //the ClientInterface (IP, Serial, etc.) in which this object should
  //respond in case of sending a Mqtts message to the client
  private ClientInterface clientInterface;

  //the BrokerInterface which represents an interface for communication with the broker
  private TCPBrokerInterface brokerInterface;

  //a timer service which is used for timeouts
  private TimerService timer;

  //a table that is used for mapping topic Ids with topic names
  private TopicMappingTable topicIdMappingTable;

  //a reference to Dispatcher object
  private Dispatcher dispatcher;

  //class that represents the state of the client at any given time
  private ClientState clientState;

  //class that represents the state of the gateway (actually this handler's state) at any given time
  private GatewayState gateway;

  //variable for checking the time of inactivity of this object
  //in order to remove it from Dispatcher's mapping table
  private long timeout;

  //messages for storing session information
  private MqttsConnect mqttsConnect;
  private MqttsWillTopic mqttsWillTopic;
  private MqttsSubscribe mqttsSubscribe;
  private MqttsUnsubscribe mqttsUnsubscribe;
  private MqttsRegister mqttsRegister;
  private MqttPublish mqttPublish;
  private MqttsPublish mqttsPublish;

  private ArrayList<MqttPublish> bufferedPublishMessages = new ArrayList<>();

  //variables for storing the msgId and topicId that are issued by the gateway
  private UniqueID msgId, localTopicId;

  private int keepAliveTime;


  /**
   * Constructor of the ClientMsgHandler.
   *
   * @param addr The address of the client.
   */
  public ClientMsgHandler(ClientAddress addr) {
    clientAddress = addr;
  }


  /* (non-Javadoc)
   * @see org.eclipse.paho.mqttsn.gateway.core.MsgHandler#initialize()
   */
  public void initialize() {
    brokerInterface = new TCPBrokerInterface(clientAddress);
    brokerInterface.setClientId(clientId);
    timer = TimerService.getInstance();
    dispatcher = Dispatcher.getInstance();
    topicIdMappingTable = new TopicMappingTable();
    topicIdMappingTable.initialize();
    timeout = 0;
    clientState = ClientState.NOT_CONNECTED;
    gateway = new GatewayState();
    msgId = new UniqueID(1);
    localTopicId = new UniqueID(GWParameters.getPredfTopicIdSize() + 1);
  }

  /******************************************************************************************/
  /**                      HANDLING OF MQTTS MESSAGES FROM THE CLIENT                     **/
  /****************************************************************************************/

  private boolean sendMessageToBroker(MqttMessage message, String messageType) {
    try {
      brokerInterface.sendMsg(message);
    } catch (MqttsException e) {
      e.printStackTrace();
      log(GatewayLogger.ERROR,
          String.format("Failed sending Mqtt %s message to the broker.", messageType));
      connectionLost();
      return false;
    }
    return true;
  }

  /* (non-Javadoc)
   * @see org.eclipse.paho.mqttsn.gateway.core.MsgHandler#handleMqttsMessage(org.eclipse.paho.mqttsn.gateway.messages.mqtts.MqttsMessage)
   */
  public void handleMqttsMessage(MqttsMessage receivedMsg) {
    //update this handler's timeout
    timeout = System.currentTimeMillis() + GWParameters.getHandlerTimeout() * 1000;

    //get the type of the Mqtts message and handle the message according to that type
    switch (receivedMsg.getMsgType()) {
      case MqttsMessage.SEARCHGW:
        handleMqttsSearchGW((MqttsSearchGW) receivedMsg);
        break;

      //we will never receive these types of messages from the client
      case MqttsMessage.CONNACK:
      case MqttsMessage.WILLTOPICREQ:
      case MqttsMessage.GWINFO:
      case MqttsMessage.WILLMSGREQ:
      case MqttsMessage.SUBACK:
      case MqttsMessage.UNSUBACK:
      case MqttsMessage.WILLTOPICRESP:
      case MqttsMessage.WILLMSGRESP:
        break;

      case MqttsMessage.CONNECT:
        handleMqttsConnect((MqttsConnect) receivedMsg);
        break;

      case MqttsMessage.WILLTOPIC:
        handleMqttsWillTopic((MqttsWillTopic) receivedMsg);
        break;

      case MqttsMessage.WILLMSG:
        handleMqttsWillMsg((MqttsWillMsg) receivedMsg);
        break;

      case MqttsMessage.REGISTER:
        handleMqttsRegister((MqttsRegister) receivedMsg);
        break;

      case MqttsMessage.REGACK:
        handleMqttsRegack((MqttsRegack) receivedMsg);
        break;

      case MqttsMessage.PUBLISH:
        handleMqttsPublish((MqttsPublish) receivedMsg);
        break;

      case MqttsMessage.PUBACK:
        handleMqttsPuback((MqttsPuback) receivedMsg);
        break;

      case MqttsMessage.PUBCOMP:
        handleMqttsPubComp((MqttsPubComp) receivedMsg);
        break;

      case MqttsMessage.PUBREC:
        handleMqttsPubRec((MqttsPubRec) receivedMsg);
        break;

      case MqttsMessage.PUBREL:
        handleMqttsPubRel((MqttsPubRel) receivedMsg);
        break;

      case MqttsMessage.SUBSCRIBE:
        handleMqttsSubscribe((MqttsSubscribe) receivedMsg);
        break;

      case MqttsMessage.UNSUBSCRIBE:
        handleMqttsUnsubscribe((MqttsUnsubscribe) receivedMsg);
        break;

      case MqttsMessage.PINGREQ:
        handleMqttsPingReq((MqttsPingReq) receivedMsg);
        break;

      case MqttsMessage.PINGRESP:
        handleMqttsPingResp((MqttsPingResp) receivedMsg);
        break;

      case MqttsMessage.DISCONNECT:
        handleMqttsDisconnect((MqttsDisconnect) receivedMsg);
        break;

      case MqttsMessage.WILLTOPICUPD:
        log(GatewayLogger.INFO, "Mqtts WILLTOPICUPD message received.");
        break;

      case MqttsMessage.WILLMSGUPD:
        log(GatewayLogger.INFO, "Mqtts WILLMSGUPD received.");
        break;

      default:
        log(GatewayLogger.WARN,
            "Mqtts message of unknown type \"" + receivedMsg.getMsgType() + "\" received.");
        break;
    }
  }


  /**
   * The method that handles a Mqtts SEARCHGW message.
   *
   * @param receivedMsg The received MqttsSearchGW message.
   */
  private void handleMqttsSearchGW(MqttsSearchGW receivedMsg) {
    //construct an "internal" message (see org.eclipse.paho.mqttsn.gateway.messages.Message)
    //for the GatewayMsgHandler and put it to the dispatcher's queue
    GatewayAddress gwAddress = GWParameters.getGatewayAddress();
    Message msg = new Message(gwAddress);
    msg.setType(Message.MQTTS_MSG);
    msg.setMqttsMessage(receivedMsg);
    dispatcher.putMessage(msg);
  }


  /**
   * The method that handles a Mqtts CONNECT message.
   *
   * @param receivedMsg The received MqttsConnect message.
   */
  private void handleMqttsConnect(MqttsConnect receivedMsg) {
    log(GatewayLogger.INFO, "Mqtts CONNECT message with \"Will\" = \"" + receivedMsg.isWill()
        + "\" and \"CleanSession\" = \"" + receivedMsg.isCleanSession() + "\" received.");

    if (clientState == ClientState.ASLEEP || clientState == ClientState.AWAKE) {
      handleConnectAfterSleep();
      return;
    }

    clientId = receivedMsg.getClientId();
    brokerInterface.setClientId(clientId);

    //if the client is already connected return a Mqtts CONNACK
    if (clientState == ClientState.CONNECTED) {
      log(GatewayLogger.WARN,
          "Client is already connected. Mqtts CONNACK message will be send to the client.");
      MqttsConnack connack = new MqttsConnack();
      connack.setReturnCode(MqttsMessage.RETURN_CODE_ACCEPTED);
      log(GatewayLogger.INFO, "Sending Mqtts CONNACK message to the client.");
      clientInterface.sendMsg(this.clientAddress, connack);
      return;
    }

    //if the gateway is already in process of establishing a connection with the client, drop the message
    if (gateway.isEstablishingConnection()) {
      log(GatewayLogger.WARN,
          "Client is already establishing a connection. The received Mqtts CONNECT message cannot be processed.");
      return;
    }

    //if the will flag of the Mqtts CONNECT message is not set,
    //construct a Mqtt CONNECT message, send it to the broker and return
    if (!receivedMsg.isWill()) {
      MqttConnect mqttConnect = new MqttConnect();
      mqttConnect.setProtocolName(receivedMsg.getProtocolName());
      mqttConnect.setProtocolVersion(receivedMsg.getProtocolVersion());
      mqttConnect.setWill(receivedMsg.isWill());
      mqttConnect.setCleanStart(receivedMsg.isCleanSession());
      mqttConnect.setKeepAlive(receivedMsg.getDuration());
      mqttConnect.setClientId(receivedMsg.getClientId());
      keepAliveTime = receivedMsg.getDuration();

      //open a new TCP/IP connection with the broker
      try {
        brokerInterface.initialize();
      } catch (MqttsException e) {
        e.printStackTrace();
        log(GatewayLogger.ERROR,
            "An error occurred while TCP/IP connection setup with the broker.");
        return;
      }

      log(GatewayLogger.INFO, "Sending Mqtt CONNECT message to the broker.");
      try {
        brokerInterface.sendMsg(mqttConnect);
        clientState = ClientState.CONNECTED;
      } catch (MqttsException e) {
        e.printStackTrace();
        log(GatewayLogger.ERROR, "Failed sending Mqtt CONNECT message to the broker.");
      }
      return;
    }

    mqttsConnect = receivedMsg;

    log(GatewayLogger.INFO, "Sending Mqtts WILLTOPICREQ message to the client.");
    clientInterface.sendMsg(clientAddress, new MqttsWillTopicReq());
    gateway.setWaitingWillTopic();
    gateway.increaseTriesSendingWillTopicReq();
    timer.register(clientAddress, ControlMessage.WAITING_WILLTOPIC_TIMEOUT,
        GWParameters.getWaitingTime());
  }

  private void handleConnectAfterSleep() {
    timer.unregister(clientAddress, ControlMessage.SLEEP_TIMEOUT);
    timer.unregister(clientAddress, ControlMessage.SLEEP_SERVER_PING);
    clientState = ClientState.CONNECTED;
    bufferedPublishMessages.forEach(this::handleMqttPublish);
    bufferedPublishMessages.clear();
    clientInterface.sendMsg(clientAddress, new MqttsConnack());
  }

  /**
   * The method that handles a Mqtts WILLTOPIC message.
   *
   * @param receivedMsg The received MqttsWillTopic message.
   */
  private void handleMqttsWillTopic(MqttsWillTopic receivedMsg) {
    log(GatewayLogger.INFO,
        "Mqtts WILLTOPIC message with \"WillTopic\" = \"" + receivedMsg.getWillTopic()
            + "\" received.");

    if (!gateway.isWaitingWillTopic()) {
      log(GatewayLogger.WARN,
          "Gateway is not waiting a Mqtts WILLTOPIC message from the client. The received message cannot be processed.");
      return;
    }

    gateway.resetWaitingWillTopic();
    gateway.resetTriesSendingWillTopicReq();
    timer.unregister(clientAddress, ControlMessage.WAITING_WILLTOPIC_TIMEOUT);

    mqttsWillTopic = receivedMsg;

    log(GatewayLogger.INFO, "Sending Mqtts WILLMSGREQ message to the client.");
    clientInterface.sendMsg(clientAddress, new MqttsWillMsgReq());

    gateway.setWaitingWillMsg();
    gateway.increaseTriesSendingWillMsgReq();
    timer.register(clientAddress, ControlMessage.WAITING_WILLMSG_TIMEOUT,
        GWParameters.getWaitingTime());
  }

  /**
   * The method that handles a Mqtts WILLMSG message.
   *
   * @param receivedMsg The received MqttsWillMsg message.
   */
  private void handleMqttsWillMsg(MqttsWillMsg receivedMsg) {
    log(GatewayLogger.INFO,
        "Mqtts WILLMSG message with \"WillMsg\" = \"" + receivedMsg.getWillMsg() + "\" received.");

    if (!gateway.isWaitingWillMsg()) {
      log(GatewayLogger.WARN,
          "Gateway is not waiting a Mqtts WILLMSG message from the client.The received message cannot be processed.");
      return;
    }

    gateway.resetWaitingWillMsg();
    gateway.resetTriesSendingWillMsgReq();
    timer.unregister(clientAddress, ControlMessage.WAITING_WILLMSG_TIMEOUT);

    //assure that the stored Mqtts CONNECT and Mqtts WILLTOPIC messages that we received before are not null
    //if one of them is null delete the other and return (debugging checks)
    if (mqttsConnect == null || mqttsWillTopic == null) {
      log(GatewayLogger.WARN,
          "The stored Mqtts CONNECT or Mqtts WILLTOPIC message is null. The received Mqtts WILLMSG message cannot be processed.");
      mqttsWillTopic = null;
      mqttsConnect = null;
      return;
    }

    MqttConnect mqttConnect = new MqttConnect();
    mqttConnect.setProtocolName(mqttsConnect.getProtocolName());
    mqttConnect.setProtocolVersion(mqttsConnect.getProtocolVersion());
    mqttConnect.setWillRetain(mqttsWillTopic.isRetain());
    mqttConnect.setWillQoS(mqttsWillTopic.getQos());
    mqttConnect.setWill(mqttsConnect.isWill());
    mqttConnect.setCleanStart(mqttsConnect.isCleanSession());
    mqttConnect.setKeepAlive(mqttsConnect.getDuration());
    mqttConnect.setClientId(mqttsConnect.getClientId());
    mqttConnect.setWillTopic(mqttsWillTopic.getWillTopic());
    mqttConnect.setWillMessage(receivedMsg.getWillMsg());

    //open a new TCP/IP connection with the broker
    try {
      brokerInterface.initialize();
    } catch (MqttsException e) {
      e.printStackTrace();
      log(GatewayLogger.ERROR, "An error occurred while TCP/IP connection setup with the broker.");
      return;
    }

    log(GatewayLogger.INFO, "Sending Mqtt CONNECT message to the broker.");
    if (sendMessageToBroker(mqttConnect, "CONNECT")) {
      clientState = ClientState.CONNECTED;
      mqttsConnect = null;
      mqttsWillTopic = null;
    }
  }


  /**
   * The method that handles a Mqtts REGISTER message.
   *
   * @param receivedMsg The received MqttsRegister message.
   */
  private void handleMqttsRegister(MqttsRegister receivedMsg) {
    log(GatewayLogger.INFO,
        "Mqtts REGISTER message with \"TopicName\" = \"" + receivedMsg.getTopicName()
            + "\" received.");

    if (clientState != ClientState.CONNECTED) {
      log(GatewayLogger.WARN,
          "Client is not connected. The received Mqtts REGISTER message cannot be processed.");
      sendClientDisconnect();
      return;
    }

    if (topicIdMappingTable.getTopicId(receivedMsg.getTopicName()) == 0) {
      topicIdMappingTable.assignTopicId(localTopicId.getUniqueID(), receivedMsg.getTopicName());
    }

    MqttsRegack regack = new MqttsRegack();
    regack.setTopicId(topicIdMappingTable.getTopicId(receivedMsg.getTopicName()));
    regack.setMsgId(receivedMsg.getMsgId());
    regack.setReturnCode(MqttsMessage.RETURN_CODE_ACCEPTED);

    log(GatewayLogger.INFO,
        "Sending Mqtts REGACK message with \"TopicID\" = \"" + regack.getTopicId()
            + "\" to the client.");
    clientInterface.sendMsg(clientAddress, regack);
  }

  /**
   * The method that handles a Mqtts REGACK message.
   *
   * @param receivedMsg The received MqttsRegack message.
   */
  private void handleMqttsRegack(MqttsRegack receivedMsg) {
    log(GatewayLogger.INFO,
        "Mqtts REGACK message with \"TopicId\" = \"" + receivedMsg.getTopicId() + "\" received.");

    if (clientState != ClientState.CONNECTED) {
      log(GatewayLogger.WARN,
          "Client is not connected. The received Mqtts REGACK message cannot be processed.");
      sendClientDisconnect();
      return;
    }

    if (!gateway.isWaitingRegack()) {
      log(GatewayLogger.WARN,
          "Gateway is not waiting a Mqtts REGACK message from the client. The received message cannot be processed.");
      return;
    }

    //assure that the stored Mqtts REGISTER and Mqtt PUBLISH messages are not null
    //if one of them is null delete the other and return (debugging checks)
    if (this.mqttsRegister == null || this.mqttPublish == null) {
      log(GatewayLogger.WARN,
          "The stored Mqtts REGISTER or Mqtt PUBLISH message is null. The received Mqtts REGACK message cannot be processed.");

      gateway.resetWaitingRegack();
      gateway.resetTriesSendingRegister();
      timer.unregister(this.clientAddress, ControlMessage.WAITING_REGACK_TIMEOUT);

      mqttsRegister = null;
      mqttPublish = null;
      return;
    }

    //if the MsgId of the received Mqtt REGACK is not the same with MsgId of the stored Mqtts
    //REGISTER message drop the received message and return (don't delete any stored message)
    if (receivedMsg.getMsgId() != this.mqttsRegister.getMsgId()) {
      log(GatewayLogger.WARN, "MsgId (\"" + receivedMsg.getMsgId()
          + "\") of the received Mqtts REGACK message does not match the MsgId (\""
          + this.mqttsRegister.getMsgId()
          + "\") of the stored Mqtts REGISTER message. The message cannot be processed.");
      return;
    }

    topicIdMappingTable.assignTopicId(receivedMsg.getTopicId(), this.mqttPublish.getTopicName());

    MqttsPublish publish = new MqttsPublish();
    publish.setDup(mqttPublish.isDup());
    publish.setQos(mqttPublish.getQos());
    publish.setRetain(mqttPublish.isRetain());
    publish.setTopicIdType(MqttsMessage.NORMAL_TOPIC_ID);
    publish.setTopicId(receivedMsg.getTopicId());
    publish.setMsgId(mqttPublish.getMsgId());
    publish.setData(mqttPublish.getPayload());

    log(GatewayLogger.INFO, "Sending Mqtts PUBLISH message with \"QoS\" = \"" + mqttPublish.getQos()
        + "\" and \"TopicId\" = \"" + receivedMsg.getTopicId() + "\" to the client.");
    clientInterface.sendMsg(this.clientAddress, publish);

    gateway.resetWaitingRegack();
    gateway.resetTriesSendingRegister();
    timer.unregister(this.clientAddress, ControlMessage.WAITING_REGACK_TIMEOUT);
    this.mqttsRegister = null;
    this.mqttPublish = null;
  }


  /**
   * The method that handles a Mqtts PUBLISH message.
   *
   * @param receivedMsg The received MqttsPublish message.
   */
  private void handleMqttsPublish(MqttsPublish receivedMsg) {
    String logMessage = "Mqtts PUBLISH message with \"QoS\" = \"" + receivedMsg.getQos()
        + "\" and \"TopicId\" = \"" + receivedMsg.getTopicId() + "\" ";
    switch (receivedMsg.getTopicIdType()) {
      case MqttsMessage.NORMAL_TOPIC_ID:
        break;
      case MqttsMessage.PREDIFINED_TOPIC_ID:
        logMessage += "(predefined topicId) ";
        break;
      case MqttsMessage.SHORT_TOPIC_NAME:
        logMessage += "(short topic name) ";
        break;
      default:
        log(GatewayLogger.WARN,
            "Mqtts PUBLISH message with unknown topicIdType (\"" + receivedMsg.getTopicIdType()
                + "\") received. The message cannot be processed.");
        return;
    }
    log(GatewayLogger.INFO, logMessage + "received.");

    //if Mqtts PUBLISH message has QoS = -1, construct an "internal" message (see org.eclipse.paho.mqttsn.gateway.core.Message)
    //for the GatewayMsgHandler and put it to the dispatcher's queue
    if (receivedMsg.getQos() == -1) {
      log(GatewayLogger.INFO,
          "The received Mqtts PUBLISH message with \"QoS\" = \"-1\" will be handled by GatewayMsgHandler.");

      Message msg = new Message(GWParameters.getGatewayAddress());

      msg.setType(Message.MQTTS_MSG);
      msg.setMqttsMessage(receivedMsg);
      dispatcher.putMessage(msg);
      return;
    }

    if (clientState != ClientState.CONNECTED) {
      log(GatewayLogger.WARN,
          "Client is not connected. The received Mqtts PUBLISH message cannot be processed.");
      sendClientDisconnect();
      return;
    }

    //if there is already a publish procedure from the client with QoS 1 and the
    //gateway is expecting a Mqtt PUBACK from the broker, then drop the message if it has QoS 1
    if (gateway.isWaitingPuback() && (receivedMsg.getQos() == 1 || receivedMsg.getQos() == 2)) {
      log(GatewayLogger.WARN,
          "Client is already in a publish procedure with \"QoS\" = \"1\". The received Mqtts PUBLISH message with \"QoS\" = \""
              + receivedMsg.getQos() + "\" cannot be processed.");
      return;
    }

    MqttPublish publish = new MqttPublish();

    switch (receivedMsg.getTopicIdType()) {

      case MqttsMessage.NORMAL_TOPIC_ID:
        if (receivedMsg.getTopicId() <= GWParameters.getPredfTopicIdSize()) {
          log(GatewayLogger.WARN, "TopicId (\"" + receivedMsg.getTopicId()
              + "\") of the received Mqtts PUBLISH message is in the range of predefined topic Ids [1,"
              + GWParameters.getPredfTopicIdSize()
              + "]. The message cannot be processed. Mqtts PUBACK with rejection reason will be sent to the client.");

          MqttsPuback puback = new MqttsPuback();
          puback.setTopicId(receivedMsg.getTopicId());
          puback.setMsgId(receivedMsg.getMsgId());
          puback.setReturnCode(MqttsMessage.RETURN_CODE_INVALID_TOPIC_ID);

          log(GatewayLogger.INFO, "Sending Mqtts PUBACK message with \"TopicId\" = \""
              + receivedMsg.getTopicId()
              + "\" and \"ReturnCode\" = \"Rejected: invalid TopicId\" to the client.");
          clientInterface.sendMsg(this.clientAddress, puback);
          return;
        }

        String topicName = topicIdMappingTable.getTopicName(receivedMsg.getTopicId());

        //if there is no such an entry
        if (topicName == null) {
          log(GatewayLogger.WARN, "TopicId (\"" + receivedMsg.getTopicId()
              + "\") of the received Mqtts PUBLISH message does not exist. The message cannot be processed. Mqtts PUBACK with rejection reason will be sent to the client.");

          MqttsPuback puback = new MqttsPuback();
          puback.setTopicId(receivedMsg.getTopicId());
          puback.setMsgId(receivedMsg.getMsgId());
          puback.setReturnCode(MqttsMessage.RETURN_CODE_INVALID_TOPIC_ID);

          log(GatewayLogger.INFO, "Sending Mqtts PUBACK message with \"TopicId\" = \""
              + receivedMsg.getTopicId()
              + "\" and \"ReturnCode\" = \"Rejected: invalid TopicId\" to the client.");
          clientInterface.sendMsg(this.clientAddress, puback);
          return;
        }

        publish.setTopicName(topicName);
        break;

      case MqttsMessage.SHORT_TOPIC_NAME:
        publish.setTopicName(receivedMsg.getShortTopicName());
        break;

      case MqttsMessage.PREDIFINED_TOPIC_ID:
        if (receivedMsg.getTopicId() > GWParameters.getPredfTopicIdSize()) {
          log(GatewayLogger.WARN, "Predefined topicId (\"" + receivedMsg.getTopicId()
              + "\") of the received Mqtts PUBLISH message is out of the range of predefined topic Ids [1,"
              + GWParameters.getPredfTopicIdSize()
              + "]. The message cannot be processed. Mqtts PUBACK with rejection reason will be sent to the client.");

          MqttsPuback puback = new MqttsPuback();
          puback.setTopicId(receivedMsg.getTopicId());
          puback.setMsgId(receivedMsg.getMsgId());
          puback.setReturnCode(MqttsMessage.RETURN_CODE_INVALID_TOPIC_ID);

          log(GatewayLogger.INFO, "Sending Mqtts PUBACK message with \"TopicId\" = \""
              + receivedMsg.getTopicId()
              + "\" and \"ReturnCode\" = \"Rejected: invalid TopicId\" to the client.");
          clientInterface.sendMsg(this.clientAddress, puback);
          return;
        }

        topicName = topicIdMappingTable.getTopicName(receivedMsg.getTopicId());

        //this should not happen as predefined topic ids are already stored
        if (topicName == null) {
          log(GatewayLogger.WARN, "Predefined topicId (\"" + receivedMsg.getTopicId()
              + "\") of the received Mqtts PUBLISH message does not exist. The message cannot be processed. Mqtts PUBACK with rejection reason will be sent to the client.");

          MqttsPuback puback = new MqttsPuback();
          puback.setTopicId(receivedMsg.getTopicId());
          puback.setMsgId(receivedMsg.getMsgId());
          puback.setReturnCode(MqttsMessage.RETURN_CODE_INVALID_TOPIC_ID);

          log(GatewayLogger.INFO, "] - Sending Mqtts PUBACK message with \"TopicId\" = \""
              + receivedMsg.getTopicId()
              + "\" and \"ReturnCode\" = \"Rejected: invalid TopicId\" to the client.");
          clientInterface.sendMsg(this.clientAddress, puback);
          return;
        }

        publish.setTopicName(topicName);
        break;

      default:
        log(GatewayLogger.WARN, "Unknown topicIdType (\"" + receivedMsg.getTopicIdType()
            + "\"). The received Mqtts PUBLISH message cannot be processed.");
        return;
    }

    publish.setDup(receivedMsg.isDup());
    publish.setQos(receivedMsg.getQos());
    publish.setRetain(receivedMsg.isRetain());
    publish.setMsgId(receivedMsg.getMsgId());
    publish.setPayload(receivedMsg.getData());

    log(GatewayLogger.INFO, "Sending Mqtt PUBLISH message with \"QoS\" = \"" + receivedMsg.getQos()
        + "\" and \"TopicName\" = \"" + publish.getTopicName() + "\" to the broker.");
    if (sendMessageToBroker(publish, "PUBLISH")) {
      if (receivedMsg.getQos() == 1 || receivedMsg.getQos() == 2) {
        gateway.setWaitingPuback();
        mqttsPublish = receivedMsg;
      }
    }
  }


  /**
   * The method that handles a Mqtts PUBACK message.
   *
   * @param receivedMsg The received MqttsPuback message.
   */
  private void handleMqttsPuback(MqttsPuback receivedMsg) {
    log(GatewayLogger.INFO, "Mqtts PUBACK message received.");

    if (clientState != ClientState.CONNECTED) {
      log(GatewayLogger.WARN,
          "Client is not connected. The received Mqtts PUBACK message cannot be processed.");
      sendClientDisconnect();
      return;
    }

    //if the return code of the Mqtts PUBACK message is "Rejected: Invalid topic ID", then
    //delete this topicId(and the associate topic name)from the mapping table
    if (receivedMsg.getReturnCode() == MqttsMessage.RETURN_CODE_INVALID_TOPIC_ID) {
      log(GatewayLogger.WARN,
          "The received Mqtts PUBACK has \"ReturnCode\" = \"Rejected: invalid TopicId\". TopicId \""
              + receivedMsg.getTopicId() + "\" will be deleted from mapping table.");
      topicIdMappingTable.removeTopicId(receivedMsg.getTopicId());
      return;
    }

    log(GatewayLogger.INFO, "Sending Mqtt PUBACK message to the broker.");
    sendMessageToBroker(new MqttPuback(receivedMsg.getMsgId()), "PUBACK");
  }


  /**
   * The method that handles a Mqtts PUBCOMP message.
   *
   * @param receivedMsg The received MqttsPubComp message.
   */
  private void handleMqttsPubComp(MqttsPubComp receivedMsg) {
    log(GatewayLogger.INFO, "Mqtts PUBCOMP message received.");

    if (clientState != ClientState.CONNECTED) {
      log(GatewayLogger.WARN,
          "Client is not connected. The received Mqtts PUBCOMP message cannot be processed.");
      sendClientDisconnect();
      return;
    }

    log(GatewayLogger.INFO, "Sending Mqtt PUBCOMP message to the broker.");
    sendMessageToBroker(new MqttPubComp(receivedMsg.getMsgId()), "PUBCOMP");
  }


  /**
   * The method that handles a Mqtts PUBREC message.
   *
   * @param receivedMsg The received MqttsPubRec message.
   */
  private void handleMqttsPubRec(MqttsPubRec receivedMsg) {
    log(GatewayLogger.INFO, "Mqtts PUBREC message received.");

    if (clientState != ClientState.CONNECTED) {
      log(GatewayLogger.WARN,
          "Client is not connected. The received Mqtts PUBREC message cannot be processed.");
      sendClientDisconnect();
      return;
    }

    log(GatewayLogger.INFO, "Sending Mqtt PUBREC message to the broker.");
    sendMessageToBroker(new MqttPubRec(receivedMsg.getMsgId()), "PUBREC");
  }


  /**
   * The method that handles a Mqtts PUBREL message.
   *
   * @param receivedMsg The received MqttsPubRel message.
   */
  private void handleMqttsPubRel(MqttsPubRel receivedMsg) {
    log(GatewayLogger.INFO, "Mqtts PUBREL message received.");

    if (clientState != ClientState.CONNECTED) {
      log(GatewayLogger.WARN,
          "Client is not connected. The received Mqtts PUBREL message cannot be processed.");
      sendClientDisconnect();
      return;
    }

    log(GatewayLogger.INFO, "Sending Mqtt PUBREL message to the broker.");
    sendMessageToBroker(new MqttPubRel(receivedMsg.getMsgId()), "PUBREL");
  }


  /**
   * The method that handles a Mqtts SUBSCRIBE message.
   *
   * @param receivedMsg The received MqttsSubscribe message.
   */
  private void handleMqttsSubscribe(MqttsSubscribe receivedMsg) {
    if (receivedMsg.getTopicIdType() == MqttsMessage.TOPIC_NAME) {
      log(GatewayLogger.INFO,
          "Mqtts SUBSCRIBE message with \"TopicName\" = \"" + receivedMsg.getTopicName()
              + "\" received.");
    } else if (receivedMsg.getTopicIdType() == MqttsMessage.PREDIFINED_TOPIC_ID) {
      log(GatewayLogger.INFO,
          "Mqtts SUBSCRIBE message with \"TopicId\" = \"" + receivedMsg.getPredefinedTopicId()
              + "\" (predefined topic Id) received.");
    } else if (receivedMsg.getTopicIdType() == MqttsMessage.SHORT_TOPIC_NAME) {
      log(GatewayLogger.INFO,
          "Mqtts SUBSCRIBE message with \"TopicId\" = \"" + receivedMsg.getShortTopicName()
              + "\" (short topic name) received.");
    } else {
      log(GatewayLogger.WARN,
          "Mqtts SUBSCRUBE message with unknown topicIdType (\"" + receivedMsg.getTopicIdType()
              + "\") received. The message cannot be processed");
      return;
    }

    if (clientState != ClientState.CONNECTED) {
      log(GatewayLogger.WARN,
          "Client is not connected. The received Mqtts SUBSCRIBE message cannot be processed.");
      sendClientDisconnect();
      return;
    }

    //if we are already in a subscription process, drop the received message and return
    if (gateway.isWaitingSuback()) {
      log(GatewayLogger.WARN,
          "Client is already in a subscription procedure. The received Mqtts SUBSCRIBE message cannot be processed.");
      return;
    }

    MqttSubscribe mqttSubscribe = new MqttSubscribe();

    switch (receivedMsg.getTopicIdType()) {

      case MqttsMessage.TOPIC_NAME:
        mqttSubscribe.setTopicName(receivedMsg.getTopicName());
        break;

      case MqttsMessage.SHORT_TOPIC_NAME:
        mqttSubscribe.setTopicName(receivedMsg.getShortTopicName());
        break;

      case MqttsMessage.PREDIFINED_TOPIC_ID:
        if (receivedMsg.getPredefinedTopicId() > GWParameters.getPredfTopicIdSize()) {
          log(GatewayLogger.WARN, "Predefined topicId (\"" + +receivedMsg.getPredefinedTopicId()
              + "\") of the received Mqtts SUBSCRIBE message is out of the range of predefined topic Ids [1,"
              + GWParameters.getPredfTopicIdSize()
              + "]. The message cannot be processed. Mqtts SUBACK with rejection reason will be sent to the client.");

          MqttsSuback suback = new MqttsSuback();
          suback.setTopicIdType(MqttsMessage.PREDIFINED_TOPIC_ID);
          suback.setPredefinedTopicId(receivedMsg.getPredefinedTopicId());
          suback.setMsgId(receivedMsg.getMsgId());
          suback.setReturnCode(MqttsMessage.RETURN_CODE_INVALID_TOPIC_ID);

          log(GatewayLogger.INFO, "Sending Mqtts SUBACK message with \"TopicId\" = \""
              + receivedMsg.getPredefinedTopicId()
              + "\" and \"ReturnCode\" = \"Rejected: invalid TopicId\" to the client.");

          clientInterface.sendMsg(this.clientAddress, suback);

          return;
        }

        String topicName = topicIdMappingTable.getTopicName(receivedMsg.getPredefinedTopicId());

        //this should not happen as predefined topic ids are already stored
        if (topicName == null) {
          log(GatewayLogger.WARN, "Predefined topicId (\"" + receivedMsg.getPredefinedTopicId()
              + "\") of the received Mqtts SUBSCRIBE message does not exist. The message cannot be processed. Mqtts SUBACK with rejection reason will be sent to the client.");

          MqttsSuback suback = new MqttsSuback();
          suback.setTopicIdType(MqttsMessage.PREDIFINED_TOPIC_ID);
          suback.setPredefinedTopicId(receivedMsg.getPredefinedTopicId());
          suback.setMsgId(receivedMsg.getMsgId());
          suback.setReturnCode(MqttsMessage.RETURN_CODE_INVALID_TOPIC_ID);

          log(GatewayLogger.INFO, "Sending Mqtts SUBACK message with \"TopicId\" = \""
              + receivedMsg.getPredefinedTopicId()
              + "\" and \"ReturnCode\" = \"Rejected: invalid TopicId\" to the client.");

          clientInterface.sendMsg(this.clientAddress, suback);
          return;
        }
        mqttSubscribe.setTopicName(topicName);
        break;

      default:
        log(GatewayLogger.WARN, "Unknown topicIdType (\"" + receivedMsg.getTopicIdType()
            + "\"). The received Mqtts SUBSCRIBE message cannot be processed.");
        return;
    }

    mqttsSubscribe = receivedMsg;
    mqttSubscribe.setDup(receivedMsg.isDup());
    mqttSubscribe.setMsgId(receivedMsg.getMsgId());
    mqttSubscribe.setRequestedQoS(receivedMsg.getQos());

    log(GatewayLogger.INFO, "Sending Mqtt SUBSCRIBE message with \"TopicName\" = \"" + mqttSubscribe
        .getTopicName() + "\" to the broker.");
    if (sendMessageToBroker(mqttSubscribe, "SUBSCRIBE")) {
      gateway.setWaitingSuback();
    }
  }


  /**
   * The method that handles a Mqtts UNSUBSCRIBE message.
   *
   * @param receivedMsg The received MqttsUnsubscribe message.
   */
  private void handleMqttsUnsubscribe(MqttsUnsubscribe receivedMsg) {
    if (receivedMsg.getTopicIdType() == MqttsMessage.TOPIC_NAME) {
      log(GatewayLogger.INFO,
          "Mqtts UNSUBSCRIBE message with \"TopicName\" = \"" + receivedMsg.getTopicName()
              + "\" received.");
    } else if (receivedMsg.getTopicIdType() == MqttsMessage.PREDIFINED_TOPIC_ID) {
      log(GatewayLogger.INFO, "Mqtts UNSUBSCRIBE message with \"TopicId\" = \"" + receivedMsg
          .getPredefinedTopicId() + "\" (predefined topid Id) received.");
    } else if (receivedMsg.getTopicIdType() == MqttsMessage.SHORT_TOPIC_NAME) {
      log(GatewayLogger.INFO, "Mqtts UNSUBSCRIBE message with \"TopicId\" = \"" + receivedMsg
          .getShortTopicName() + "\" (short topic name) received.");
    } else {
      log(GatewayLogger.WARN, "Mqtts UNSUBSCRIBE message with unknown topicIdType (\"" + receivedMsg
          .getTopicIdType() + "\") received. The message cannot be processed.");
      return;
    }

    if (clientState != ClientState.CONNECTED) {
      log(GatewayLogger.WARN,
          "Client is not connected. The received Mqtts UNSUBSCRIBE message cannot be processed.");
      sendClientDisconnect();
      return;
    }

    //if we are already in an un-subscription process, drop the received message and return
    if (gateway.isWaitingUnsuback()) {
      log(GatewayLogger.WARN,
          "Client is already in an un-subscription procedure. The received Mqtts UNSUBSCRIBE message cannot be processed.");
      return;
    }

    MqttUnsubscribe mqttUnsubscribe = new MqttUnsubscribe();

    switch (receivedMsg.getTopicIdType()) {

      case MqttsMessage.TOPIC_NAME:
        mqttUnsubscribe.setTopicName(receivedMsg.getTopicName());
        break;

      case MqttsMessage.SHORT_TOPIC_NAME:
        mqttUnsubscribe.setTopicName(receivedMsg.getShortTopicName());
        break;

      case MqttsMessage.PREDIFINED_TOPIC_ID:
        if (receivedMsg.getPredefinedTopicId() > GWParameters.getPredfTopicIdSize()) {
          log(GatewayLogger.WARN, "Predefined topicId (\"" + +receivedMsg.getPredefinedTopicId()
              + "\") of the received Mqtts UNSUBSCRIBE message is out of the range of predefined topic Ids [1,"
              + GWParameters.getPredfTopicIdSize() + "]. The message cannot be processed.");
          return;
        }

        String topicName = topicIdMappingTable.getTopicName(receivedMsg.getPredefinedTopicId());

        //this should not happen
        if (topicName == null) {
          log(GatewayLogger.WARN, "Predefined topicId (\"" + +receivedMsg.getPredefinedTopicId()
              + "\") does not exist. The received Mqtts UNSUBSCRIBE message cannot be processed.");
          return;
        }
        mqttUnsubscribe.setTopicName(topicName);
        break;

      default:
        log(GatewayLogger.WARN, "Unknown topicIdType (\"" + receivedMsg.getTopicIdType()
            + "\"). The received Mqtts UNSUBSCRIBE message cannot be processed.");
        return;
    }

    mqttsUnsubscribe = receivedMsg;
    mqttUnsubscribe.setDup(receivedMsg.isDup());
    mqttUnsubscribe.setMsgId(receivedMsg.getMsgId());

    log(GatewayLogger.INFO,
        "Sending Mqtt UNSUBSCRIBE message with \"TopicName\" = \"" + mqttUnsubscribe.getTopicName()
            + "\" to the broker.");
    if (sendMessageToBroker(mqttUnsubscribe, "UNSUBSCRIBE")) {
      gateway.setWaitingUnsuback();
    }
  }


  /**
   * The method that handles a Mqtts PINGREQ message.
   *
   * @param receivedMsg The received MqttsPingReq message.
   */
  private void handleMqttsPingReq(MqttsPingReq receivedMsg) {
    log(GatewayLogger.INFO, "Mqtts PINGREQ message received");

    if (clientState == ClientState.ASLEEP) {
      log(GatewayLogger.INFO, "Client woke up.");
      handleAwake();
      return;
    }

    if (clientState != ClientState.CONNECTED) {
      log(GatewayLogger.WARN,
          "Client is not connected. The received Mqtts PINGREQ message cannot be processed.");
      sendClientDisconnect();
      return;
    }

    log(GatewayLogger.INFO, "Sending Mqtt PINGREQ message to the broker");
    sendMessageToBroker(new MqttPingReq(), "PINGREQ");
  }

  /**
   * The method that handles the wakeup process of a client
   */
  private void handleAwake() {
    // Need to set state first
    clientState = ClientState.AWAKE;
    bufferedPublishMessages.forEach(this::handleMqttPublish);
    bufferedPublishMessages.clear();
    clientInterface.sendMsg(clientAddress, new MqttsPingResp());
    clientState = ClientState.ASLEEP;
  }


  /**
   * The method that handles a Mqtts PINGRESP message.
   *
   * @param receivedMsg The received MqttsPingResp message.
   */
  private void handleMqttsPingResp(MqttsPingResp receivedMsg) {
    log(GatewayLogger.INFO, "Mqtts PINGRESP message received.");

    if (clientState != ClientState.CONNECTED) {
      log(GatewayLogger.WARN,
          "Client is not connected. The received Mqtts PINGRESP message cannot be processed.");
      sendClientDisconnect();
      return;
    }

    log(GatewayLogger.INFO, "Sending Mqtt PINGRESP message to the broker.");
    sendMessageToBroker(new MqttPingResp(), "PINGRESP");
  }


  /**
   * The method that handles a Mqtts DISCONNECT message.
   *
   * @param receivedMsg The received MqttsDisconnect message.
   */
  private void handleMqttsDisconnect(MqttsDisconnect receivedMsg) {
    log(GatewayLogger.INFO, "Mqtts DISCONNECT message received.");

    if (clientState == ClientState.CONNECTED && receivedMsg.getSleepDuration() > 0) {
      log(GatewayLogger.INFO, "Mqtts sleep messsage received.");
      handleSleep(receivedMsg.getSleepDuration());
      return;
    }

    if ((clientState == ClientState.AWAKE || clientState == ClientState.ASLEEP)
        && receivedMsg.getSleepDuration() > 0) {
      log(GatewayLogger.INFO, "Updating client sleep duration");
      updateSleep(receivedMsg.getSleepDuration());
      return;
    }

    if (clientState != ClientState.CONNECTED && clientState != ClientState.ASLEEP
        && clientState != ClientState.AWAKE) {
      log(GatewayLogger.WARN,
          "Client is not connected. The received Mqtts DISCONNECT message cannot be processed.");
      return;
    }

    brokerInterface.setRunning(false);

    log(GatewayLogger.INFO, "Sending Mqtt DISCONNECT message to the broker.");
    try {
      brokerInterface.sendMsg(new MqttDisconnect());
    } catch (MqttsException ignored) {
    }

    sendClientDisconnect();
  }

  /**
   * Method that handles a disconnect message with a set sleep duration
   *
   * @param sleepDuration The maximum sleeping duration
   */
  private void handleSleep(int sleepDuration) {
    clientState = ClientState.ASLEEP;
    timer.register(clientAddress, ControlMessage.SLEEP_SERVER_PING, keepAliveTime);
    timer.register(clientAddress, ControlMessage.SLEEP_TIMEOUT, sleepDuration);
    clientInterface.sendMsg(clientAddress, new MqttsDisconnect());
  }

  /**
   * Method that handles updating the sleep duration to the set sleep durations
   *
   * @param sleepDuration The maximum sleep duration
   */
  private void updateSleep(int sleepDuration) {
    timer.unregister(clientAddress, ControlMessage.SLEEP_TIMEOUT);
    timer.register(clientAddress, ControlMessage.SLEEP_TIMEOUT, sleepDuration);
    clientInterface.sendMsg(clientAddress, new MqttsDisconnect());
  }

  /* (non-Javadoc)
   * @see org.eclipse.paho.mqttsn.gateway.core.MsgHandler#handleMqttMessage(org.eclipse.paho.mqttsn.gateway.messages.mqtt.MqttMessage)
   */
  public void handleMqttMessage(MqttMessage receivedMsg) {
    timeout = System.currentTimeMillis() + GWParameters.getHandlerTimeout() * 1000;

    switch (receivedMsg.getMsgType()) {

      case MqttMessage.CONNACK:
        handleMqttConnack((MqttConnack) receivedMsg);
        break;

      case MqttMessage.PUBLISH:
        handleMqttPublish((MqttPublish) receivedMsg);
        break;

      case MqttMessage.PUBACK:
        handleMqttPuback((MqttPuback) receivedMsg);
        break;

      case MqttMessage.PUBREC:
        handleMqttPubRec((MqttPubRec) receivedMsg);
        break;

      case MqttMessage.PUBREL:
        handleMqttPubRel((MqttPubRel) receivedMsg);
        break;

      case MqttMessage.PUBCOMP:
        handleMqttPubComp((MqttPubComp) receivedMsg);
        break;

      case MqttMessage.SUBACK:
        handleMqttSuback((MqttSuback) receivedMsg);
        break;

      // We will never receive these messages from the broker
      case MqttMessage.CONNECT:
      case MqttMessage.SUBSCRIBE:
      case MqttMessage.UNSUBSCRIBE:
      case MqttMessage.DISCONNECT:
        break;

      case MqttMessage.UNSUBACK:
        handleMqttUnsuback((MqttUnsuback) receivedMsg);
        break;

      case MqttMessage.PINGREQ:
        handleMqttPingReq((MqttPingReq) receivedMsg);
        break;

      case MqttMessage.PINGRESP:
        handleMqttPingResp((MqttPingResp) receivedMsg);
        break;

      default:
        log(GatewayLogger.WARN, "Mqtt message of unknown type \"" + receivedMsg.getMsgType()
            + "\" received.");
        break;
    }
  }


  /**
   * The method that handles a Mqtt CONNACK message.
   *
   * @param receivedMsg The received MqttConnack message.
   */
  private void handleMqttConnack(MqttConnack receivedMsg) {
    log(GatewayLogger.INFO, "Mqtt CONNACK message received.");

    if (clientState != ClientState.CONNECTED) {
      log(GatewayLogger.WARN,
          "Client is not connected. The received Mqtt CONNACK message cannot be processed.");
      return;
    }

    if (receivedMsg.getReturnCode() != MqttMessage.RETURN_CODE_CONNECTION_ACCEPTED) {
      log(GatewayLogger.WARN,
          "Return Code of Mqtt CONNACK message is not \"Connection Accepted\". The received Mqtt CONNACK message cannot be processed.");
      sendClientDisconnect();
      return;
    }

    MqttsConnack msg = new MqttsConnack();
    msg.setReturnCode(MqttsMessage.RETURN_CODE_ACCEPTED);

    log(GatewayLogger.INFO, "Sending Mqtts CONNACK message to the client.");
    clientInterface.sendMsg(clientAddress, msg);
  }


  /**
   * The method that handles a Mqtt PUBLISH message.
   *
   * @param receivedMsg The received MqttPublish message
   */
  private void handleMqttPublish(MqttPublish receivedMsg) {
    log(GatewayLogger.INFO, "Mqtt PUBLISH message with \"QoS\" = \"" + receivedMsg.getQos()
        + "\" and \"TopicName\" = \"" + receivedMsg.getTopicName() + "\" received.");

    if (clientState == ClientState.ASLEEP) {
      log(GatewayLogger.INFO, "Client is asleep. The received Mqtt PUBLISH message is buffered");
      bufferedPublishMessages.add(receivedMsg);
      return;
    }

    if (clientState != ClientState.CONNECTED && clientState != ClientState.AWAKE) {
      log(GatewayLogger.WARN,
          "Client is not connected. The received Mqtt PUBLISH message cannot be processed.");
      return;
    }

    if (receivedMsg.getPayload().length > GWParameters.getMaxMqttsLength() - 7) {
      log(GatewayLogger.WARN,
          "The payload in the received Mqtt PUBLISH message does not fit into a Mqtts PUBLISH message (payload length = "
              + receivedMsg.getPayload().length + ". The message cannot be processed.");
      return;
    }

    if (receivedMsg.getTopicName().length() > GWParameters.getMaxMqttsLength() - 6) {
      log(GatewayLogger.WARN,
          "The topic name in the received Mqtt PUBLISH message does not fit into a Mqtts REGISTER message (topic name length = "
              + receivedMsg.getTopicName().length() + ". The message cannot be processed.");
      return;
    }

    int topicId = topicIdMappingTable.getTopicId(receivedMsg.getTopicName());

    MqttsPublish publish = new MqttsPublish();

    //if topicId exists then accept the Mqtt PUBLISH and send a Mqtts PUBLISH to the client
    if (topicId != 0) {
      publish.setDup(receivedMsg.isDup());
      publish.setQos(receivedMsg.getQos());
      publish.setRetain(receivedMsg.isRetain());
      publish.setMsgId(receivedMsg.getMsgId());
      publish.setData(receivedMsg.getPayload());

      //check if the retrieved topicID is associated with a normal topicName
      if (topicId > GWParameters.getPredfTopicIdSize()) {
        publish.setTopicIdType(MqttsMessage.NORMAL_TOPIC_ID);
        publish.setTopicId(topicId);
        log(GatewayLogger.INFO, "] - Sending Mqtts PUBLISH message with \"QoS\" = \"" + receivedMsg
            .getQos() + "\" and \"TopicId\" = \"" + topicId + "\" to the client.");
      }
      //or a predefined topic Id
      else if (topicId > 0 && topicId <= GWParameters.getPredfTopicIdSize()) {
        publish.setTopicIdType(MqttsMessage.PREDIFINED_TOPIC_ID);
        publish.setTopicId(topicId);
        log(GatewayLogger.INFO, "] - Sending Mqtts PUBLISH message with \"QoS\" = \"" + receivedMsg
            .getQos() + "\" and \"TopicId\" = \"" + topicId + "\" to the client.");
      }
      clientInterface.sendMsg(clientAddress, publish);
      return;
    }

    //handle the case of short topic names
    if (topicId == 0 && receivedMsg.getTopicName().length() == 2) {
      publish.setTopicIdType(MqttsMessage.SHORT_TOPIC_NAME);
      publish.setShortTopicName(receivedMsg.getTopicName());
      publish.setDup(receivedMsg.isDup());
      publish.setQos(receivedMsg.getQos());
      publish.setRetain(receivedMsg.isRetain());
      publish.setMsgId(receivedMsg.getMsgId());
      publish.setData(receivedMsg.getPayload());
      log(GatewayLogger.INFO,
          "] - Sending Mqtts PUBLISH message with \"QoS\" = \"" + receivedMsg.getQos()
              + "\" and \"TopicId\" = \"" + receivedMsg.getTopicName()
              + "\" (short topic name) to the client.");
      return;
    }

    //if topicId doesn't exist and we are not already in a register procedure initiated by
    //the gateway, then store the Mqtts PUBLISH and send a Mqtts REGISTER to the client
    if (topicId == 0 && !gateway.isWaitingRegack()) {
      mqttPublish = receivedMsg;
      topicId = localTopicId.getUniqueID();
      mqttsRegister = new MqttsRegister();
      mqttsRegister.setTopicId(topicId);
      mqttsRegister.setMsgId(msgId.getUniqueID());
      mqttsRegister.setTopicName(receivedMsg.getTopicName());

      log(GatewayLogger.INFO, "Sending Mqtts REGISTER message with \"TopicId\" = \"" + topicId
          + "\"  and \"TopicName\" = \"" + receivedMsg.getTopicName() + "\" to the client.");
      clientInterface.sendMsg(clientAddress, mqttsRegister);

      gateway.setWaitingRegack();
      gateway.increaseTriesSendingRegister();
      timer.register(clientAddress, ControlMessage.WAITING_REGACK_TIMEOUT,
          GWParameters.getWaitingTime());
      return;
    }

    //if topicId doesn't exist and we are already in a register procedure initiated by
    //the gateway, then drop the received Mqtt PUBLISH message
    if (topicId == 0 && gateway.isWaitingRegack()) {
      log(GatewayLogger.WARN, "] - Topic name (\"" + receivedMsg.getTopicName()
          + "\") does not exist in the mapping table and the gateway is waiting a Mqtts REGACK message from the client. The received Mqtt PUBLISH message cannot be processed.");
      return;
    }
  }


  /**
   * The method that handles a Mqtt PUBACK message.
   *
   * @param receivedMsg The received MqttPuback message.
   */
  private void handleMqttPuback(MqttPuback receivedMsg) {
    log(GatewayLogger.INFO, "Mqtt PUBACK message received");

    if (clientState != ClientState.CONNECTED) {
      log(GatewayLogger.WARN,
          "Client is not connected. The received Mqtt PUBACK message cannot be processed.");
      return;
    }

    if (!gateway.isWaitingPuback()) {
      log(GatewayLogger.WARN,
          "Gateway is not waiting a Mqtt PUBACK message from the broker. The received message cannot be processed.");
      return;
    }

    if (mqttsPublish == null) {
      log(GatewayLogger.WARN,
          "The stored Mqtts PUBLISH message is null. The received Mqtt PUBACK message cannot be processed.");
      gateway.resetWaitingPuback();
      return;
    }

    if (receivedMsg.getMsgId() != mqttsPublish.getMsgId()) {
      log(GatewayLogger.WARN,
          "Message ID of the received Mqtt PUBACK does not match the message ID of the stored Mqtts PUBLISH message. The message cannot be processed.");
      return;
    }

    MqttsPuback puback = new MqttsPuback();
    puback.setMsgId(receivedMsg.getMsgId());
    puback.setReturnCode(MqttsMessage.RETURN_CODE_ACCEPTED);

    switch (this.mqttsPublish.getTopicIdType()) {
      case MqttsMessage.NORMAL_TOPIC_ID:
      case MqttsMessage.PREDIFINED_TOPIC_ID:
        puback.setTopicId(mqttsPublish.getTopicId());
        log(GatewayLogger.INFO,
            "Sending Mqtts PUBACK message with \"TopicId\" = \"" + puback.getTopicId()
                + "\" to the client.");
        break;

      case MqttsMessage.SHORT_TOPIC_NAME:
        puback.setShortTopicName(mqttsPublish.getShortTopicName());
        log(GatewayLogger.INFO,
            "Sending Mqtts PUBACK message with the \"TopicId\" = \"" + puback.getShortTopicName()
                + "\" (short topic name) to the client.");
        break;

      default:
        log(GatewayLogger.WARN, "Unknown topicIdType of the stored Mqtts PUBLISH message: "
            + this.mqttsPublish.getTopicIdType()
            + ". The received Mqtt PUBACK message cannot be processed.");
        return;
    }

    clientInterface.sendMsg(this.clientAddress, puback);
    gateway.resetWaitingPuback();
    mqttsPublish = null;
  }


  /**
   * The method that handles a Mqtt PUBREC message.
   *
   * @param receivedMsg The received MqttPubRec message.
   */
  private void handleMqttPubRec(MqttPubRec receivedMsg) {
    log(GatewayLogger.INFO, "Mqtt PUBREC message received.");

    if (clientState != ClientState.CONNECTED) {
      log(GatewayLogger.WARN,
          "Client is not connected. The received Mqtt PUBREC message cannot be processed.");
      return;
    }

    log(GatewayLogger.INFO, "Sending Mqtts PUBREC message to the client.");
    clientInterface.sendMsg(this.clientAddress, new MqttsPubRec(receivedMsg.getMsgId()));
  }


  /**
   * The method that handles a Mqtt PUBREL message.
   *
   * @param receivedMsg The received MqttPubRel message.
   */
  private void handleMqttPubRel(MqttPubRel receivedMsg) {
    log(GatewayLogger.INFO, "Mqtt PUBREL message received.");

    if (clientState != ClientState.CONNECTED) {
      log(GatewayLogger.WARN,
          "Client is not connected. The received Mqtt PUBREL message cannot be processed.");
      return;
    }

    log(GatewayLogger.INFO, "Sending Mqtts PUBREL message to the client.");
    clientInterface.sendMsg(this.clientAddress, new MqttsPubRel(receivedMsg.getMsgId()));
  }

  /**
   * The method that handles a Mqtt PUBCOMP message.
   *
   * @param receivedMsg The received MqttPubComp message.
   */
  private void handleMqttPubComp(MqttPubComp receivedMsg) {
    log(GatewayLogger.INFO, "Mqtt PUBCOM message received");

    if (clientState != ClientState.CONNECTED) {
      log(GatewayLogger.WARN,
          "Client is not connected. The received Mqtt PUBCOMP message cannot be processed.");
      return;
    }

    log(GatewayLogger.INFO, "Sending Mqtts PUBCOMP message to the client");
    clientInterface.sendMsg(clientAddress, new MqttsPubComp(receivedMsg.getMsgId()));
  }

  /**
   * The method that handles a Mqtt SUBACK message.
   *
   * @param receivedMsg The received MqttSuback message.
   */
  private void handleMqttSuback(MqttSuback receivedMsg) {
    log(GatewayLogger.INFO, "Mqtt SUBACK message received");

    if (clientState != ClientState.CONNECTED) {
      log(GatewayLogger.WARN,
          "Client is not connected. The received Mqtt SUBACK message cannot be processed.");
      return;
    }

    if (!gateway.isWaitingSuback()) {
      log(GatewayLogger.WARN,
          "Gateway is not waiting a Mqtt SUBACK message from the broker. The received message cannot be processed.");
      return;
    }

    if (mqttsSubscribe == null) {
      log(GatewayLogger.WARN,
          "THe stored Mqtts SUBSCRIBE is null. The received Mqtt SUBACK message cannot be processed.");
      gateway.resetWaitingSuback();
      return;
    }

    if (receivedMsg.getMsgId() != mqttsSubscribe.getMsgId()) {
      log(GatewayLogger.WARN, "MsgId (\"" + receivedMsg.getMsgId()
          + "\") of the received Mqtts SUBACK message does not match the MsgID (\"" + mqttsSubscribe
          .getMsgId()
          + "\") of the stored Mqtts SUBSCRIBE message. The message cannot be processed.");
      return;
    }

    MqttsSuback suback = new MqttsSuback();
    suback.setGrantedQoS(receivedMsg.getGrantedQoS());
    suback.setMsgId(receivedMsg.getMsgId());
    suback.setReturnCode(MqttsMessage.RETURN_CODE_ACCEPTED);

    switch (mqttsSubscribe.getTopicIdType()) {

      case MqttsMessage.TOPIC_NAME:
        suback.setTopicIdType(MqttsMessage.NORMAL_TOPIC_ID);
        if (mqttsSubscribe.getTopicName().equals("#")
            || mqttsSubscribe.getTopicName().equals("+")
            || mqttsSubscribe.getTopicName().contains("/#/")
            || mqttsSubscribe.getTopicName().contains("/+/")
            || mqttsSubscribe.getTopicName().endsWith("/#")
            || mqttsSubscribe.getTopicName().endsWith("/+")
            || mqttsSubscribe.getTopicName().startsWith("#/")
            || mqttsSubscribe.getTopicName().startsWith("+/")) {
          suback.setTopicId(0);
        } else if (topicIdMappingTable.getTopicId(mqttsSubscribe.getTopicName()) == 0) {
          topicIdMappingTable
              .assignTopicId(localTopicId.getUniqueID(), mqttsSubscribe.getTopicName());
        }
        suback.setTopicId(topicIdMappingTable.getTopicId(mqttsSubscribe.getTopicName()));
        log(GatewayLogger.INFO,
            "Sending Mqtts SUBACK message with \"TopicID\" = \"" + suback.getTopicId()
                + "\" to the client.");
        break;

      case MqttsMessage.SHORT_TOPIC_NAME:
        suback.setTopicIdType(MqttsMessage.SHORT_TOPIC_NAME);
        suback.setShortTopicName(mqttsSubscribe.getShortTopicName());
        log(GatewayLogger.INFO,
            "Sending Mqtts SUBACK message with \"TopicId\" = \"" + suback.getShortTopicName()
                + "\" (short topic name) to the client");
        break;

      case MqttsMessage.PREDIFINED_TOPIC_ID:
        suback.setTopicIdType(MqttsMessage.PREDIFINED_TOPIC_ID);
        suback.setPredefinedTopicId(mqttsSubscribe.getPredefinedTopicId());
        log(GatewayLogger.INFO,
            "Sending Mqtts SUBACK message with \"TopicId\" = \"" + suback.getPredefinedTopicId()
                + "\" to the client.");
        break;

      default:
        log(GatewayLogger.WARN,
            "UnknownTopicId type of the stored Mqtts SUBSCRIBE message: " + mqttsSubscribe
                .getTopicIdType() + ". The received Mqtt SUBACK message cannot be processed.");
        return;
    }

    clientInterface.sendMsg(clientAddress, suback);
    gateway.resetWaitingSuback();
    mqttsSubscribe = null;
  }


  /**
   * The method that handles a Mqtt UNSUBACK message.
   *
   * @param receivedMsg The received MqttUnsuback message.
   */
  private void handleMqttUnsuback(MqttUnsuback receivedMsg) {
    log(GatewayLogger.INFO, "Mqtt UNSUBACK message received.");

    if (clientState != ClientState.CONNECTED) {
      log(GatewayLogger.WARN,
          "Client is not connected. The receieved Mqtt UNSUBACK message cannot be processed.");
      return;
    }

    if (!gateway.isWaitingUnsuback()) {
      log(GatewayLogger.WARN,
          "Gateway is not waiting a Mqtt UNSUBACK message from the broker. The receieved message cannot be processed.");
      return;
    }

    if (this.mqttsUnsubscribe == null) {
      log(GatewayLogger.WARN,
          "The stored Mqtts UNSUBSCRIBE is null. The received Mqtt UNSUBACK message cannot be processed.");
      gateway.resetWaitingUnsuback();
      return;
    }

    if (receivedMsg.getMsgId() != this.mqttsUnsubscribe.getMsgId()) {
      log(GatewayLogger.WARN, "MsgId (\"" + receivedMsg.getMsgId()
          + "\") of the received Mqtts UNSUBACK message does not match the MsgId (\""
          + this.mqttsUnsubscribe.getMsgId()
          + "\") of the stored Mqtts UNSUBSCRIBE message. The message cannot be processed.");
      return;
    }

    if (!(this.mqttsUnsubscribe.getTopicIdType() == MqttsMessage.SHORT_TOPIC_NAME
        || this.mqttsUnsubscribe.getTopicIdType() == MqttsMessage.PREDIFINED_TOPIC_ID)) {
      topicIdMappingTable.removeTopicId(this.mqttsUnsubscribe.getTopicName());
    }

    log(GatewayLogger.INFO, "Sending Mqtts UNSUBBACK message to the client.");
    clientInterface.sendMsg(this.clientAddress, new MqttsUnsuback(receivedMsg.getMsgId()));

    gateway.resetWaitingUnsuback();
    mqttsUnsubscribe = null;
  }


  /**
   * The method that handles a Mqtt PINGREQ message.
   *
   * @param receivedMsg The received MqttPingReq message.
   */
  private void handleMqttPingReq(MqttPingReq receivedMsg) {
    log(GatewayLogger.INFO, "Mqtt PINGREQ message received.");

    if (clientState != ClientState.CONNECTED) {
      log(GatewayLogger.WARN,
          "Client is not connected. The received Mqtt PINGREQ message cannot be processed.");
      return;
    }

    log(GatewayLogger.INFO, "Sending Mqtts PINGREQ message to the client.");
    clientInterface.sendMsg(this.clientAddress, new MqttsPingReq());
  }


  /**
   * The method that handles a Mqtt PINGRESP message.
   *
   * @param receivedMsg The received MqttPingResp message.
   */
  private void handleMqttPingResp(MqttPingResp receivedMsg) {
    log(GatewayLogger.INFO, "Mqtt PINGRESP message received.");

    if (clientState == ClientState.ASLEEP || clientState == ClientState.AWAKE) {
      log(GatewayLogger.INFO, "Client is asleep or awake, ignoring Mqtt PINGRESP");
      return;
    }

    if (clientState != ClientState.CONNECTED) {
      log(GatewayLogger.WARN,
          "Client is not connected. The received Mqtt PINGRESP message cannot be processed.");
      return;
    }

    log(GatewayLogger.INFO, "Sending Mqtts PINGRESP message to the client.");
    clientInterface.sendMsg(this.clientAddress, new MqttsPingResp());
  }

  /* (non-Javadoc)
   * @see org.eclipse.paho.mqttsn.gateway.core.MsgHandler#handleControlMessage(org.eclipse.paho.mqttsn.gateway.messages.control.ControlMessage)
   */
  public void handleControlMessage(ControlMessage receivedMsg) {
    switch (receivedMsg.getMsgType()) {
      case ControlMessage.CONNECTION_LOST:
        connectionLost();
        break;

      case ControlMessage.WAITING_WILLTOPIC_TIMEOUT:
        handleWaitingWillTopicTimeout();
        break;

      case ControlMessage.WAITING_WILLMSG_TIMEOUT:
        handleWaitingWillMsgTimeout();
        break;

      case ControlMessage.WAITING_REGACK_TIMEOUT:
        handleWaitingRegackTimeout();
        break;

      case ControlMessage.CHECK_INACTIVITY:
        handleCheckInactivity();
        break;

      case ControlMessage.SEND_KEEP_ALIVE_MSG:
        //we will never receive such a message
        break;

      case ControlMessage.SHUT_DOWN:
        shutDown();
        break;

      case ControlMessage.SLEEP_TIMEOUT:
        // This indicates that the client has not had connect with the server for its entire sleep duration
        sendClientDisconnect();
        break;

      case ControlMessage.SLEEP_SERVER_PING:
        log(GatewayLogger.INFO, "Sending Mqtt PINGREQ for sleeping client");
        sendMessageToBroker(new MqttPingReq(), "PINGREQ");
        break;

      default:
        log(GatewayLogger.WARN,
            "Control message of unknown type \"" + receivedMsg.getMsgType() + "\" received");
        break;
    }
  }


  /**
   * The method that is invoked when the TCP/IP connection with the broker was lost.
   */
  private void connectionLost() {
    log(GatewayLogger.INFO, "Control CONNECTION_LOST message received");

    if (clientState != ClientState.CONNECTED && clientState != ClientState.AWAKE
        && clientState != ClientState.ASLEEP) {
      log(GatewayLogger.WARN,
          "Client is not connected. The call on connectionLost() method has no effect.");
      return;
    }

    if (clientState != ClientState.CONNECTED) {
      log(GatewayLogger.ERROR,
          "TCP/IP connection with the broker was lost while asleep. Should not happen unless the broker has gone down");
    }

    log(GatewayLogger.ERROR, "TCP/IP connection with the broker was lost.");
    sendClientDisconnect();
  }


  /**
   * The method that is invoked when waiting for a Mqtts WILLTOPIC message from the client has
   * timeout.
   */
  private void handleWaitingWillTopicTimeout() {
    log(GatewayLogger.WARN, "Control WAITING_WILLTOPIC_TIMEOUT message received.");

    if (!gateway.isWaitingWillTopic()) {
      log(GatewayLogger.WARN,
          "Gateway is not waiting a Mqtts WILLTOPIC message from the client. The received control WAITING_WILLTOPIC_TIMEOUT message cannot be processed.");
      return;
    }

    if (gateway.getTriesSendingWillTopicReq() > GWParameters.getMaxRetries()) {
      log(GatewayLogger.INFO,
          "Maximum retries of sending Mqtts WILLTOPICREQ message to the client were reached. The message will not be sent again.");
      gateway.resetWaitingWillTopic();
      gateway.resetTriesSendingWillTopicReq();
      timer.unregister(clientAddress, ControlMessage.WAITING_WILLTOPIC_TIMEOUT);
      mqttsConnect = null;
    } else {
      MqttsWillTopicReq willTopicReq = new MqttsWillTopicReq();
      log(GatewayLogger.INFO,
          "Re-sending Mqtts WILLTOPICREQ message to the client. Retry: " + gateway
              .getTriesSendingWillTopicReq() + ".");
      clientInterface.sendMsg(this.clientAddress, willTopicReq);
      gateway.increaseTriesSendingWillTopicReq();
    }
  }


  /**
   * The method that is invoked when waiting for a Mqtts WILLMSG message from the client has
   * timeout.
   */
  private void handleWaitingWillMsgTimeout() {
    log(GatewayLogger.WARN, "Control WAITING_WILLMSG_TIMEOUT message received.");

    if (!gateway.isWaitingWillMsg()) {
      log(GatewayLogger.WARN,
          "Gateway is not waiting a Mqtts WILLMSG message from the client. The received control WAITING_WILLMSG_TIMEOUT message cannot be processed.");
      return;
    }

    if (gateway.getTriesSendingWillMsgReq() > GWParameters.getMaxRetries()) {
      log(GatewayLogger.INFO,
          "Maximum retries of sending Mqtts WILLMSGREQ message to the client were reached. The message will not be sent again.");
      gateway.resetWaitingWillMsg();
      gateway.resetTriesSendingWillMsgReq();
      timer.unregister(this.clientAddress, ControlMessage.WAITING_WILLMSG_TIMEOUT);
      this.mqttsConnect = null;
      this.mqttsWillTopic = null;
    } else {
      log(GatewayLogger.INFO, "Re-sending Mqtts WILLMSGREQ message to the client. Retry: " + gateway
          .getTriesSendingWillMsgReq() + ".");
      clientInterface.sendMsg(this.clientAddress, new MqttsWillMsgReq());

      gateway.increaseTriesSendingWillMsgReq();
    }
  }


  /**
   * The method that is invoked when waiting for a Mqtts REGACK message from the client has
   * timeout.
   */
  private void handleWaitingRegackTimeout() {
    log(GatewayLogger.WARN, "Control WAITING_REGACK_TIMEOUT message received.");

    if (!gateway.isWaitingRegack()) {
      log(GatewayLogger.WARN,
          "Gateway is not in state of waiting a Mqtts REGACK message from the client. The received control REGACK_TIMEOUT message cannot be processed.");
      return;
    }

    //if we have reached the maximum tries of sending the Mqtts REGISTER message
    if (gateway.getTriesSendingRegister() > GWParameters.getMaxRetries()) {
      log(GatewayLogger.INFO,
          "Maximum retries of sending Mqtts REGISTER message to the client were reached. The message will not be sent again.");
      //"reset" the 'waitingRegack" state of the gateway, "reset" the tries of sending
      //Mqtts REGISTER message to the client, unregister from the timer and delete
      //the stored Mqtt PUBLISH and Mqtts REGISTER messages
      gateway.resetWaitingRegack();
      gateway.resetTriesSendingRegister();
      timer.unregister(this.clientAddress, ControlMessage.WAITING_REGACK_TIMEOUT);
      this.mqttPublish = null;
      this.mqttsRegister = null;
    }

    // Modify the MsgId (get a new one) of the stored Mqtts REGISTER message, and send it to the client
    else {
      mqttsRegister.setMsgId(msgId.getUniqueID());
      log(GatewayLogger.INFO, "Re-sending Mqtts REGISTER message to the client. Retry: " + gateway
          .getTriesSendingRegister() + ".");
      clientInterface.sendMsg(clientAddress, mqttsRegister);
      gateway.increaseTriesSendingRegister();
    }
  }


  /**
   * This method is invoked in regular intervals to check the inactivity of this handler in order to
   * remove it from Dispatcher's mapping table.
   */
  private void handleCheckInactivity() {
    log(GatewayLogger.INFO, "Control CHECK_INACTIVITY message received.");

    if (clientState == ClientState.ASLEEP || clientState == ClientState.AWAKE) {
      log(GatewayLogger.INFO, "Client is asleep, CHECK_INACTIVITY message ignored");
      return;
    }

    if (System.currentTimeMillis() > timeout) {
      log(GatewayLogger.WARN,
          "Client is inactive for more than " + GWParameters.getHandlerTimeout() / 60
              + " minutes. The associated ClientMsgHandler will be removed from Dispatcher's mapping table.");
      brokerInterface.disconnect();
      dispatcher.removeHandler(clientAddress);
    }
  }

  /**
   * This method is invoked when the gateway is shutting down.
   */
  private void shutDown() {
    log(GatewayLogger.INFO, "Control SHUT_DOWN message received");

    if (clientState != ClientState.CONNECTED) {
      log(GatewayLogger.WARN,
          "Client is not connected. The received Control SHUT_DOWN message cannot be processed.");
      return;
    }

    //stop the reading thread of the BrokerInterface
    brokerInterface.setRunning(false);

    log(GatewayLogger.INFO, "Sending Mqtt DISCONNECT message to the broker.");
    try {
      brokerInterface.sendMsg(new MqttDisconnect());
    } catch (MqttsException ignored) {
    }
  }


  /**
   * The method that sets the Client interface in which this handler should respond in case of
   * sending a Mqtts message to the client.
   */
  public void setClientInterface(ClientInterface clientInterface) {
    this.clientInterface = clientInterface;
  }

  /**
   * This method sends a Mqtts DISCONNECT message to the client.
   */
  private void sendClientDisconnect() {
    log(GatewayLogger.INFO, "Sending Mqtts DISCONNECT message to the client");
    clientInterface.sendMsg(this.clientAddress, new MqttsDisconnect());

    clientState = ClientState.DISCONNECTED;
    timer.unregister(this.clientAddress);
    gateway.reset();

    // Reset mqtt states
    mqttsConnect = null;
    mqttsWillTopic = null;
    mqttsSubscribe = null;
    mqttsUnsubscribe = null;
    mqttsRegister = null;
    mqttsPublish = null;
    mqttPublish = null;

    //close the connection with the broker (if any)
    brokerInterface.disconnect();
  }

  /**
   * Method for doing a formatted log
   *
   * @param level The log level
   * @param content The content of the log message
   */
  private void log(int level, String content) {
    GatewayLogger.log(level, "ClientMsgHandler [" + Utils.hexString(this.clientAddress
        .getAddress()) + "]/[" + clientId + "] - " + content);
  }

}