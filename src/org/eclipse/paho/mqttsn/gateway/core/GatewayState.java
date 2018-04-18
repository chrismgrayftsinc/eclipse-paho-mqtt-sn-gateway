package org.eclipse.paho.mqttsn.gateway.core;

/**
 * The class that represents the state of the gateway at any given time.
 *
 */
class GatewayState {

  //waiting message from the client
  private boolean waitingWillTopic;
  private boolean waitingWillMsg;
  private boolean waitingRegack;

  //waiting message from the broker
  private boolean waitingSuback;
  private boolean waitingUnsuback;
  private boolean waitingPuback;

  //counters
  private int triesSendingWillTopicReq;
  private int triesSendingWillMsgReq;
  private int triesSendingRegister;

  public GatewayState(){
    this.waitingWillTopic = false;
    this.waitingWillMsg = false;
    this.waitingRegack = false;

    this.waitingSuback = false;
    this.waitingUnsuback = false;
    this.waitingPuback = false;

    this.triesSendingWillTopicReq = 0;
    this.triesSendingWillMsgReq = 0;
    this.triesSendingRegister = 0;
  }

  public void reset(){
    this.waitingWillTopic = false;
    this.waitingWillMsg = false;
    this.waitingRegack = false;

    this.waitingSuback = false;
    this.waitingUnsuback = false;
    this.waitingPuback = false;

    this.triesSendingWillTopicReq = 0;
    this.triesSendingWillMsgReq = 0;
    this.triesSendingRegister = 0;

    //delete also all stored messages (if any)

  }

  public boolean isEstablishingConnection() {
    return (isWaitingWillTopic() || isWaitingWillMsg());
  }

  public boolean isWaitingWillTopic() {
    return this.waitingWillTopic;
  }

  public void setWaitingWillTopic() {
    this.waitingWillTopic = true;
  }

  public void resetWaitingWillTopic() {
    this.waitingWillTopic = false;
  }

  public boolean isWaitingWillMsg() {
    return this.waitingWillMsg;
  }

  public void setWaitingWillMsg() {
    this.waitingWillMsg = true;
  }

  public void resetWaitingWillMsg() {
    this.waitingWillMsg = false;
  }

  public boolean isWaitingRegack() {
    return this.waitingRegack;
  }

  public void setWaitingRegack() {
    this.waitingRegack = true;
  }

  public void resetWaitingRegack() {
    this.waitingRegack = false;
  }

  public boolean isWaitingSuback() {
    return this.waitingSuback;
  }

  public void setWaitingSuback() {
    this.waitingSuback = true;
  }

  public void resetWaitingSuback() {
    this.waitingSuback = false;
  }

  public boolean isWaitingUnsuback() {
    return this.waitingUnsuback;
  }

  public void setWaitingUnsuback() {
    this.waitingUnsuback = true;
  }

  public void resetWaitingUnsuback() {
    this.waitingUnsuback = false;
  }

  public boolean isWaitingPuback() {
    return this.waitingPuback;
  }

  public void setWaitingPuback() {
    this.waitingPuback = true;
  }

  public void resetWaitingPuback() {
    this.waitingPuback = false;
  }

  public int getTriesSendingWillTopicReq() {
    return this.triesSendingWillTopicReq;
  }

  public void increaseTriesSendingWillTopicReq() {
    this.triesSendingWillTopicReq ++;
  }

  public void resetTriesSendingWillTopicReq() {
    this.triesSendingWillTopicReq = 0;
  }

  public int getTriesSendingWillMsgReq() {
    return this.triesSendingWillMsgReq;
  }

  public void increaseTriesSendingWillMsgReq() {
    this.triesSendingWillMsgReq ++;
  }

  public void resetTriesSendingWillMsgReq() {
    this.triesSendingWillMsgReq = 0;
  }

  public int getTriesSendingRegister() {
    return this.triesSendingRegister;
  }

  public void increaseTriesSendingRegister() {
    this.triesSendingRegister ++;
  }

  public void resetTriesSendingRegister() {
    this.triesSendingRegister = 0;
  }
}
