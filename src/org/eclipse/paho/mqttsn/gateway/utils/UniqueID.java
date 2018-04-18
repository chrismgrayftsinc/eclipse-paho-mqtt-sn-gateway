package org.eclipse.paho.mqttsn.gateway.utils;

public class UniqueID {

  private int ID;

  public UniqueID(int startValue) {
    ID = startValue;
  }

  public int getUniqueID() {
    return ID++;
  }

}
