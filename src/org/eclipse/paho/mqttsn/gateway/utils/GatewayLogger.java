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

package org.eclipse.paho.mqttsn.gateway.utils;


import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class GatewayLogger {

  public final static int INFO = Level.INFO.toInt();
  public final static int WARN = Level.WARN.toInt();
  public final static int ERROR = Level.ERROR.toInt();

  private static final Logger logger = Logger.getLogger(GatewayLogger.class);


  public static void info(String msg) {
    logger.info(msg);
  }

  public static void warn(String msg) {
    logger.warn(msg);
  }

  public static void error(String msg) {
    logger.error(msg);
  }

  public static void log(int logLevel, String msg) {
    logger.log(Level.toLevel(logLevel), msg);
  }

}