/*
 * Copyright (c) 2013, Cloudera, Inc. All Rights Reserved.
 *
 * Cloudera, Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"). You may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for
 * the specific language governing permissions and limitations under the
 * License.
 */

package com.cloudera.oryx.common.log;

import java.util.LinkedList;
import java.util.Queue;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.SimpleFormatter;

/**
 * Simple {@link Handler} that records recent log lines in memory.
 * 
 * @author Sean Owen
 */
public final class MemoryHandler extends Handler {

  private static final int NUM_LINES = 1000;
  private static final String JAVA7_LOG_FORMAT_PROP = "java.util.logging.SimpleFormatter.format";

  private final Queue<String> logLines;

  public MemoryHandler() {
    logLines = new LinkedList<>();
    setFormatter(new SimpleFormatter());
    setLevel(Level.FINE);
  }

  /**
   * @return recent log lines
   */
  public Iterable<String> getLogLines() {
    return logLines;
  }

  @Override
  public void publish(LogRecord logRecord) {
    String line = getFormatter().format(logRecord);
    synchronized (logLines) {
      logLines.add(line);
      for (int i = 0; i < logLines.size() - NUM_LINES; i++) {
        logLines.remove();
      }
    }
  }

  @Override
  public void flush() {
  }

  @Override
  public void close() {
    logLines.clear();
  }

  /**
   * <p>Sets the {@code java.util.logging} default output format to something more sensible than the 2-line default.
   * This can be overridden further on the command line. The format is like:</p>
   *
   * {@code Mon Nov 26 23:16:09 GMT 2012 INFO Starting service Tomcat}
   */
  public static void setSensibleLogFormat() {
    if (System.getProperty(JAVA7_LOG_FORMAT_PROP) == null) {
      System.setProperty(JAVA7_LOG_FORMAT_PROP, "%1$tc %4$s %5$s%6$s%n");
    }
    // Note that this above is a Java system property, not our app config
  }

}
