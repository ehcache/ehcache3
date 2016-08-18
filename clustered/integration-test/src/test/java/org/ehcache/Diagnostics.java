/*
 * Copyright Terracotta, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.ehcache;

import com.sun.management.HotSpotDiagnosticMXBean;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.lang.management.LockInfo;
import java.lang.management.ManagementFactory;
import java.lang.management.MonitorInfo;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.Calendar;
import java.util.Date;

import javax.management.MBeanServer;

/**
 * Provides methods to produce diagnostic output.
 */
@SuppressWarnings({ "UnusedDeclaration", "WeakerAccess" })
public final class Diagnostics {

  private static final String HOTSPOT_DIAGNOSTIC_MXBEAN_NAME = "com.sun.management:type=HotSpotDiagnostic";
  private static final String HEAP_DUMP_FILENAME_TEMPLATE = "java_%1$04d_%2$tFT%2$tH%2$tM%2$tS.%2$tL.hprof";
  private static final File WORKING_DIRECTORY = new File(System.getProperty("user.dir"));

  /**
   * Private niladic constructor to prevent instantiation.
   */
  private Diagnostics() {
  }

  /**
   * Writes a complete thread dump to {@code System.err}.
   */
  public static void threadDump() {
    threadDump(System.err);
  }

  /**
   * Writes a complete thread dump to the designated {@code PrintStream}.
   *
   * @param out the {@code PrintStream} to which the thread dump is written
   */
  public static void threadDump(final PrintStream out) {
    if (out == null) {
      throw new NullPointerException("out");
    }

    final ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();

    final Calendar when = Calendar.getInstance();
    final ThreadInfo[] threadInfos = threadMXBean.dumpAllThreads(
        threadMXBean.isObjectMonitorUsageSupported(), threadMXBean.isSynchronizerUsageSupported());

    out.format("%nFull thread dump %1$tF %1$tT.%1$tL %1$tz%n", when);
    for (final ThreadInfo threadInfo : threadInfos) {
      out.print(format(threadInfo));
    }
  }

  /**
   * Format a {@code ThreadInfo} instance <i>without</i> a stack depth limitation.  This method reproduces the
   * formatting performed in {@code java.lang.management.ThreadInfo.toString()} without the stack depth limit.
   *
   * @param threadInfo the {@code ThreadInfo} instance to foramt
   *
   * @return a {@code CharSequence} instance containing the formatted {@code ThreadInfo}
   */
  private static CharSequence format(final ThreadInfo threadInfo) {
    StringBuilder sb = new StringBuilder(4096);

    Thread.State threadState = threadInfo.getThreadState();
    sb.append('"')
        .append(threadInfo.getThreadName())
        .append('"')
        .append(" Id=")
        .append(threadInfo.getThreadId())
        .append(' ')
        .append(threadState);

    if (threadInfo.getLockName() != null) {
      sb.append(" on ").append(threadInfo.getLockName());
    }
    if (threadInfo.getLockOwnerName() != null) {
      sb.append(" owned by ").append('"').append(threadInfo.getLockOwnerName()).append('"')
          .append(" Id=").append(threadInfo.getLockOwnerId());
    }

    if (threadInfo.isSuspended()) {
      sb.append(" (suspended)");
    }
    if (threadInfo.isInNative()) {
      sb.append(" (in native)");
    }
    sb.append('\n');

    StackTraceElement[] stackTrace = threadInfo.getStackTrace();
    for (int i = 0; i < stackTrace.length; i++) {
      StackTraceElement element = stackTrace[i];
      sb.append("\tat ").append(element);
      sb.append('\n');
      if (i == 0) {
        if (threadInfo.getLockInfo() != null) {
          switch (threadState) {
            case BLOCKED:
              sb.append("\t- blocked on ").append(threadInfo.getLockInfo());
              sb.append('\n');
              break;
            case WAITING:
              sb.append("\t- waiting on ").append(threadInfo.getLockInfo());
              sb.append('\n');
              break;
            case TIMED_WAITING:
              sb.append("\t- waiting on ").append(threadInfo.getLockInfo());
              sb.append('\n');
              break;
            default:
          }
        }
      }

      for (MonitorInfo monitorInfo : threadInfo.getLockedMonitors()) {
        if (monitorInfo.getLockedStackDepth() == i) {
          sb.append("\t- locked ").append(monitorInfo);
          sb.append('\n');
        }
      }
    }

    LockInfo[] lockedSynchronizers = threadInfo.getLockedSynchronizers();
    if (lockedSynchronizers.length > 0) {
      sb.append("\n\tNumber of locked synchronizers = ").append(lockedSynchronizers.length);
      sb.append('\n');
      for (LockInfo lockedSynchronizer : lockedSynchronizers) {
        sb.append("\t- ").append(lockedSynchronizer);
        sb.append('\n');
      }
    }

    sb.append('\n');
    return sb;
  }

  /**
   * Take a Java heap dump into a file whose name is produced from the template
   * <code>{@value #HEAP_DUMP_FILENAME_TEMPLATE}</code> where {@code 1$} is the PID of
   * the current process obtained from {@link #getPid()}.
   *
   * @param dumpLiveObjects if {@code true}, only "live" (reachable) objects are dumped;
   *                        if {@code false}, all objects in the heap are dumped
   *
   * @return the name of the dump file; the file is written to the current directory (generally {@code user.dir})
   */
  public static String dumpHeap(final boolean dumpLiveObjects) {

    String dumpName;
    final int pid = getPid();
    final Date currentTime = new Date();
    if (pid > 0) {
      dumpName = String.format(HEAP_DUMP_FILENAME_TEMPLATE, pid, currentTime);
    } else {
      dumpName = String.format(HEAP_DUMP_FILENAME_TEMPLATE, 0, currentTime);
    }

    dumpName = new File(WORKING_DIRECTORY, dumpName).getAbsolutePath();

    try {
      dumpHeap(dumpLiveObjects, dumpName);
    } catch (IOException e) {
      System.err.printf("Unable to write heap dump to %s: %s%n", dumpName, e);
      e.printStackTrace(System.err);
      return null;
    }

    return dumpName;
  }

  /**
   * Write a Java heap dump to the named file.  If the dump file exists, this method will
   * fail.
   *
   * @param dumpLiveObjects if {@code true}, only "live" (reachable) objects are dumped;
   *                        if {@code false}, all objects in the heap are dumped
   * @param dumpName the name of the file to which the heap dump is written; relative names
   *                 are relative to the current directory ({@code user.dir}).  If the value
   *                 of {@code dumpName} does not end in {@code .hprof}, it is appended.
   *
   * @throws IOException if thrown while loading the HotSpot Diagnostic MXBean or writing the heap dump
   *
   * @see <a href="http://docs.oracle.com/javase/8/docs/jre/api/management/extension/com/sun/management/HotSpotDiagnosticMXBean.html">
   *        com.sun.management.HotSpotDiagnosticMXBean</a>
   */
  public static void dumpHeap(final boolean dumpLiveObjects, String dumpName) throws IOException {
    if (dumpName == null) {
      throw new NullPointerException("dumpName");
    }

    if (!dumpName.endsWith(".hprof")) {
      dumpName += ".hprof";
    }

    final MBeanServer server = ManagementFactory.getPlatformMBeanServer();
    final HotSpotDiagnosticMXBean hotSpotDiagnosticMXBean =
        ManagementFactory.newPlatformMXBeanProxy(server, HOTSPOT_DIAGNOSTIC_MXBEAN_NAME, HotSpotDiagnosticMXBean.class);
    hotSpotDiagnosticMXBean.dumpHeap(dumpName, dumpLiveObjects);
  }

  /**
   * Gets the PID of the current process.  This method is dependent upon "common"
   * operation of the {@code java.lang.management.RuntimeMXBean#getName()} method.
   *
   * @return the PID of the current process or {@code -1} if the PID can not be determined
   */
  public static int getPid() {
    // Expected to be of the form "<pid>@<hostname>"
    final String jvmProcessName = ManagementFactory.getRuntimeMXBean().getName();
    try {
      return Integer.valueOf(jvmProcessName.substring(0, jvmProcessName.indexOf('@')));
    } catch (NumberFormatException e) {
      return -1;
    } catch (IndexOutOfBoundsException e) {
      return -1;
    }
  }
}