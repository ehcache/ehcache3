package org.ehcache.jsr107;

import com.sun.jmx.mbeanserver.JmxMBeanServer;

import javax.management.ListenerNotFoundException;
import javax.management.MBeanNotificationInfo;
import javax.management.MBeanServer;
import javax.management.MBeanServerBuilder;
import javax.management.MBeanServerDelegate;
import javax.management.Notification;
import javax.management.NotificationFilter;
import javax.management.NotificationListener;

/**
 * @author Alex Snaps
 */
public class Eh107MBeanServerBuilder extends MBeanServerBuilder {

  /**
   * Empty public constructor as required
   */
  public Eh107MBeanServerBuilder() {
    super();
  }

  @Override
  public MBeanServer newMBeanServer(String defaultDomain, MBeanServer outer, MBeanServerDelegate delegate) {
    MBeanServerDelegate decoratingDelegate = new RIMBeanServerDelegate(delegate);
    return JmxMBeanServer.newMBeanServer(defaultDomain, outer, decoratingDelegate, false);
  }

  /**
   * A decorator around the MBeanServerDelegate which sets the mBeanServerId to
   * the value of the <code>org.jsr107.tck.management.agentId</code> system
   * property so that the TCK can precisely identify the correct MBeanServer
   * when running tests.
   */
  public class RIMBeanServerDelegate extends MBeanServerDelegate {

    private final MBeanServerDelegate delegate;

    /**
     * Constructor
     * 
     * @param delegate
     *          the provided delegate
     */
    public RIMBeanServerDelegate(MBeanServerDelegate delegate) {
      this.delegate = delegate;
    }

    @Override
    public String getSpecificationName() {
      return delegate.getSpecificationName();
    }

    @Override
    public String getSpecificationVersion() {
      return delegate.getSpecificationVersion();
    }

    @Override
    public String getSpecificationVendor() {
      return delegate.getSpecificationVendor();
    }

    @Override
    public String getImplementationName() {
      return delegate.getImplementationName();
    }

    @Override
    public String getImplementationVersion() {
      return delegate.getImplementationVersion();
    }

    @Override
    public String getImplementationVendor() {
      return delegate.getImplementationVendor();
    }

    @Override
    public MBeanNotificationInfo[] getNotificationInfo() {
      return delegate.getNotificationInfo();
    }

    @Override
    public synchronized void addNotificationListener(NotificationListener listener, NotificationFilter filter,
        Object handback) throws IllegalArgumentException {
      delegate.addNotificationListener(listener, filter, handback);
    }

    @Override
    public synchronized void removeNotificationListener(NotificationListener listener, NotificationFilter filter,
        Object handback) throws ListenerNotFoundException {
      delegate.removeNotificationListener(listener, filter, handback);
    }

    @Override
    public synchronized void removeNotificationListener(NotificationListener listener) throws ListenerNotFoundException {
      delegate.removeNotificationListener(listener);
    }

    @Override
    public void sendNotification(Notification notification) {
      delegate.sendNotification(notification);
    }

    @Override
    public synchronized String getMBeanServerId() {
      return System.getProperty("org.jsr107.tck.management.agentId");
    }
  }

}
