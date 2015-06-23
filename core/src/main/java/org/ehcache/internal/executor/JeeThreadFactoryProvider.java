package org.ehcache.internal.executor;

import java.util.concurrent.ThreadFactory;

import javax.naming.InitialContext;
import javax.naming.NamingException;

/**
 * This class is for demonstration only , showing how {@link ThreadFactory} can be source from JEE environment.
 * @author palmanojkumar
 *
 */
public class JeeThreadFactoryProvider implements ThreadFactoryProvider {

  @Override
  public ThreadFactory newThreadFactory() {
    
    InitialContext context;

    try {
      context = new InitialContext();
      return  (ThreadFactory) context.lookup("java:comp/DefaultManagedThreadFactory");
    } catch (NamingException e) {
      e.printStackTrace();
    } 
        
    return null;
  }

}
