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

package org.ehcache.core;


import org.ehcache.Status;

/**
 * @author Alex Snaps
 */
enum InternalStatus {

  UNINITIALIZED {
    @Override
    public Transition init() {
      return new Transition(AVAILABLE);
    }

    @Override
    public Transition maintenance() {
      return new Transition(MAINTENANCE);
    }

    @Override
    public Status toPublicStatus() {
      return Status.UNINITIALIZED;
    }
  },

  MAINTENANCE {
    @Override
    public Transition close() {
      return new Transition(UNINITIALIZED);
    }

    @Override
    public Status toPublicStatus() {
      return Status.MAINTENANCE;
    }
  },

  AVAILABLE {
    @Override
    public Transition close() {
      return new Transition(UNINITIALIZED);
    }

    @Override
    public Status toPublicStatus() {
      return Status.AVAILABLE;
    }
  },
  ;

  public Transition init() {
    throw new IllegalStateException("Init not supported from " + name());
  }

  public Transition close() {
    throw new IllegalStateException("Close not supported from " + name());
  }

  public Transition maintenance() {
    throw new IllegalStateException("Maintenance not supported from " + name());
  }

  public abstract Status toPublicStatus();

  public class Transition {

    private final InternalStatus to;
    private final Thread owner = Thread.currentThread();

    private volatile InternalStatus done;

    private Transition(final InternalStatus to) {
      if(to == null) {
        throw new NullPointerException();
      }
      this.to = to;
    }

    public InternalStatus get() {
      if(done != null) {
        return done;
      } else if(owner == Thread.currentThread()) {
        return to.compareTo(from()) > 0 ? to : from();
      }
      synchronized (this) {
        boolean interrupted = false;
        try {
          while(done == null) {
            try {
              wait();
            } catch (InterruptedException e) {
              interrupted = true;
            }
          }
        } finally {
          if(interrupted) {
            Thread.currentThread().interrupt();
          }
        }
        return done;
      }
    }

    public synchronized void succeeded() {
      done = to;
      notifyAll();
    }

    public synchronized void failed() {
      done = to.compareTo(from()) > 0 ? from() : to;
      notifyAll();
    }

    public InternalStatus from() {
      return InternalStatus.this;
    }

    public InternalStatus to() {
      return to;
    }

    public boolean done() {
      return done != null;
    }
  }

  public static Transition initial() {
    final Transition close = MAINTENANCE.close();
    close.succeeded();
    return close;
  }

}
