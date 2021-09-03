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
package org.ehcache.clustered.util;

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.terracotta.connection.Connection;
import org.terracotta.connection.ConnectionException;
import org.terracotta.passthrough.IClusterControl;
import org.terracotta.testing.rules.Cluster;

import java.net.URI;
import java.util.concurrent.Phaser;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

public class ParallelTestCluster implements TestRule {

  private final Cluster cluster;
  private final IClusterControl control;
  private final AtomicReference<ClusterTask> nextTask = new AtomicReference<>();

  private final Phaser membership = new Phaser() {
    @Override
    protected boolean onAdvance(int phase, int registeredParties) {
      activeCycle.bulkRegister(registeredParties);
      return false;
    }
  };
  private final Phaser activeCycle = new Phaser() {
    @Override
    protected boolean onAdvance(int phase, int registeredParties) {
      return false;
    }
  };

  public ParallelTestCluster(Cluster cluster) {
    this.cluster = cluster;

    IClusterControl underlyingControl = cluster.getClusterControl();
    this.control = new IClusterControl() {
      @Override
      public void waitForActive() throws Exception {
        underlyingControl.waitForActive();
      }

      @Override
      public void waitForRunningPassivesInStandby() throws Exception {
        underlyingControl.waitForRunningPassivesInStandby();
      }

      @Override
      public void startOneServer() {
        request(ClusterTask.START_ONE_SERVER);
      }

      @Override
      public void startAllServers() {
        request(ClusterTask.START_ALL_SERVERS);
      }

      @Override
      public void terminateActive() {
        request(ClusterTask.TERMINATE_ACTIVE);
      }

      @Override
      public void terminateOnePassive() {
        request(ClusterTask.TERMINATE_ONE_PASSIVE);
      }

      @Override
      public void terminateAllServers() {
        request(ClusterTask.TERMINATE_ALL_SERVERS);
      }

      private void request(ClusterTask task) {
        try {
          if (nextTask.compareAndSet(null, task)) {
            activeCycle.awaitAdvanceInterruptibly(activeCycle.arrive());
            nextTask.getAndSet(null).accept(underlyingControl);
            activeCycle.awaitAdvanceInterruptibly(activeCycle.arrive());
          } else {
            ClusterTask requestedTask = nextTask.get();
            if (requestedTask.equals(task)) {
              activeCycle.awaitAdvanceInterruptibly(activeCycle.arrive());
              activeCycle.awaitAdvanceInterruptibly(activeCycle.arrive());
            } else {
              throw new AssertionError("Existing requested task is " + requestedTask);
            }
          }
        } catch (InterruptedException e) {
          throw new AssertionError(e);
        }
      }
    };
  }

  public URI getConnectionURI() {
    return cluster.getConnectionURI();
  }

  public String[] getClusterHostPorts() {
    return cluster.getClusterHostPorts();
  }

  public Connection newConnection() throws ConnectionException {
    return cluster.newConnection();
  }

  public IClusterControl getClusterControl() {
    return control;
  }

  @Override
  public Statement apply(Statement base, Description description) {
    if (description.isSuite()) {
      return cluster.apply(base, description);
    } else if (description.isTest()) {
      return new Statement() {
        @Override
        public void evaluate() throws Throwable {
          membership.register();
          Thread.sleep(100);
          membership.awaitAdvanceInterruptibly(membership.arrive());
          try {
            activeCycle.awaitAdvanceInterruptibly(activeCycle.arrive());
            try {
              base.evaluate();
            } finally {
              activeCycle.arriveAndDeregister();
            }
          } finally {
            membership.arriveAndDeregister();
          }
        }
      };
    } else {
      return base;
    }
  }

  enum ClusterTask implements Consumer<IClusterControl> {
    START_ONE_SERVER(IClusterControl::startOneServer),
    START_ALL_SERVERS(IClusterControl::startAllServers),
    TERMINATE_ACTIVE(IClusterControl::terminateActive),
    TERMINATE_ONE_PASSIVE(IClusterControl::terminateOnePassive),
    TERMINATE_ALL_SERVERS(IClusterControl::terminateAllServers);

    private final Task task;

    ClusterTask(Task task) {
      this.task = task;
    }

    public void accept(IClusterControl control) {
      try {
        task.run(control);
      } catch (Exception e) {
        throw new AssertionError(e);
      }
    }

    interface Task {
      void run(IClusterControl control) throws Exception;
    }
  }
}
