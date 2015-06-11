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
package org.ehcache.internal.executor;

/**
 * @author palmanojkumar
 *
 */
public class ExecutionContext {

  private String executionContextId;
  private TaskPriority priority = TaskPriority.NORMAL;
  
  public ExecutionContext() {
    
  }
  
  public ExecutionContext(String executionContextId, TaskPriority priority) {
    this.executionContextId = executionContextId;
    this.priority = priority;
  }
  
  public String getExecutionContextId() {
    return executionContextId;
  }

  public void setExecutionContextId(String executionContextId) {
    this.executionContextId = executionContextId;
  }

  public TaskPriority getPriority() {
    return priority;
  }

  public void setPriority(TaskPriority priority) {
    this.priority = priority;
  }




  static enum TaskPriority {
    LEASED, HIGH, NORMAL;
  }
}
