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

import java.util.UUID;

/**
 * @author palmanojkumar
 *
 */
public class RequestContext {

  private String contextId;
  private TaskPriority priority = TaskPriority.NORMAL;
  
  public RequestContext() {
    this(UUID.randomUUID().toString(), TaskPriority.NORMAL);
  }

  public RequestContext(TaskPriority tPriority) {
    this(UUID.randomUUID().toString(), tPriority);
  }
  
  public RequestContext(String contextId, TaskPriority priority) {
    this.contextId = contextId;
    this.priority = priority;
  }
  
  public String getContextId() {
    return contextId;
  }

  public TaskPriority getTaskPriority() {
    return priority;
  }

  static enum TaskPriority {
    HIGH, NORMAL;
  }
}
