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
package org.ehcache.clustered.common.messages;

import java.io.Serializable;

import org.terracotta.entity.EntityResponse;

/**
 *
 * @author cdennis
 */
public abstract class EhcacheEntityResponse implements EntityResponse, Serializable {
  private static final long serialVersionUID = 4559645892362408528L;

  public enum Type {
    SUCCESS,
    FAILURE;
  }

  public abstract Type getType();

  public static Success success() {
    return Success.INSTANCE;
  }

  public static class Success extends EhcacheEntityResponse {
    private static final long serialVersionUID = -9061756298941151437L;

    private static final Success INSTANCE = new Success();

    private Success() {
      //singleton
    }

    @Override
    public Type getType() {
      return Type.SUCCESS;
    }
  }

  public static Failure failure(Exception cause) {
    return new Failure(cause);
  }

  public static class Failure extends EhcacheEntityResponse {
    private static final long serialVersionUID = -2120407256454458315L;

    private final Exception cause;

    private Failure(Exception cause) {
      this.cause = cause;
    }

    @Override
    public Type getType() {
      return Type.FAILURE;
    }

    public Exception getCause() {
      return cause;
    }
  }

}
