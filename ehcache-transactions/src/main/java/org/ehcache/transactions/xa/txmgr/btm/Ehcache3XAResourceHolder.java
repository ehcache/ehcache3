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
package org.ehcache.transactions.xa.txmgr.btm;

import bitronix.tm.resource.common.AbstractXAResourceHolder;
import bitronix.tm.resource.common.ResourceBean;
import bitronix.tm.resource.common.XAResourceHolder;

import javax.transaction.xa.XAResource;
import java.util.Collections;
import java.util.Date;
import java.util.List;

/**
 * @author Ludovic Orban
 */
class Ehcache3XAResourceHolder extends AbstractXAResourceHolder {

  private final XAResource resource;
  private final ResourceBean bean;

  /**
   * Create a new EhCacheXAResourceHolder for a particular XAResource
   * @param resource the required XAResource
   * @param bean the required ResourceBean
   */
  Ehcache3XAResourceHolder(XAResource resource, ResourceBean bean) {
    this.resource = resource;
    this.bean = bean;
  }

  /**
   * {@inheritDoc}
   */
  public XAResource getXAResource() {
    return resource;
  }

  /**
   * {@inheritDoc}
   */
  public ResourceBean getResourceBean() {
    return bean;
  }

  /**
   * {@inheritDoc}
   */
  public void close() {
    throw new UnsupportedOperationException("Ehcache3XAResourceHolder cannot be used with an XAPool");
  }

  /**
   * {@inheritDoc}
   */
  public Object getConnectionHandle() {
    throw new UnsupportedOperationException("Ehcache3XAResourceHolder cannot be used with an XAPool");
  }

  /**
   * {@inheritDoc}
   */
  public Date getLastReleaseDate() {
    throw new UnsupportedOperationException("Ehcache3XAResourceHolder cannot be used with an XAPool");
  }

  /**
   * {@inheritDoc}
   */
  public List<XAResourceHolder> getXAResourceHolders() {
    return Collections.singletonList(this);
  }

}
