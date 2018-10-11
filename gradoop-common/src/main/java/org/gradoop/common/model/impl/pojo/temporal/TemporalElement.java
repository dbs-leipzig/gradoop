/*
 * Copyright © 2014 - 2018 Leipzig University (Database Research Group)
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
package org.gradoop.common.model.impl.pojo.temporal;

import org.gradoop.common.model.api.entities.EPGMElement;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Element;
import org.gradoop.common.model.impl.properties.Properties;

/**
 * TODO: descriptions
 */
public abstract class TemporalElement extends Element implements EPGMElement {

  protected long txFrom;
  protected long txTo;
  protected long validFrom;
  protected long validTo;

  /**
   * Default constructor.
   */
  protected TemporalElement() {
  }

  protected TemporalElement(GradoopId id, String label, Properties properties) {
    super(id, label, properties);

    // Set transaction time beginning to the current system time
    this.txFrom = System.currentTimeMillis();
    this.txTo = Long.MAX_VALUE;
  }

  protected TemporalElement(GradoopId id, String label, Properties properties, long validFrom) {
    this(id, label, properties);
    this.validFrom = validFrom;
  }

  protected TemporalElement(GradoopId id, String label, Properties properties, long validFrom,
    long validTo) {
    this(id, label, properties, validFrom);
    this.validTo = validTo;
  }

  public long getValidFrom() {
    return this.validFrom;
  }

  public void setValidFrom(long validFrom) {
    this.validFrom = validFrom;
  }

  public long getValidTo() {
    return this.validTo;
  }

  public void setValidTo(long validTo) {
    this.validTo = validTo;
  }

  public long getTxFrom() {
    return this.txFrom;
  }

  public long getTxTo() {
    return this.txTo;
  }
}
