/*
 * Copyright © 2014 - 2020 Leipzig University (Database Research Group)
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
package org.gradoop.temporal.model.impl.pojo;

import org.gradoop.common.model.api.entities.GraphHead;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.Properties;

/**
 * POJO Implementation of a TPGM graph head.
 */
public class TemporalGraphHead extends TemporalElement implements GraphHead {

  /**
   * Default constructor to create an empty temporal graph head instance.
   */
  public TemporalGraphHead() {
    super();
  }

  /**
   * Creates a temporal graph head instance with a time interval that implies its validity.
   *
   * @param id the Gradoop identifier
   * @param label the label
   * @param properties the graph head properties
   * @param validFrom the start of the graph head validity as timestamp represented as long
   * @param validTo the end of the graph head validity as timestamp represented as long
   */
  public TemporalGraphHead(GradoopId id, String label, Properties properties, Long validFrom,
    Long validTo) {
    super(id, label, properties, validFrom, validTo);
  }
}
