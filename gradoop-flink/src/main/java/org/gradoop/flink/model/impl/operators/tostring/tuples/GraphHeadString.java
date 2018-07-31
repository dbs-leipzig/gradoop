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
package org.gradoop.flink.model.impl.operators.tostring.tuples;

import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.api.entities.EPGMLabeled;
import org.gradoop.common.model.impl.id.GradoopId;

/**
 * (graphId, label)
 */
public class GraphHeadString extends Tuple2<GradoopId, String>
  implements EPGMLabeled {

  /**
   * default constructor
   */
  public GraphHeadString() {
  }

  /**
   * constructor with field values
   * @param id graph id
   * @param label graph head label
   */
  public GraphHeadString(GradoopId id, String label) {
    this.f0 = id;
    this.f1 = label;
  }

  @Override
  public String getLabel() {
    return this.f1;
  }

  @Override
  public void setLabel(String label) {
    this.f1 = label;
  }

  public GradoopId getId() {
    return this.f0;
  }
}
