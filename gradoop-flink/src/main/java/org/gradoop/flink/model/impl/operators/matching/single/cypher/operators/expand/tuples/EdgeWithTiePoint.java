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
package org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.expand.tuples;

import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.Embedding;

/**
 * Represents an Edge with an extracted tie point
 *
 * f0 -> edge join key
 * f1 -> edge id
 * f2 -> edge expand key
 */
public class EdgeWithTiePoint extends Tuple3<GradoopId, GradoopId, GradoopId> {

  /**
   * Creates an empty Object
   */
  public EdgeWithTiePoint() {
  }

  /**
   * Creates a new Edge with extracted tie point from the given edge
   * @param edge edge embedding
   */
  public EdgeWithTiePoint(Embedding edge) {
    this.f0 = edge.getId(0);
    this.f1 = edge.getId(1);
    this.f2 = edge.getId(2);
  }

  /**
   * Set source id
   * @param id source id
   */
  public void setSource(GradoopId id) {
    f0 = id;
  }

  /**
   * Get source id
   * @return source id
   */
  public GradoopId getSource() {
    return f0;
  }

  /**
   * Set edge id
   * @param id edge id
   */
  public void setId(GradoopId id) {
    f1 = id;
  }

  /**
   * Get edge id
   * @return edge id
   */
  public GradoopId getId() {
    return f1;
  }

  /**
   * Set target id
   * @param id target id
   */
  public void setTarget(GradoopId id) {
    f2 = id;
  }

  /**
   * Get target id
   * @return target id
   */
  public GradoopId getTarget() {
    return f2;
  }
}
