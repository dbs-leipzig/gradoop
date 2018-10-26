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
package org.gradoop.flink.model.impl.operators.statistics.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.impl.tuples.WithCount;

/**
 * Join function for adding degrees to vertices
 */
public class AddDegreeJoinFunction implements JoinFunction<Vertex, WithCount<GradoopId>, Vertex> {

  /**
   * name property key for degrestorese
   */
  private final String degreeKey;

  /**
   * Constructor
   * @param degreeKey name property key for degree
   */
  public AddDegreeJoinFunction(String degreeKey) {
    this.degreeKey = degreeKey;
  }

  @Override
  public Vertex join(Vertex vertex, WithCount<GradoopId> gradoopIdWithCount) throws Exception {

    vertex.setProperty(degreeKey, gradoopIdWithCount.f1);

    return vertex;
  }
}
