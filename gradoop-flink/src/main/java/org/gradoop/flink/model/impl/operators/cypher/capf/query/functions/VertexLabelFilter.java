/*
 * Copyright © 2014 - 2019 Leipzig University (Database Research Group)
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
package org.gradoop.flink.model.impl.operators.cypher.capf.query.functions;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.pojo.EPGMVertex;

/**
 * Function filters tuples by the label of the {@link EPGMVertex} in the second field.
 */
@FunctionAnnotation.ReadFields("f1")
public class VertexLabelFilter implements FilterFunction<Tuple2<Long, EPGMVertex>> {

  /**
   * The label to be filtered for.
   */
  private String label;

  /**
   * Constructor.
   *
   * @param label label to be filtered for
   */
  public VertexLabelFilter(String label) {
    this.label = label;
  }

  @Override
  public boolean filter(Tuple2<Long, EPGMVertex> tuple) throws Exception {
    return tuple.f1.getLabel().equals(label);
  }
}
