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

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.EPGMEdge;
import org.gradoop.common.model.impl.properties.Properties;

/**
 * MapFunction that maps tuples containing a Long id and an edge to an edge tuple.
 */

@FunctionAnnotation.ForwardedFields(
  "f0;" +
    "f1.sourceId->f1;" +
    "f1.targetId->f2;" +
    "f1.label->f3;" +
    "f1.properties->f4")
public class EdgeToTuple
  implements MapFunction<Tuple2<Long, EPGMEdge>,
  Tuple5<Long, GradoopId, GradoopId, String, Properties>> {

  /**
   * Reduce object instantiations
   */
  private Tuple5<Long, GradoopId, GradoopId, String, Properties> returnTuple = new Tuple5<>();

  @Override
  public Tuple5<Long, GradoopId, GradoopId, String, Properties> map(
    Tuple2<Long, EPGMEdge> tuple) throws Exception {

    EPGMEdge edge = tuple.f1;

    returnTuple.f0 = tuple.f0;
    returnTuple.f1 = edge.getSourceId();
    returnTuple.f2 = edge.getTargetId();
    returnTuple.f3 = edge.getLabel();
    returnTuple.f4 = edge.getProperties();

    return returnTuple;
  }
}
