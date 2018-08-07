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
package org.gradoop.flink.model.impl.operators.split.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.api.functions.Function;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.PropertyValue;

import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Maps the vertices to pairs, where each pair contains the vertex id and one
 * split value. The split values are determined using a user defined function.
 *
 * @param <V> EPGM vertex type
 */
@FunctionAnnotation.ForwardedFields("id->f0")
@FunctionAnnotation.ReadFields("properties")
public class SplitValues<V extends Vertex>
  implements FlatMapFunction<V, Tuple2<GradoopId, PropertyValue>> {
  /**
   * Self defined Function
   */
  private Function<V, List<PropertyValue>> function;

  /**
   * Constructor
   *
   * @param function user-defined function to determine split values
   */
  public SplitValues(Function<V, List<PropertyValue>> function) {
    this.function = checkNotNull(function);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void flatMap(V vertex,
    Collector<Tuple2<GradoopId, PropertyValue>> collector) throws Exception {
    List<PropertyValue> splitValues = function.apply(vertex);
    for (PropertyValue value : splitValues) {
      collector.collect(new Tuple2<>(vertex.getId(), value));
    }
  }
}

