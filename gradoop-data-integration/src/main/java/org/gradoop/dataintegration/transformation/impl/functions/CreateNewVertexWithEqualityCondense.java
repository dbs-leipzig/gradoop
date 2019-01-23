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
package org.gradoop.dataintegration.transformation.impl.functions;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.pojo.VertexFactory;
import org.gradoop.common.model.impl.properties.PropertyValue;

import java.util.ArrayList;
import java.util.List;

/**
 * A {@link GroupReduceFunction} that creates one new vertex and adds all origin Ids to a {@link List}
 * for later use. This function reduces the amount of equal property values significantly.
 */
public class CreateNewVertexWithEqualityCondense implements GroupReduceFunction<Tuple2<PropertyValue, GradoopId>, Tuple2<Vertex, List<GradoopId>>> {

  /**
   * The new vertex label.
   */
  private final String newVertexLabel;

  /**
   * The new property key.
   */
  private final String newPropertyName;

  /**
   * The factory new vertices are created with.
   */
  private final VertexFactory vertexFactory;

  /**
    * Reduce object instantiation.
    */
  private final Tuple2<Vertex, List<GradoopId>> reuseTuple;

  /**
   * The constructor for condensation of same property values to one newly created vertex.
   *
   * @param factory The factory new vertices are created with.
   * @param newVertexLabel  The new vertex label.
   * @param newPropertyName The new property key.
   */
  public CreateNewVertexWithEqualityCondense(VertexFactory factory, String newVertexLabel,
                                             String newPropertyName) {
    this.vertexFactory = factory;
    this.newVertexLabel = newVertexLabel;
    this.newPropertyName = newPropertyName;

    this.reuseTuple = new Tuple2<>();
  }

  @Override
  public void reduce(Iterable<Tuple2<PropertyValue, GradoopId>> values,
                     Collector<Tuple2<Vertex, List<GradoopId>>> out) {
    List<GradoopId> sources = new ArrayList<>();
    PropertyValue pv = null;

    for (Tuple2<PropertyValue, GradoopId> tuple : values) {
      sources.add(tuple.f1);

      if (pv == null) {
        pv = tuple.f0;
      }
    }

    Vertex vertex = vertexFactory.createVertex(newVertexLabel);
    vertex.setProperty(newPropertyName, pv);

    reuseTuple.f0 = vertex;
    reuseTuple.f1 = sources;

    out.collect(reuseTuple);
  }
}
