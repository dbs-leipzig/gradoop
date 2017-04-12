/*
 * This file is part of Gradoop.
 *
 * Gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Gradoop. If not, see <http://www.gnu.org/licenses/>.
 */
package org.gradoop.benchmark.nesting.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple6;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdList;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.EdgeFactory;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.Properties;

/**
 * Instantiates the new edge from the partially reconstructed information
 *
 * @param <K> Id parameter
 */
@FunctionAnnotation.ForwardedFieldsFirst
  ("f1 -> sourceId; f3 -> label; f4 -> properties; f5 -> graphIds")
@FunctionAnnotation.ForwardedFieldsSecond("f1.id -> targetId")
public class CreateEdge<K extends Comparable<K>>
  implements  JoinFunction<Tuple6<K, GradoopId, K, String, Properties, GradoopIdList>,
                           Tuple2<K, Vertex>,
                            Edge> {

  /**
   * Factory generating new edges
   */
  private final EdgeFactory factory;

  /**
   * Default constructor
   * @param factory Edge generator
   */
  public CreateEdge(EdgeFactory factory) {
    this.factory = factory;
  }

  @Override
  public Edge join(Tuple6<K, GradoopId, K, String, Properties, GradoopIdList> first,
    Tuple2<K, Vertex> second) throws Exception {
    return factory.createEdge(first.f3, first.f1, second.f1.getId(), first.f4, first.f5);
  }
}
