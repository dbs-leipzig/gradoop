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

import org.apache.flink.api.common.functions.GroupCombineFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdList;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.pojo.VertexFactory;
import org.gradoop.flink.io.impl.graph.tuples.ImportVertex;

/**
 * Groups all the vertices with the same id (and hence, describing the same entity) from all
 * the graph informations to a single vertex
 *
 * @param <K>  user-defined id representation
 */
public class CreateVertex<K extends Comparable<K>>
  implements GroupCombineFunction<Tuple2<ImportVertex<K>, GradoopId>, Tuple2<K, Vertex>> {

  /**
   * Generating new vertices
   */
  private final VertexFactory factory;

  /**
   * reusable element associating newly generated vertices to old K ids
   */
  private final Tuple2<K, Vertex> reusable;

  /**
   * Default constructor
   * @param factory Factory generating the vertices
   */
  public CreateVertex(VertexFactory factory) {
    this.factory = factory;
    reusable = new Tuple2<>();
  }

  @Override
  public void combine(Iterable<Tuple2<ImportVertex<K>, GradoopId>> values,
    Collector<Tuple2<K, Vertex>> out) throws Exception {
    Vertex toReturn = null;
    K key = null;
    for (Tuple2<ImportVertex<K>, GradoopId> x : values) {
      if (toReturn == null) {
        key = x.f0.getId();
        toReturn = factory.createVertex();
        toReturn.setProperties(x.f0.getProperties());
        toReturn.setLabel(x.f0.getLabel());
        toReturn.setGraphIds(new GradoopIdList());
      }
      if (! toReturn.getGraphIds().contains(x.f1)) {
        toReturn.addGraphId(x.f1);
      }
    }
    if (toReturn != null) {
      reusable.f1 = toReturn;
      reusable.f0 = key;
      out.collect(reusable);
    }
  }


}
