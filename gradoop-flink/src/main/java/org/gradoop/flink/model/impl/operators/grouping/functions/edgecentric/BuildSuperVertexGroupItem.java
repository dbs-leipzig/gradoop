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

package org.gradoop.flink.model.impl.operators.grouping.functions.edgecentric;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.impl.operators.grouping.tuples.edgecentric.SuperVertexGroupItem;

import java.util.Set;

/**
 * Assigns the same new gradoop id to all super vertex group items which represent the same group.
 * It is necessary to keep all these representative group items because they contain the
 * information on which edge they are part of.
 * */
@FunctionAnnotation.ForwardedFields(
  "f0;" +   //vertex ids
    "f2;" + //super edge id
    "f3;" + // vertex group label
    "f4;" + // vertex group property values
    "f5;" + // vertex label group
    "f6")
public class BuildSuperVertexGroupItem
  implements GroupReduceFunction<SuperVertexGroupItem, SuperVertexGroupItem> {

  @Override
  public void reduce(
    Iterable<SuperVertexGroupItem> superVertexGroupItems, Collector<SuperVertexGroupItem> collector)
    throws Exception {

    GradoopId superVertexId = null;
    Set<GradoopId> vertices;
    boolean isFirst = true;

    for (SuperVertexGroupItem superVertexGroupItem : superVertexGroupItems) {
      if (isFirst) {
        vertices = superVertexGroupItem.getVertexIds();
        // super vertex represents multiple or all vertices
        if (vertices.size() != 1) {
          superVertexId = GradoopId.get();
        // super vertex represents only one vertex
        } else {
          superVertexId = vertices.iterator().next();
        }
        isFirst = false;
      }
      superVertexGroupItem.setSuperVertexId(superVertexId);
      collector.collect(superVertexGroupItem);
    }
  }
}



