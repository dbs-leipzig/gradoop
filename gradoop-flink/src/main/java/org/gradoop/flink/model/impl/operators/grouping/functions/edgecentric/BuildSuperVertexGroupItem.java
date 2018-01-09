/**
 * Copyright © 2014 - 2017 Leipzig University (Database Research Group)
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



