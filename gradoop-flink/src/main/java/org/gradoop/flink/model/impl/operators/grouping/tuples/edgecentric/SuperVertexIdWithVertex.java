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
package org.gradoop.flink.model.impl.operators.grouping.tuples.edgecentric;

import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Vertex;

/**
 * Assigns each super vertex id to all vertices the super vertex represents. This tuple is also
 * comparable based on the labels of the vertex.
 */
public class SuperVertexIdWithVertex
  extends Tuple2<GradoopId, Vertex>
  implements Comparable<SuperVertexIdWithVertex> {

  public void setSuperVertexid(GradoopId gradoopId) {
    f0 = gradoopId;
  }

  public GradoopId getSuperVertexid() {
    return f0;
  }

  public void setVertex(Vertex vertex) {
    f1 = vertex;
  }

  public Vertex getVertex() {
    return f1;
  }

  @Override
  public int compareTo(SuperVertexIdWithVertex o) {
    return getVertex().getLabel().compareTo(o.getVertex().getLabel());
  }
}
