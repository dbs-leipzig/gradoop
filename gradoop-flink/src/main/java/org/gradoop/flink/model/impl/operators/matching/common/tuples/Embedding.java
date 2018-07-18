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
package org.gradoop.flink.model.impl.operators.matching.common.tuples;

import org.apache.flink.api.java.tuple.Tuple2;

/**
 * Represents an embedding of a query pattern in the search graph. Vertex and
 * edge mappings are represented by two arrays where the index in the array
 * refers to the index of the query vertex/edge.
 *
 * f0: vertex mapping
 * f1: edge mapping
 *
 * @param <K> key type
 */
public class Embedding<K> extends Tuple2<K[], K[]> {

  public K[] getVertexMapping() {
    return f0;
  }

  public void setVertexMapping(K[] vertexMappings) {
    f0 = vertexMappings;
  }

  public K[] getEdgeMapping() {
    return f1;
  }

  public void setEdgeMapping(K[] edgeMappings) {
    f1 = edgeMappings;
  }
}
