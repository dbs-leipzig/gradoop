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

package org.gradoop.flink.model.impl.operators.matching.common.tuples;

import org.apache.flink.api.java.tuple.Tuple2;

/**
 * Represents an embedding of a query pattern in the search graph. Vertex and
 * edge embeddings are represented by two arrays where the index in the array
 * refers to the index of the query vertex/edge.
 *
 * f0: vertex mappings
 * f1: edge mappings
 *
 * @param <K> key type
 */
public class Embedding<K> extends Tuple2<K[], K[]> {

  public K[] getVertexMappings() {
    return f0;
  }

  public void setVertexMappings(K[] vertexMappings) {
    f0 = vertexMappings;
  }

  public K[] getEdgeMappings() {
    return f1;
  }

  public void setEdgeMappings(K[] edgeMappings) {
    f1 = edgeMappings;
  }
}
