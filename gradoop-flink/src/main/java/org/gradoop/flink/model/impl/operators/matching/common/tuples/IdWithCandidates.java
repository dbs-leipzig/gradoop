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
 * Represents an EPGM graph element (vertex/edge) and its query candidates. The
 * query candidates are represented by a bit vector. The value for each index
 * in the vector indicates if the element may represent the query element
 * with the same index.
 *
 * Example:
 *
 * query graph: (0:A)-[0:a]->(1:B)
 * data graph:  (0:A)-[0:a]->(1:B)-[1:b]->(2:A)
 *
 * Vertices:
 *
 * bit vector size is 2 as there are 2 query vertices
 *
 * IdWithCandidates(0): (0,[true, false])
 * IdWithCandidates(1): (1,[false, true])
 * IdWithCandidates(2): (1,[true, false])
 *
 * Edges:
 *
 * bit vector size is 1 as there is 1 query edge
 *
 * IdWithCandidates(0): (0, [true])
 * IdWithCandidates(1): (1, [false])
 *
 * f0: element id
 * f1: query candidates
 *
 * @param <K> key type
 */
public class IdWithCandidates<K> extends Tuple2<K, boolean[]> {

  public K getId() {
    return f0;
  }

  public void setId(K id) {
    f0 = id;
  }

  public boolean[] getCandidates() {
    return f1;
  }

  public void setCandidates(boolean[] candidates) {
    f1 = candidates;
  }
}
