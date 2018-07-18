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
