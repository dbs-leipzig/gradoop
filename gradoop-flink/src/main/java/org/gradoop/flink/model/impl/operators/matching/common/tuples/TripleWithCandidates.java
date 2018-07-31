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

import org.apache.flink.api.java.tuple.Tuple4;

/**
 * Represents an edge, source and target vertex triple that matches at least one
 * triple in the data graph. Each triple contains a list of identifiers that
 * match to edge ids in the query graph.

 * f0: edge id
 * f1: source vertex id
 * f2: target vertex id
 * f3: edge query candidates
 *
 * @param <K> key type
 */
public class TripleWithCandidates<K> extends Tuple4<K, K, K, boolean[]> {

  public K getEdgeId() {
    return f0;
  }

  public void setEdgeId(K id) {
    f0 = id;
  }

  public K getSourceId() {
    return f1;
  }

  public void setSourceId(K id) {
    f1 = id;
  }

  public K getTargetId() {
    return f2;
  }

  public void setTargetId(K id) {
    f2 = id;
  }

  public boolean[] getCandidates() {
    return f3;
  }

  public void setCandidates(boolean[] candidates) {
    f3 = candidates;
  }
}
