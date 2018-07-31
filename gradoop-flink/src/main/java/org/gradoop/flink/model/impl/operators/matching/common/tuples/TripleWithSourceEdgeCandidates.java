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

import org.apache.flink.api.java.tuple.Tuple5;

/**
 * Represents a triple with source and edge candidates.
 *
 * f0: edge id
 * f1: source vertex id
 * f2: source vertex candidates
 * f3: target vertex id
 * f4: edge candidates
 *
 * @param <K> key type
 */
public class TripleWithSourceEdgeCandidates<K> extends Tuple5<K, K, boolean[], K, boolean[]> {

  public K getEdgeId() {
    return f0;
  }

  public void setEdgeId(K edgeId) {
    f0 = edgeId;
  }

  public K getSourceId() {
    return f1;
  }

  public void setSourceId(K sourceId) {
    f1 = sourceId;
  }

  public boolean[] getSourceCandidates() {
    return f2;
  }

  public void setSourceCandidates(boolean[] sourceCandidates) {
    f2 = sourceCandidates;
  }

  public K getTargetId() {
    return f3;
  }

  public void setTargetId(K targetId) {
    f3 = targetId;
  }

  public boolean[] getEdgeCandidates() {
    return f4;
  }

  public void setEdgeCandidates(boolean[] edgeCandidates) {
    f4 = edgeCandidates;
  }
}
