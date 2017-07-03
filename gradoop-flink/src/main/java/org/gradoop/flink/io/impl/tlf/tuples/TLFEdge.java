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
package org.gradoop.flink.io.impl.tlf.tuples;

import org.apache.flink.api.java.tuple.Tuple3;

/**
 * Represents an edge used in a graph generation from TLF-files.
 */
public class TLFEdge extends Tuple3<Integer, Integer, String> {

  /**
   * Symbol identifying a line to represent an edge.
   */
  public static final String SYMBOL = "e";

  /**
   * default constructor
   */
  public TLFEdge() {
  }

  /**
   * valued constructor
   * @param sourceId id of the source vertex
   * @param targetId id of the target vertex
   * @param label edge label
   */
  public TLFEdge(Integer sourceId, Integer targetId, String label) {
    super(sourceId, targetId, label);
  }

  public Integer getSourceId() {
    return this.f0;
  }

  public void setSourceId(Integer sourceId) {
    this.f0 = sourceId;
  }

  public Integer getTargetId() {
    return this.f1;
  }

  public void setTargetId(Integer targetId) {
    this.f1 = targetId;
  }

  public String getLabel() {
    return this.f2;
  }

  public void setLabel(String label) {
    this.f2 = label;
  }

}
