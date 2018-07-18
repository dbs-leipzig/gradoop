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
package org.gradoop.flink.algorithms.fsm.transactional.tle.pojos;

/**
 * Represents an edge within Subgraph embeddings.
 */
public class FSMEdge {

  /**
   * source vertex id
   */
  private final int sourceId;
  /**
   * edge label
   */
  private final String label;
  /**
   * target vertex id
   */
  private final int targetId;

  /**
   * Constructor.
   *
   * @param sourceId source vertex id
   * @param label edge label
   * @param targetId target vertex id
   */
  public FSMEdge(int sourceId, String label, int targetId) {
    this.sourceId = sourceId;
    this.label = label;
    this.targetId = targetId;
  }

  public int getSourceId() {
    return sourceId;
  }

  public int getTargetId() {
    return targetId;
  }

  public String getLabel() {
    return label;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    FSMEdge that = (FSMEdge) o;

    if (sourceId != that.sourceId) {
      return false;
    }
    if (targetId != that.targetId) {
      return false;
    }
    return label.equals(that.label);

  }

  @Override
  public int hashCode() {
    int result = sourceId;
    result = 31 * result + label.hashCode();
    result = 31 * result + targetId;
    return result;
  }

  @Override
  public String toString() {
    return sourceId + "-" + label + "->" + targetId;
  }
}
