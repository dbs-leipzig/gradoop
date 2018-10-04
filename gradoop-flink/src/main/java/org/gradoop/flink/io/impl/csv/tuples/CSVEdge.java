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
package org.gradoop.flink.io.impl.csv.tuples;

import org.apache.flink.api.java.tuple.Tuple6;

/**
 * Tuple representing an edge in a CSV file.
 */
public class CSVEdge extends Tuple6<String, String, String, String, String, String> implements CSVElement {

  @Override
  public String getId() {
    return f0;
  }

  @Override
  public void setId(String id) {
    f0 = id;
  }

  /**
   * Returns the gradoop ids of the graphs that the edge belongs to.
   *
   * @return graph gradoop ids
   */
  public String getGradoopIds() {
    return f1;
  }

  /**
   * Sets the gradoop ids of the graphs which contain this edge.
   *
   * @param gradoopIds graph gradoop ids
   */
  public void setGradoopIds(String gradoopIds) {
    f1 = gradoopIds;
  }

  /**
   * Returns the string that represents the source.
   *
   * @return returns the source id.
   */
  public String getSourceId() {
    return f2;
  }

  /**
   * Sets the string that represents the source.
   *
   * @param sourceId represents the source id.
   */
  public void setSourceId(String sourceId) {
    f2 = sourceId;
  }


  /**
   * Returns the string that represents the target.
   *
   * @return returns the target id.
   */
  public String getTargetId() {
    return f3;
  }

  /**
   * Sets the string that represents the target.
   *
   * @param targetId represents the target id.
   */
  public void setTargetId(String targetId) {
    f3 = targetId;
  }

  @Override
  public String getLabel() {
    return f4;
  }

  @Override
  public void setLabel(String label) {
    f4 = label;
  }

  @Override
  public String getProperties() {
    return f5;
  }

  @Override
  public void setProperties(String properties) {
    f5 = properties;
  }
}
