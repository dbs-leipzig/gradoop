/*
 * Copyright © 2014 - 2020 Leipzig University (Database Research Group)
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
package org.gradoop.temporal.io.impl.csv.tuples;

import org.apache.flink.api.java.tuple.Tuple7;

/**
 * Tuple representing a temporal edge in a CSV file.
 * <p>
 * The tuple fields are:
 * <ol start="0">
 *   <li>the ID, as a String</li>
 *   <li>the IDs of the graphs that contain this edge</li>
 *   <li>the ID of the source vertex</li>
 *   <li>the ID of the target vertex</li>
 *   <li>the label</li>
 *   <li>the properties, encoded as a String according to the meta data</li>
 *   <li>the temporal data (transaction- and valid-times)</li>
 * </ol>
 */
public class TemporalCSVEdge extends Tuple7<String, String, String, String, String, String, String>
  implements TemporalCSVElement {

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


  @Override
  public void setTemporalData(String temporalData) {
    f6 = temporalData;
  }
}
