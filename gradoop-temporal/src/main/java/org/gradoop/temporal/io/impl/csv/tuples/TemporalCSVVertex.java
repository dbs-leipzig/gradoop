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

import org.apache.flink.api.java.tuple.Tuple5;

/**
 * Tuple representing a temporal vertex in a CSV file.
 * <p>
 * The tuple fields are:
 * <ol start="0">
 *   <li>the ID, as a String</li>
 *   <li>the IDs of the graphs that contain this vertex</li>
 *   <li>the label</li>
 *   <li>the properties, encoded as a String according to the meta data</li>
 *   <li>the temporal data (transaction- and valid-times)</li>
 * </ol>
 */
public class TemporalCSVVertex extends Tuple5<String, String, String, String, String>
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
   * Returns the gradoop ids of the graphs that the vertex belongs to.
   *
   * @return graph gradoop ids
   */
  public String getGradoopIds() {
    return f1;
  }

  /**
   * Sets the gradoop ids of the graphs which contain this vertex.
   *
   * @param gradoopIds graph gradoop ids
   */
  public void setGradoopIds(String gradoopIds) {
    f1 = gradoopIds;
  }

  @Override
  public String getLabel() {
    return f2;
  }

  @Override
  public void setLabel(String label) {
    f2 = label;
  }

  @Override
  public String getProperties() {
    return f3;
  }

  @Override
  public void setProperties(String properties) {
    f3 = properties;
  }

  @Override
  public void setTemporalData(String temporalData) {
    f4 = temporalData;
  }
}
