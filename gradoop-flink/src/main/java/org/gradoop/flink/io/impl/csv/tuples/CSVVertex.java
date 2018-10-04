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

import org.apache.flink.api.java.tuple.Tuple4;

/**
 * Tuple representing a vertex in a CSV file.
 */
public class CSVVertex extends Tuple4<String, String, String, String> implements CSVElement {

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
}
