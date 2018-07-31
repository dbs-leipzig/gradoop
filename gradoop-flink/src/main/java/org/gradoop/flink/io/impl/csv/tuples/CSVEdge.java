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

  public String getId() {
    return f0;
  }

  public void setId(String id) {
    f0 = id;
  }

  public String getGradoopIds() {
    return f1;
  }

  public void setGradoopIds(String gradoopIds) {
    f1 = gradoopIds;
  }

  public String getSourceId() {
    return f2;
  }

  public void setSourceId(String sourceId) {
    f2 = sourceId;
  }

  public String getTargetId() {
    return f3;
  }

  public void setTargetId(String targetId) {
    f3 = targetId;
  }

  public String getLabel() {
    return f4;
  }

  public void setLabel(String label) {
    f4 = label;
  }

  public String getProperties() {
    return f5;
  }

  public void setProperties(String properties) {
    f5 = properties;
  }
}
