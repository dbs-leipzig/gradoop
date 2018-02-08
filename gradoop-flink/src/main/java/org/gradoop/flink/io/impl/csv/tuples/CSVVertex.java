/**
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

import org.apache.flink.api.java.tuple.Tuple3;

/**
 * Tuple representing a vertex in a CSV file.
 */
public class CSVVertex extends Tuple3<String, String, String> {

  public String getId() {
    return f0;
  }

  public void setId(String id) {
    f0 = id;
  }

  public String getLabel() {
    return f1;
  }

  public void setLabel(String label) {
    f1 = label;
  }

  public String getProperties() {
    return f2;
  }

  public void setProperties(String properties) {
    f2 = properties;
  }
}
