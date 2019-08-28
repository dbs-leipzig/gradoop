/*
 * Copyright © 2014 - 2019 Leipzig University (Database Research Group)
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

import org.apache.flink.api.java.tuple.Tuple4;

/**
 * Tuple representing a temporal graph head in a CSV file.
 * <p>
 * The tuple fields are:
 * <ol start="0">
 *   <li>the ID, as a String</li>
 *   <li>the label</li>
 *   <li>the properties, encoded as a String according to the meta data</li>
 *   <li>the temporal data (transaction- and valid-times)</li>
 * </ol>
 */
public class TemporalCSVGraphHead extends Tuple4<String, String, String, String>
  implements TemporalCSVElement {

  @Override
  public String getId() {
    return f0;
  }

  @Override
  public void setId(String id) {
    f0 = id;
  }

  @Override
  public String getLabel() {
    return f1;
  }

  @Override
  public void setLabel(String label) {
    f1 = label;
  }

  @Override
  public String getProperties() {
    return f2;
  }

  @Override
  public void setProperties(String properties) {
    f2 = properties;
  }

  @Override
  public void setTemporalData(String temporalData) {
    f3 = temporalData;
  }
}
