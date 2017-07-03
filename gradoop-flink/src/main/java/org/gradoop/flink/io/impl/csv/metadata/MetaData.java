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
package org.gradoop.flink.io.impl.csv.metadata;

import java.util.List;
import java.util.Map;

/**
 * Describes the data stored in the vertex and edge CSV files.
 */
public class MetaData {
  /**
   * Mapping between an element label and its associated property meta data.
   */
  private Map<String, List<PropertyMetaData>> metaData;

  /**
   * Constructor
   *
   * @param metaData meta data
   */
  MetaData(Map<String, List<PropertyMetaData>> metaData) {
    this.metaData = metaData;
  }

  /**
   * Returns the property meta data associated with the specified label.
   *
   * @param label element label
   * @return property meta data for the element
   */
  public List<PropertyMetaData> getPropertyMetaData(String label) {
    return metaData.get(label);
  }
}
