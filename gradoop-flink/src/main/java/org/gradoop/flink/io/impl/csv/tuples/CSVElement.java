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

/**
 * Representing a CSVElement in a CSV file.
 */
public interface CSVElement {
  /**
   * Returns the elements label.
   *
   * @return label of the element
   */
  String getLabel();

  /**
   * Sets a new label for the element.
   *
   * @param label new label
   */
  void setLabel(String label);

  /**
   * Returns the elements id.
   *
   * @return element id
   */
  String getId();

  /**
   * Sets an Id for the elements
   *
   * @param id new element id
   */
  void setId(String id);

  /**
   * Returns a string that represents the elements properties.
   *
   * @return element properties
   */
  String getProperties();

  /**
   * Sets a property string for the element
   *
   * @param properties string that represents the elements properties
   */
  void setProperties(String properties);
}
