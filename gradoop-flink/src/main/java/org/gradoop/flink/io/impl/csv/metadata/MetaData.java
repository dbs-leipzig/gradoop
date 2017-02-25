/*
 * This file is part of Gradoop.
 *
 * Gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Gradoop. If not, see <http://www.gnu.org/licenses/>.
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
  public MetaData(Map<String, List<PropertyMetaData>> metaData) {
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
