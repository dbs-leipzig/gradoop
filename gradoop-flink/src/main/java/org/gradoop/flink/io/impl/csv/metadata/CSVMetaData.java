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
package org.gradoop.flink.io.impl.csv.metadata;

import org.gradoop.common.model.impl.metadata.MetaData;
import org.gradoop.common.model.impl.metadata.PropertyMetaData;
import org.gradoop.flink.io.api.metadata.MetaDataSource;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;


/**
 * Class used for meta data read from CSV. Adds a method for accessing the meta data via a type
 * string instead of calling the specific method.
 */
public class CSVMetaData extends MetaData {
  /**
   * Constructor
   *
   * @param graphMetaData  a map between each graph label and its property metadata
   * @param vertexMetaData a map between each vertex label and its property metadata
   * @param edgeMetaData   a map between each edge label and its property metadata
   */
  CSVMetaData(
    Map<String, List<PropertyMetaData>> graphMetaData,
    Map<String, List<PropertyMetaData>> vertexMetaData,
    Map<String, List<PropertyMetaData>> edgeMetaData) {
    super(graphMetaData, vertexMetaData, edgeMetaData);
  }

  /**
   * Get the property meta data for the type and label.
   *
   * @param type  entity type, (g,v or e)
   * @param label entity label
   * @return list containing the property meta data
   */
  public List<PropertyMetaData> getPropertyMetaData(String type, String label) {
    switch (type) {
    case MetaDataSource.GRAPH_TYPE:
      return this.graphMetaData.getOrDefault(label, new ArrayList<>());
    case MetaDataSource.VERTEX_TYPE:
      return this.vertexMetaData.getOrDefault(label, new ArrayList<>());
    case MetaDataSource.EDGE_TYPE:
      return this.edgeMetaData.getOrDefault(label, new ArrayList<>());
    default:
      throw new IllegalArgumentException("Entity type " + type + " is not supported. Supported " +
        "types are g, v and e.");
    }
  }
}
