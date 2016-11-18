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

package org.gradoop.flink.model.impl.operators.matching.single.cypher.utils;

import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.common.model.impl.properties.Property;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.embeddings.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.embeddings.EmbeddingEntry;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.embeddings.ProjectionEntry;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Projects an Embedding
 */
public class Projector {

  /**
   * Projects an embedding
   * @param embedding the embedding to project
   * @param propertyKeyMap mapping of embedding entries to property keys
   * @return the projected embedding
   */
  public static Embedding project(Embedding embedding, Map<Integer, List<String>> propertyKeyMap) {
    for (Map.Entry<Integer, List<String>> pair : propertyKeyMap.entrySet()) {
      Integer column = pair.getKey();

      EmbeddingEntry entry = embedding.getEntry(column);
      Properties properties = entry.getProperties().orElse(new Properties());
      List<String> propertyKeys = pair.getValue();

      ProjectionEntry projectionEntry = new ProjectionEntry(entry.getId());
      projectionEntry.setProperties(projectProperties(properties, propertyKeys));

      embedding.setEntry(column, projectionEntry);
    }

    return embedding;
  }

  /**
   * projects Properties to include only specified properties
   * @param properties the properties which will be kept
   * @param propertyKeys List of property names that will be kept in the projection
   * @return the projected property list
   */
  private static Properties projectProperties(Properties properties,
    List<String> propertyKeys) {

    Properties projectedList = new Properties();
    List<String> remainingKeys = new ArrayList<>(propertyKeys);

    // add existing properties to projection
    for (Property property : properties) {
      if (remainingKeys.contains(property.getKey())) {
        remainingKeys.remove(property.getKey());
        projectedList.set(property);
      }
    }

    // add missing properties as null values
    for (String key : remainingKeys) {
      projectedList.set(Property.create(key, PropertyValue.NULL_VALUE));
    }

    return projectedList;
  }
}
