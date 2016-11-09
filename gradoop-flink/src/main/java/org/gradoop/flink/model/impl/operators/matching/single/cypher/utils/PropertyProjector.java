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

import org.gradoop.common.model.impl.pojo.GraphElement;
import org.gradoop.common.model.impl.properties.Property;
import org.gradoop.common.model.impl.properties.PropertyList;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.embeddings.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.embeddings.EmbeddingEntry;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.embeddings.ProjectionEntry;

import java.util.ArrayList;
import java.util.List;

/**
 * Projects a PropertyList to include only specified properties
 */
public class PropertyProjector {
  /**
   * Which properties to keep in the projection
   */
  private List<String> propertyKeys;

  /**
   * Create a new projector
   * @param propertyKeys properties which will be kept
   */
  public PropertyProjector(List<String> propertyKeys) {
    this.propertyKeys = propertyKeys;
  }

  /**
   * projects an embedding entry
   * @param entry the entry to project
   * @return the projected entry
   */
  public ProjectionEntry project(EmbeddingEntry entry) {
    ProjectionEntry projectionEntry = new ProjectionEntry(entry.getId());

    if(entry.getProperties().isPresent()) {
      PropertyList properties = projectPropertyList(entry.getProperties().get());
      projectionEntry.setProperties(properties);
    }

    return projectionEntry;
  }

  /**
   * Projects a graph element
   * @param element the element to project
   * @return the projected entry
   */
  public ProjectionEntry project(GraphElement element) {
    ProjectionEntry projectionEntry = new ProjectionEntry(element.getId());

    projectionEntry.setProperties(projectPropertyList(element.getProperties()));

    // add label to projectionEntry
    if(propertyKeys.contains("__label__")) {
      Property labelProperty = Property.create("__label__", element.getLabel());
      projectionEntry.addProperty(labelProperty);
    }

    return projectionEntry;
  }

  /**
   * projects a PropertyList to include only specified properties
   * @param properties the properties which will be kept
   * @return the projected property list
   */
  private PropertyList projectPropertyList(PropertyList properties) {
    PropertyList projectedList = new PropertyList();
    List<String> remainingKeys = new ArrayList<>(propertyKeys);

    // add existing properties to projection
    for(Property property : properties) {
      if(remainingKeys.contains(property.getKey())) {
        remainingKeys.remove(property.getKey());
        projectedList.set(property);
      }
    }

    // add missing properties ass null values
    for(String key : remainingKeys) {
      projectedList.set(Property.create(key, PropertyValue.NULL_VALUE));
    }

    return projectedList;
  }
}
