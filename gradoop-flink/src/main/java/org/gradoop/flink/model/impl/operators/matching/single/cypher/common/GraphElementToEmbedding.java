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

package org.gradoop.flink.model.impl.operators.matching.single.cypher.common;

import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphElement;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.common.pojos.Embedding;

import java.util.ArrayList;
import java.util.List;

/**
 * Utility class to convert vertices and edges into {@link Embedding}
 */
public class GraphElementToEmbedding {

  /**
   * Converts a vertex into an Embedding
   * The Embedding has one entry with the vertex id and includes the specified properties (in the
   * order specified by the list)
   * @param vertex Vertex that will be converted
   * @param propertyKeys Keys of the properties that will be adopted into the embedding
   * @return Vertex Embedding
   */
  public static Embedding convert(Vertex vertex, List<String> propertyKeys) {
    Embedding embedding = new Embedding();
    embedding.add(vertex.getId(), project(vertex, propertyKeys));

    return embedding;
  }

  /**
   * Converts an Edge into an Embedding
   * The Embedding has three entries (SourceID, EdgeId, TargetID) and includes the specified
   * properties (in the order specified by the list)
   * @param edge Edge that will be converted
   * @param propertyKeys Keys of the properties that will be adopted into the embedding
   * @return Edge Embedding
   */
  public static Embedding convert(Edge edge, List<String> propertyKeys) {
    Embedding embedding = new Embedding();
    embedding.add(edge.getSourceId());
    embedding.add(edge.getId(), project(edge, propertyKeys));
    embedding.add(edge.getTargetId());

    return embedding;
  }

  /**
   * Projects the elements properties into a list of property values. Only those properties
   * specified by their key will be kept. Properties that are specified but not present in the
   * list will be adopted as NULL values.
   * @param element Element of which the properties will be projected
   * @param propertyKeys White-list of property keys
   * @return List of projected property values
   */
  private static List<PropertyValue> project(GraphElement element, List<String> propertyKeys) {
    List<PropertyValue> propertyValues = new ArrayList<>();
    for (String propertyKey : propertyKeys) {
      if (propertyKey.equals("__label__")) {
        propertyValues.add(PropertyValue.create(element.getLabel()));
      } else {
        propertyValues.add(
          element.hasProperty(propertyKey) ?
            element.getPropertyValue(propertyKey) : PropertyValue.NULL_VALUE);
      }
    }

    return propertyValues;
  }
}
