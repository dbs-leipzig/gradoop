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

package org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos;

import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphElement;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.PropertyValue;

import java.util.List;

/**
 * Utility class to convert an EPGM element ({@link Vertex} and {@link Edge} into an
 * {@link Embedding}.
 */
public class EmbeddingFactory {

  /**
   * Converts a {@link Vertex} into an {@link Embedding}.
   *
   * The resulting embedding has one entry containing the vertex id and one entry for each property
   * value associated with the specified property keys (ordered by list order). Note that missing
   * property values are represented by a {@link PropertyValue#NULL_VALUE}.
   *
   * @param vertex vertex to create embedding from
   * @param propertyKeys properties that will be stored in the embedding
   * @return Embedding
   */
  public static Embedding fromVertex(Vertex vertex, List<String> propertyKeys) {
    Embedding embedding = new Embedding();
    embedding.add(vertex.getId(), project(vertex, propertyKeys));

    return embedding;
  }

  /**
   * Converts an {@link Edge} into an {@link Embedding}.
   *
   * The resulting embedding has three entries containing the source vertex id, the edge id and the
   * target vertex id. Furthermore, the embedding has one entry for each property value associated
   * with the specified property keys (ordered by list order). Note that missing property values are
   * represented by a {@link PropertyValue#NULL_VALUE}.
   *
   * @param edge edge to create embedding from
   * @param propertyKeys properties that will be stored in the embedding
   * @param isLoop indicates if the edges is a loop
   * @return Embedding
   */
  public static Embedding fromEdge(Edge edge, List<String> propertyKeys, boolean isLoop) {
    Embedding embedding = new Embedding();

    if (isLoop) {
      embedding.addAll(edge.getSourceId(), edge.getId());
    } else {
      embedding.addAll(edge.getSourceId(), edge.getId(), edge.getTargetId());
    }

    embedding.addPropertyValues(project(edge, propertyKeys));

    return embedding;
  }

  /**
   * Converts an {@link Triple} into an {@link Embedding}.
   *
   * The resulting embedding has two or three entries containing the source vertex id, the edge id
   * and the target vertex id. Furthermore, the embedding has one entry for each property value
   * associated with the specified property keys (ordered by source properties, edge properties,
   * target properties in list order). Note that missing property values are represented
   * by a {@link PropertyValue#NULL_VALUE}.
   *
   * @param triple triple to create embedding from
   * @param sourcePropertyKeys source properties that will be stored in the embedding
   * @param edgePropertyKeys edge properties that will be stored in the embedding
   * @param targetPropertyKeys target properties that will be stored in the embedding
   * @param sourceVertexVariable variable of the source vertex
   * @param targetVertexVariable variable of the target vertex
   * @return Embedding
   */
  public static Embedding fromTriple(Triple triple, List<String> sourcePropertyKeys, List<String>
    edgePropertyKeys, List<String> targetPropertyKeys, String sourceVertexVariable,
    String targetVertexVariable) {

    Embedding embedding = new Embedding();
    embedding.add(
      triple.getSourceVertex().getId(),
      project(triple.getSourceVertex(), sourcePropertyKeys)
    );

    embedding.add(
      triple.getEdge().getId(),
      project(triple.getEdge(), edgePropertyKeys)
    );

    if (sourceVertexVariable.equals(targetVertexVariable)) {
      return embedding;
    }

    embedding.add(
      triple.getTargetVertex().getId(),
      project(triple.getTargetVertex(), targetPropertyKeys)
    );

    return embedding;
  }

  /**
   * Projects the elements properties into a list of property values. Only those properties
   * specified by their key will be kept. Properties that are specified but not present at the
   * element will be adopted as {@link PropertyValue#NULL_VALUE}.
   *
   * @param element element of which the properties will be projected
   * @param propertyKeys properties that will be projected from the specified element
   * @return projected property values
   */
  private static PropertyValue[] project(GraphElement element, List<String> propertyKeys) {
    PropertyValue[] propertyValues = new PropertyValue[propertyKeys.size()];
    int i = 0;
    for (String propertyKey : propertyKeys) {
      if (propertyKey.equals("__label__")) {
        propertyValues[i++] = PropertyValue.create(element.getLabel());
      } else {
        propertyValues[i++] = element.hasProperty(propertyKey) ?
          element.getPropertyValue(propertyKey) : PropertyValue.NULL_VALUE;
      }
    }
    return propertyValues;
  }
}
