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
import org.gradoop.flink.model.impl.operators.matching.single.cypher.common.pojos.EmbeddingRecord;

import java.util.ArrayList;
import java.util.List;

public class GraphElementToEmbedding {

  public static EmbeddingRecord convert(Vertex vertex, List<String> propertyKeys) {
    EmbeddingRecord embedding = new EmbeddingRecord();
    embedding.add(vertex.getId(), project(vertex, propertyKeys));

    return embedding;
  }

  public static EmbeddingRecord convert(Edge edge, List<String> propertyKeys) {
    EmbeddingRecord embedding = new EmbeddingRecord();
    embedding.add(edge.getSourceId());
    embedding.add(edge.getId(), project(edge, propertyKeys));
    embedding.add(edge.getTargetId());

    return embedding;
  }

  private static List<PropertyValue> project(GraphElement element, List<String> propertyKeys) {
    List<PropertyValue> propertyValues = new ArrayList<>();
    for(String propertyKey : propertyKeys) {
      if(propertyKey.equals("__label__")) {
        propertyValues.add(PropertyValue.create(element.getLabel()));
      } else {
        propertyValues.add(
          element.hasProperty(propertyKey) ? element.getPropertyValue(propertyKey) : PropertyValue.NULL_VALUE);
      }
    }

    return propertyValues;
  }
}
