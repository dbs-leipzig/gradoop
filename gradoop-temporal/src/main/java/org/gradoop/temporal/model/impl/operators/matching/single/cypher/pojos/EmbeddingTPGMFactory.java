/*
 * Copyright © 2014 - 2020 Leipzig University (Database Research Group)
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
package org.gradoop.temporal.model.impl.operators.matching.single.cypher.pojos;

import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.Embedding;
import org.gradoop.temporal.model.impl.pojo.TemporalEdge;
import org.gradoop.temporal.model.impl.pojo.TemporalGraphElement;
import org.gradoop.temporal.model.impl.pojo.TemporalVertex;
import org.s1ck.gdl.model.comparables.time.TimeSelector;

import java.util.List;

/**
 * Utility class to convert an element ({@link TemporalVertex} and {@link TemporalEdge} into an
 * {@link Embedding}.
 * Analogous to
 * {@link org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.EmbeddingFactory}
 * but creating TPGMEmbeddings from TPGM elements
 */
public class EmbeddingTPGMFactory {

  /**
   * Converts a {@link TemporalVertex} into an {@link Embedding}.
   * <p>
   * The resulting embedding has one entry containing the vertex id and one entry for each property
   * value associated with the specified property keys (ordered by list order). Note that missing
   * property values are represented by a {@link PropertyValue#NULL_VALUE}.
   * Furthermore, the time data of each element ({tx_from, tx_to, valid_from, valid_to}) is stored
   * in the embedding, just like normal properties
   *
   * @param vertex       vertex to create embedding from
   * @param propertyKeys properties that will be stored in the embedding
   * @return Embedding
   */
  public static Embedding fromVertex(TemporalVertex vertex, List<String> propertyKeys) {
    Embedding embedding = new Embedding();
    embedding.add(vertex.getId(), project(vertex, propertyKeys));
    return embedding;
  }

  /**
   * Converts an {@link TemporalEdge} into an {@link Embedding}.
   * <p>
   * The resulting embedding has three entries containing the source vertex id, the edge id and the
   * target vertex id. Furthermore, the embedding has one entry for each property value associated
   * with the specified property keys (ordered by list order). Note that missing property values are
   * represented by a {@link PropertyValue#NULL_VALUE}.
   * Additionally, the time data of the edge ({tx_from, tx_to, valid_from, valid_to}) are stored
   * in the embedding, just like properties
   *
   * @param edge         edge to create embedding from
   * @param propertyKeys properties that will be stored in the embedding
   * @param isLoop       indicates if the edges is a loop
   * @return Embedding
   */
  public static Embedding fromEdge(TemporalEdge edge, List<String> propertyKeys, boolean isLoop) {
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
   * Projects the elements properties into a list of property values. Only those properties
   * specified by their key will be kept. Properties that are specified but not present at the
   * element will be adopted as {@link PropertyValue#NULL_VALUE}.
   * Temporal properties are stored just like "normal" ones
   *
   * @param element      element of which the properties will be projected
   * @param propertyKeys properties that will be projected from the specified element
   * @return projected property values
   */
  private static PropertyValue[] project(TemporalGraphElement element, List<String> propertyKeys) {
    PropertyValue[] propertyValues = new PropertyValue[propertyKeys.size()];
    int i = 0;
    for (String propertyKey : propertyKeys) {
      if (propertyKey.equals("__label__")) {
        propertyValues[i++] = PropertyValue.create(element.getLabel());
      } else if (isProperty(propertyKey)) {
        propertyValues[i++] = element.hasProperty(propertyKey) ?
          element.getPropertyValue(propertyKey) : PropertyValue.NULL_VALUE;
      } else {
        if (propertyKey.equals(TimeSelector.TimeField.VAL_FROM.toString())) {
          propertyValues[i++] = PropertyValue.create(element.getValidFrom());
        } else if (propertyKey.equals(TimeSelector.TimeField.VAL_TO.toString())) {
          propertyValues[i++] = PropertyValue.create(element.getValidTo());
        } else if (propertyKey.equals(TimeSelector.TimeField.TX_FROM.toString())) {
          propertyValues[i++] = PropertyValue.create(element.getTxFrom());
        } else if (propertyKey.equals(TimeSelector.TimeField.TX_TO.toString())) {
          propertyValues[i++] = PropertyValue.create(element.getTxTo());
        } else {
          propertyValues[i++] = PropertyValue.NULL_VALUE;
        }
      }
    }
    return propertyValues;
  }

  /**
   * Checks if a string refers to an actual property or a time stamp
   * @param key string to check
   * @return true iff key refers to an actual property
   */
  private static boolean isProperty(String key) {
    return !key.equals(TimeSelector.TimeField.VAL_FROM.toString()) &&
      !key.equals(TimeSelector.TimeField.VAL_TO.toString()) &&
      !key.equals(TimeSelector.TimeField.TX_FROM.toString()) &&
      !key.equals(TimeSelector.TimeField.TX_TO.toString());
  }

}
