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

package org.gradoop.flink.model.impl.operators.matching.single.cypher.common.pojos;

import org.apache.commons.lang3.tuple.Pair;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * This class stores meta data information about a set of embeddings
 * It includes information about the mapping of variables to embedding entries
 * and where properties are stored in the embedding
 */
public class EmbeddingMetaData implements Serializable {
  /**
   * Stores the mapping of variables to embedding entries
   */
  private Map<String, Integer> columnMapping;

  /**
   * Stores where the coresponding PropertyValue of a Variable-PropertyKey-Pair is stored within the
   * embedding
   */
  private Map<Pair<String, String>, Integer> propertyMapping;

  /**
   * Initialises an empty EmbeddingMetaData object
   */
  public EmbeddingMetaData() {
    this(new HashMap<>(), new HashMap<>());
  }

  /**
   * Initializes a new EmbeddingMetaData object from the given mappings
   * @param columnMapping maps variables to embedding entries
   * @param propertyMapping maps variable-propertyKey pairs to embedding property data entries
   */
  public EmbeddingMetaData(Map<String, Integer> columnMapping,
    Map<Pair<String, String>, Integer> propertyMapping) {
    this.columnMapping = columnMapping;
    this.propertyMapping = propertyMapping;
  }

  /**
   * Inserts or updates a column mapping entry
   * @param variable referenced variable
   * @param column corresponding embedding entry index
   */
  public void updateColumnMapping(String variable, int column) {
    columnMapping.put(variable, column);
  }

  /**
   * Returns the position of the embedding entry corresponding to the given variable.
   * Returns -1 if the variable is not present within the embedding
   * @param variable variable name
   * @return the position of the corresponding embedding entry
   */
  public int getColumn(String variable) {
    return columnMapping.getOrDefault(variable, -1);
  }

  /**
   * Inserts or updates the mapping of a Variable-PropertyKey pair to the position of the
   * corresponding PropertyValue within the embeddings propertyData array
   * @param variable variable name
   * @param propertyKey property key
   * @param index position of the property value within the propertyData array
   */
  public void updatePropertyMapping(String variable, String propertyKey, int index) {
    propertyMapping.put(Pair.of(variable, propertyKey), index);
  }

  /**
   * Returns the position of the PropertyValue corresponding to the Variable-PropertyKey-Pair.
   * Returns -1 if the property is not present within the embedding.
   * @param variable variable name
   * @param propertyKey property key
   * @return the position of the corresponding property value
   */
  public int getPropertyIndex(String variable, String propertyKey) {
    return propertyMapping.getOrDefault(Pair.of(variable, propertyKey), -1);
  }

  /**
   * Returns a set of all variable that are contained in the embedding
   * @return a set of all variable that are contained in the embedding
   */
  public Set<String> getVariables() {
    return columnMapping.keySet();
  }

  /**
   * Returns a set of all property keys that are contained in the embedding regarding the
   * specified variable.
   * @param variable variable name
   * @return a set of all property keys contained in the embedding
   */
  public Set<String> getPropertyKeys(String variable) {
    Set<String> properties = new HashSet<>();

    for (Pair<String, String> variableAndProperty: propertyMapping.keySet()) {
      if (variableAndProperty.getLeft().equals(variable)) {
        properties.add(variableAndProperty.getRight());
      }
    }

    return properties;
  }
}
