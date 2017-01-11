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

public class EmbeddingRecordMetaData implements Serializable{
  private Map<String, Integer> columnMapping;
  private Map<Pair<String,String>, Integer> propertyMapping;

  public EmbeddingRecordMetaData() {
    this.columnMapping = new HashMap<>();
    this.propertyMapping = new HashMap<>();
  }

  public EmbeddingRecordMetaData(Map<String, Integer> columnMapping,
    Map<Pair<String,String>, Integer> propertyMapping) {
    this.columnMapping = columnMapping;
    this.propertyMapping = propertyMapping;
  }

  public void updateColumnMapping(String variable, int column) {
    columnMapping.put(variable,column);
  }

  public int getColumn(String variable) {
    return columnMapping.getOrDefault(variable, -1);
  }

  public void updatePropertyMapping(String variable, String property, int index) {
    propertyMapping.put(Pair.of(variable,property),index);
  }

  public int getPropertyIndex(String variable, String property) {
    return propertyMapping.getOrDefault(Pair.of(variable,property),-1);
  }

  public Set<String> getVariables() {
    return columnMapping.keySet();
  }

  public Set<String> getProperties(String variable) {
    Set<String> properties = new HashSet<>();

    for(Pair<String,String> variableAndProperty: propertyMapping.keySet()) {
      if(variableAndProperty.getLeft().equals(variable)) {
        properties.add(variableAndProperty.getRight());
      }
    }

    return properties;
  }
}
