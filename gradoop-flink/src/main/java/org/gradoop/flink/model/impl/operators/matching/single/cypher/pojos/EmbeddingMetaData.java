/*
 * Copyright © 2014 - 2018 Leipzig University (Database Research Group)
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
package org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos;

import org.apache.commons.lang3.tuple.Pair;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.utils.ExpandDirection;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * This class stores meta data information about a data set of {@link Embedding} objects.
 *
 * An {@link Embedding} stores identifiers (single or path) and properties associated with query
 * elements.
 *
 * The meta data contains a mapping between query variables and the column index storing the
 * associated element/path identifier. Furthermore, the meta data object contains a mapping between
 * property values associated to property keys at query elements.
 */
public class EmbeddingMetaData implements Serializable {
  /**
   * Describes the type of an embedding entry
   */
  public enum EntryType {
    /**
     * Vertex
     */
    VERTEX,
    /**
     * Edge
     */
    EDGE,
    /**
     * Path
     */
    PATH
  }

  /**
   * Stores the mapping of query variables to embedding entries
   */
  private Map<Pair<String, EntryType>, Integer> entryMapping;
  /**
   * Stores where the corresponding PropertyValue of a Variable-PropertyKey-Pair is stored within
   * the embedding
   */
  private Map<Pair<String, String>, Integer> propertyMapping;
  /**
   * Stores the direction in which paths are stored in the embedding
   */
  private Map<String, ExpandDirection> directionMapping;


  /**
   * Initialises an empty EmbeddingMetaData object
   */
  public EmbeddingMetaData() {
    this(new HashMap<>(), new HashMap<>(), new HashMap<>());
  }

  /**
   * Initializes a new EmbeddingMetaData object from the given mappings
   *
   * @param entryMapping maps variables to embedding entries
   * @param propertyMapping maps variable-propertyKey pairs to embedding property data entries
   * @param directionMapping maps (path) variables to their direction
   */
  public EmbeddingMetaData(Map<Pair<String, EntryType>, Integer> entryMapping,
    Map<Pair<String, String>, Integer> propertyMapping,
    Map<String, ExpandDirection> directionMapping) {
    this.entryMapping = entryMapping;
    this.propertyMapping = propertyMapping;
    this.directionMapping = directionMapping;
  }

  /**
   * Initializes a new EmbeddingMetaData object using copies of the provided meta data.
   *
   * @param metaData meta data to be copied
   */
  public EmbeddingMetaData(EmbeddingMetaData metaData) {
    this.entryMapping = new HashMap<>(metaData.getEntryCount());
    this.propertyMapping = new HashMap<>(metaData.getPropertyCount());
    this.directionMapping = new HashMap<>(metaData.getPathCount());

    metaData.getVariables().forEach(var -> {
        this.entryMapping.put(
          Pair.of(var, metaData.getEntryType(var)), metaData.getEntryColumn(var));
        metaData.getPropertyKeys(var).forEach(key ->
          this.propertyMapping.put(Pair.of(var, key), metaData.getPropertyColumn(var, key)));
        if (metaData.getEntryType(var) == EntryType.PATH) {
          this.directionMapping.put(var, metaData.getDirection(var));
        }
      }
    );
  }

  public Map<Pair<String, EntryType>, Integer> getEntryMapping() {
    return this.entryMapping;
  }

  public Map<Pair<String, String>, Integer> getPropertyMapping() {
    return propertyMapping;
  }

  public Map<String, ExpandDirection> getDirectionMapping() {
    return directionMapping;
  }

  /**
   * Returns the number of entries mapped in this meta data.
   *
   * @return number of mapped entries
   */
  public int getEntryCount() {
    return entryMapping.size();
  }

  /**
   * Returns the number of property values mapped in this meta data.
   *
   * @return number of mapped property values
   */
  public int getPropertyCount() {
    return propertyMapping.size();
  }

  /**
   * Returns the number of variable length paths mapped in this meta data.
   *
   * @return number of variable length paths
   */
  public int getPathCount() {
    return directionMapping.size();
  }

  /**
   * Inserts or updates a column mapping entry
   *
   * @param variable referenced variable
   * @param entryType entry type
   * @param column corresponding embedding entry index
   */
  public void setEntryColumn(String variable, EntryType entryType, int column) {
    entryMapping.put(Pair.of(variable, entryType), column);
  }

  /**
   * Returns the position of the embedding entry corresponding to the given variable.
   * The method checks if the variable is mapped to a vertex or an edge entry.
   *
   * @param variable variable name
   * @return the position of the corresponding embedding entry
   * @throws NoSuchElementException if there is no column mapped to the specified variable
   */
  public int getEntryColumn(String variable) {
    return entryMapping.get(Pair.of(variable, getEntryType(variable)));
  }

  /**
   * Checks if the specified variable is mapped to a column in the embedding.
   *
   * @param variable query variable
   * @return true, iff the variable is mapped to a column
   */
  public boolean containsEntryColumn(String variable) {
    return Arrays.stream(EntryType.values())
      .anyMatch(entryType -> entryMapping.containsKey(Pair.of(variable, entryType)));
  }

  /**
   * Returns the entry type of the given variable.
   *
   * @param variable query variable
   * @return Entry type of the referred entry
   * @throws NoSuchElementException if there is no column mapped to the specified variable
   */
  public EntryType getEntryType(String variable) {
    Optional<EntryType> result = Arrays.stream(EntryType.values())
      .filter(entryType -> entryMapping.containsKey(Pair.of(variable, entryType)))
      .findFirst();

    if (!result.isPresent()) {
      throw new NoSuchElementException(String.format("no entry for variable %s", variable));
    }
    return result.get();
  }

  /**
   * Inserts or updates the mapping of a Variable-PropertyKey pair to the position of the
   * corresponding PropertyValue within the embeddings propertyData array
   *
   * @param variable variable name
   * @param propertyKey property key
   * @param index position of the property value within the propertyData array
   */
  public void setPropertyColumn(String variable, String propertyKey, int index) {
    propertyMapping.put(Pair.of(variable, propertyKey), index);
  }

  /**
   * Returns the position of the PropertyValue corresponding to the Variable-PropertyKey-Pair.
   *
   * @param variable variable name
   * @param propertyKey property key
   * @return the position of the corresponding property value
   * @throws NoSuchElementException if there is no column mapped to the given variable and key
   */
  public int getPropertyColumn(String variable, String propertyKey) {
    Integer column = propertyMapping.get(Pair.of(variable, propertyKey));
    if (column == null) {
      throw new NoSuchElementException(
        String.format("no value for property %s.%s", variable, propertyKey));
    }
    return column;
  }

  /**
   * Inserts or updates the direction for the specified path variable.
   *
   * @param variable variable associated with a variable length path
   * @param direction direction in which the path is stored in the embedding
   */
  public void setDirection(String variable, ExpandDirection direction) {
    directionMapping.put(variable, direction);
  }

  /**
   * Returns the direction in which the path associated with the specified variable is stored
   * in the embedding.
   *
   * @param variable variable associated with a variable length path
   * @return direction
   * @throws NoSuchElementException if the variable has no assigned direction
   */
  public ExpandDirection getDirection(String variable) {
    ExpandDirection expandDirection = directionMapping.get(variable);
    if (expandDirection == null) {
      throw new NoSuchElementException("No direction for: " + variable);
    }
    return expandDirection;
  }

  /**
   * Returns a list of all variable that are contained in the embedding.
   * The order of the variables is determined by their position within the embedding.
   *
   * @return a list of all variables
   */
  public List<String> getVariables() {
    return entryMapping.entrySet().stream()
      .sorted(Comparator.comparingInt(Map.Entry::getValue))
      .map(entry -> entry.getKey().getLeft())
      .collect(Collectors.toList());
  }

  /**
   * Returns a list of all variables that are contained in the embedding and have at least one
   * associated property column.
   *
   * @return a list of all variables with at least one property column
   */
  public List<String> getVariablesWithProperties() {
    return propertyMapping.keySet().stream()
      .map(Pair::getLeft)
      .distinct()
      .collect(Collectors.toList());
  }

  /**
   * Returns a list of variables that are contained in the embedding and refer to vertices. The
   * order of the variables is determined by their position within the embedding.
   *
   * @return a list of all vertex variables
   */
  public List<String> getVertexVariables() {
    return getVariables(entry -> entry == EntryType.VERTEX);
  }

  /**
   * Returns a list of variables that are contained in the embedding and refer to edges. The
   * order of the variables is determined by their position within the embedding.
   *
   * @return a list of all edge variables
   */
  public List<String> getEdgeVariables() {
    return getVariables(entry -> entry == EntryType.EDGE);
  }

  /**
   * Returns a list of variables that are contained in the embedding and refer to paths. The order
   * of the variables is determined by their position within the embedding.
   *
   * @return a list of all path variables
   */
  public List<String> getPathVariables() {
    return getVariables(entry -> entry == EntryType.PATH);
  }

  /**
   * Returns a list of all property keys that are contained in the embedding regarding the
   * specified variable.
   * The order of the keys is determined by the position of the property value in the embedding.
   *
   * @param variable variable name
   * @return a list of all property keys contained in the embedding
   */
  public List<String> getPropertyKeys(String variable) {
    return propertyMapping.entrySet().stream()
      .filter(entry -> entry.getKey().getLeft().equals(variable))
      .sorted(Comparator.comparingInt(Map.Entry::getValue))
      .map(entry -> entry.getKey().getRight())
      .collect(Collectors.toList());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    EmbeddingMetaData metaData = (EmbeddingMetaData) o;

    return entryMapping.equals(metaData.entryMapping) &&
      propertyMapping.equals(metaData.propertyMapping);
  }

  @Override
  public int hashCode() {
    return 31 * entryMapping.hashCode() + propertyMapping.hashCode();
  }

  @Override
  public String toString() {
    List<Map.Entry<Pair<String, EntryType>, Integer>> sortedEntries = entryMapping.entrySet()
      .stream()
      .sorted(Comparator.comparingInt(Map.Entry::getValue))
      .collect(Collectors.toList());

    List<Map.Entry<Pair<String, String>, Integer>> sortiedProperties = propertyMapping.entrySet()
      .stream()
      .sorted(Comparator.comparingInt(Map.Entry::getValue))
      .collect(Collectors.toList());

    return String.format("EmbeddingMetaData{entryMapping=%s, propertyMapping=%s}",
      sortedEntries, sortiedProperties);
  }

  /**
   * Returns the variables that fulfil the specified predicate. The variables are ordered by
   * their appearance in the entry mapping.
   *
   * @param predicate predicate for entry types
   * @return variables that fulfil the predicate
   */
  private List<String> getVariables(Predicate<EntryType> predicate) {
    return entryMapping.entrySet().stream()
      .filter(entry -> predicate.test(entry.getKey().getRight()))
      .sorted(Comparator.comparingInt(Map.Entry::getValue))
      .map(entry -> entry.getKey().getLeft())
      .collect(Collectors.toList());
  }
}
