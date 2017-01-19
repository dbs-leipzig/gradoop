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
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
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
   * Initialises an empty EmbeddingMetaData object
   */
  public EmbeddingMetaData() {
    this(new HashMap<>(), new HashMap<>());
  }

  /**
   * Initializes a new EmbeddingMetaData object from the given mappings
   *
   * @param entryMapping maps variables to embedding entries
   * @param propertyMapping maps variable-propertyKey pairs to embedding property data entries
   */
  public EmbeddingMetaData(Map<Pair<String, EntryType>, Integer> entryMapping,
    Map<Pair<String, String>, Integer> propertyMapping) {
    this.entryMapping = entryMapping;
    this.propertyMapping = propertyMapping;
  }

  /**
   * Initializes a new EmbeddingMetaData object using copies of the provided meta data.
   *
   * @param metaData meta data to be copied
   */
  public EmbeddingMetaData(EmbeddingMetaData metaData) {
    this.entryMapping = new HashMap<>(metaData.getEntryCount());
    this.propertyMapping = new HashMap<>(metaData.getPropertyCount());

    metaData.getVariables().forEach(var -> {
      this.entryMapping.put(Pair.of(var, metaData.getEntryType(var)), metaData.getEntryColumn(var));
      metaData.getPropertyKeys(var).forEach(key ->
        this.propertyMapping.put(Pair.of(var, key), metaData.getPropertyColumn(var, key)));
    });
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
   * Inserts or updates a column mapping entry
   *
   * @param variable referenced variable
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
    Pair<String, EntryType> key = Pair.of(variable, EntryType.VERTEX);

    if (!entryMapping.containsKey(key)) {
      key = Pair.of(variable, EntryType.EDGE);
      if(!entryMapping.containsKey(key)) {
        key = Pair.of(variable, EntryType.PATH);
        if (!entryMapping.containsKey(key)) {
          throw new NoSuchElementException(String.format("no entry for variable %s", variable));
        }
      }
    }
    return entryMapping.get(key);
  }

  /**
   * Checks if the specified variable is mapped to a column in the embedding.
   *
   * @param variable query variable
   * @return true, iff the variable is mapped to a column
   */
  public boolean containsEntryColumn(String variable) {
    return entryMapping.containsKey(Pair.of(variable, EntryType.VERTEX)) ||
      entryMapping.containsKey(Pair.of(variable, EntryType.EDGE)) ||
      entryMapping.containsKey(Pair.of(variable, EntryType.PATH));
  }

  /**
   * Returns the entry type of the given variable.
   *
   * @param variable query variable
   * @return Entry type of the referred entry
   * @throws NoSuchElementException if there is no column mapped to the specified variable
   */
  public EntryType getEntryType(String variable) {
    Pair<String, EntryType> key = Pair.of(variable, EntryType.VERTEX);

    return Arrays.stream(EntryType.values())
      .filter(entryType -> entryMapping.containsKey(Pair.of(variable, entryType)))
      .collect(Collectors.toList()).get(0);
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
   * Returns a list of variables that are contained in the embedding and referring to vertices. The
   * order of the variables is determined by their position within the embedding.
   *
   * @return a list of all vertex variables
   */
  public List<String> getVertexVariables() {
    return entryMapping.entrySet().stream()
      .filter(entry -> entry.getKey().getRight() == EntryType.VERTEX)
      .sorted(Comparator.comparingInt(Map.Entry::getValue))
      .map(entry -> entry.getKey().getLeft())
      .collect(Collectors.toList());
  }

  /**
   * Returns a list of variables that are contained in the embedding and referring to edges. The
   * order of the variables is determined by their position within the embedding.
   *
   * @return a list of all edge variables
   */
  public List<String> getEdgeVariables() {
    return entryMapping.entrySet().stream()
      .filter(entry -> entry.getKey().getRight() == EntryType.EDGE)
      .sorted(Comparator.comparingInt(Map.Entry::getValue))
      .map(entry -> entry.getKey().getLeft())
      .collect(Collectors.toList());
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
}
