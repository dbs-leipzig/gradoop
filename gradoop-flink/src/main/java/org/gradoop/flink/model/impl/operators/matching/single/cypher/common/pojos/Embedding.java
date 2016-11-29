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

import com.google.common.collect.Lists;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;

import java.util.ArrayList;

import static java.util.stream.Collectors.joining;

/**
 * Represents a (partial) embedding of a query.
 * An Embedding consists of a list of {@link EmbeddingEntry} which represent the vertices and edges
 */
public class Embedding {
  /**
   * List of entries in this embedding
   */
  private ArrayList<EmbeddingEntry> entries;

  /**
   * Creates a new empty Embedding
   */
  public Embedding() {
    this(new ArrayList<>());
  }

  /**
   * Creates am embedding with the given entries
   *
   * @param entries initial embedding entries
   */
  public Embedding(ArrayList<EmbeddingEntry> entries) {
    this.entries = entries;
  }

  /**
   * Returns an entry specified by the index in the list
   *
   * @param index the entries index in the list
   * @return the entry
   */
  public EmbeddingEntry getEntry(int index) {
    return entries.get(index);
  }

  /**
   * Sets the entries for this embedding.
   *
   * @param entries embedding entries
   */
  public void setEntries(ArrayList<EmbeddingEntry> entries) {
    this.entries = entries;
  }

  /**
   * Returns a list of all entries.
   *
   * @return entry list
   */
  public ArrayList<EmbeddingEntry> getEntries() {
    return entries;
  }

  /**
   * Adds an entry at the end of the list.
   *
   * @param entry entry that will be appended
   */
  public void addEntry(EmbeddingEntry entry) {
    entries.add(entry);
  }

  /**
   * Adds a list of entries.
   *
   * @param other entry list
   */
  public void addEntries(ArrayList<EmbeddingEntry> other) {
    entries.addAll(other);
  }

  /**
   * Add an entry to the list at a specified index, replace what was there before.
   *
   * @param index the index where the entry will be inserted
   * @param entry the entry
   */
  public void setEntry(Integer index, EmbeddingEntry entry) {
    entries.set(index, entry);
  }

  /**
   * Returns number of entries in the embedding.
   *
   * @return embedding size
   */
  public int size() {
    return entries.size();
  }

  /**
   * Create an embedding from an Edge.
   *
   * @param edge the embedding
   * @return the embedding created from the edge
   */
  public static Embedding fromEdge(Edge edge) {
    return new Embedding(Lists.newArrayList(
      new IdEntry(edge.getSourceId()),
      new GraphElementEntry(edge),
      new IdEntry(edge.getTargetId())));
  }

  /**
   * Create an embedding from a vertex.
   *
   * @param vertex the vertex
   * @return the embedding created from the vertex
   */
  public static Embedding fromVertex(Vertex vertex) {
    return new Embedding(Lists.newArrayList(
      new GraphElementEntry(vertex)
    ));
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    Embedding embedding = (Embedding) o;

    return entries != null ? entries.equals(embedding.entries) : embedding.entries == null;

  }

  @Override
  public int hashCode() {
    return entries != null ? entries.hashCode() : 0;
  }

  @Override
  public String toString() {
    return "[ " + entries.stream().map(EmbeddingEntry::toString).collect(joining(", ")) + " ]";
  }
}
