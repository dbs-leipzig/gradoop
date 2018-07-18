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
package org.gradoop.common.model.impl.id;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.types.Value;

import java.io.IOException;
import java.util.AbstractSet;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * Represents a set of {@link GradoopId} instances, ignoring any duplicates.
 *
 * Note that by implementing {@link java.util.List} Flink uses the Kryo serializer for
 * (de-)serializing the list.
 *
 * @see GradoopId
 */
public class GradoopIdSet extends AbstractSet<GradoopId> implements Value {
  /**
   * Contains the set of gradoop ids.
   */
  private Set<GradoopId> ids;

  /**
   * Required default constructor for instantiation by serialization logic.
   */
  public GradoopIdSet() {
    this.ids = new HashSet<>();
  }

  /**
   * Initializes the set with the given byte array.
   *
   * @param bytes bytes representing multiple gradoop ids
   */
  private GradoopIdSet(byte[] bytes) {
    this.ids = readIds(bytes);
  }

  /**
   * Initializes the set with the given ids.
   *
   * @param ids a collection of {@link GradoopId}s
   */
  private GradoopIdSet(Collection<GradoopId> ids) {
    this.ids = new HashSet<>(ids);
  }

  /**
   * Since we will need to do operations on individual ids we need
   * to reconstruct the {@link GradoopId} instances.
   * @param bytes serialized sequence of {@link GradoopId}s
   * @return a set representation
   */
  private Set<GradoopId> readIds(byte[] bytes) {
    ids = new HashSet<>();
    for (int i = 0; i < bytes.length / GradoopId.ID_SIZE; i++) {
      byte[] idBytes = new byte[GradoopId.ID_SIZE];
      System.arraycopy(bytes, i * GradoopId.ID_SIZE, idBytes, 0, GradoopId.ID_SIZE);
      ids.add(GradoopId.fromByteArray(idBytes));
    }
    return ids;
  }

  /**
   * Serialize all ids into a byte array.
   * @param ids sequence of {@link GradoopId}s
   * @return a binary representation
   */
  private byte[] writeIds(Set<GradoopId> ids) {
    byte[] bytes = new byte[ids.size() * GradoopId.ID_SIZE];

    int i = 0;
    for (GradoopId id : ids) {
      System.arraycopy(id.toByteArray(), 0, bytes, i * GradoopId.ID_SIZE, GradoopId.ID_SIZE);
      i++;
    }
    return bytes;
  }

  /**
   * Creates a new instance from multiple GradoopIDs.
   *
   * @param ids array of gradoop ids
   * @return gradoop id set
   */
  public static GradoopIdSet fromExisting(GradoopId... ids) {
    return fromExisting(Arrays.asList(ids));
  }

  /**
   * Creates a new instance from multiple GradoopIDs.
   *
   * @param ids given ids
   * @return gradoop id set
   */
  public static GradoopIdSet fromExisting(Collection<GradoopId> ids) {
    return new GradoopIdSet(ids);
  }

  /**
   * Creates a new instance from multiple GradoopIDs represented as byte array.
   *
   * @param bytes byte array representing multiple gradoop ids
   * @return gradoop id set
   */
  public static GradoopIdSet fromByteArray(byte[] bytes) {
    return new GradoopIdSet(bytes);
  }

  /**
   * Adds the given gradoop id to the set.
   *
   * @param id the id to add
   */
  @Override
  public boolean add(GradoopId id) {
    return this.ids.add(id);
  }

  /**
   * Adds the given gradoop ids to the set.
   *
   * @param ids the ids to add
   */
  public void addAll(GradoopIdSet ids) {
    this.ids.addAll(ids.ids);
  }

  /**
   * Adds the given gradoop ids to the set.
   *
   * @param ids the ids to add
   */
  @Override
  public boolean addAll(Collection<? extends GradoopId> ids) {
    return this.ids.addAll(ids);
  }

  /**
   * Checks if the given id is contained in the set.
   *
   * @param identifier the id to look for
   * @return true, iff the given id is in the set
   */
  @Override
  public boolean contains(Object identifier) {
    return this.ids.contains(identifier);
  }

  /**
   * Checks if the specified ids are contained in the set.
   *
   * @param other the ids to look for
   * @return true, iff all specified ids are contained in the set
   */
  public boolean containsAll(GradoopIdSet other) {
    if (other.size() > this.size()) {
      return false;
    }
    for (GradoopId id : other) {
      if (!this.contains(id)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Checks if the specified ids are contained in the set.
   *
   * @param other the ids to look for
   * @return true, iff all specified ids are contained in the set
   */
  @Override
  public boolean containsAll(Collection<?> other) {
    if (other instanceof Set && other.size() > this.size()) {
      return false;
    }
    for (Object id : other) {
      if (!this.contains(id)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Checks if any of the specified ids is contained in the set.
   *
   * @param other the ids to look for
   * @return true, iff any of the specified ids is contained in the set
   */
  public boolean containsAny(GradoopIdSet other) {
    // Algorithm: the sizes of both lists might be vastly different
    // to prevent the case of iterating multiple times over large collections
    // we make sure to always iterate over the smaller one
    Set<GradoopId> iterate = this.ids;
    Set<GradoopId> contains = other.ids;
    int thisSize = this.size();
    int otherSize = other.size();
    if (thisSize > otherSize) {
      iterate = other.ids;
      contains = this.ids;
    }

    for (GradoopId id : iterate) {
      if (contains.contains(id)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Checks if any of the specified ids is contained in the set.
   *
   * @param other the ids to look for
   * @return true, iff any of the specified ids is contained in the set
   */
  public boolean containsAny(Set<GradoopId> other) {
    Set<GradoopId> iterate = this.ids;
    Set<GradoopId> contains = other;
    int thisSize = this.size();
    int otherSize = other.size();
    if (thisSize > otherSize) {
      iterate = other;
      contains = this.ids;
    }

    for (GradoopId id : iterate) {
      if (contains.contains(id)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Checks if the set is empty.
   *
   * @return true, iff the set contains no elements
   */
  public boolean isEmpty() {
    return ids.isEmpty();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Iterator<GradoopId> iterator() {
    return ids.iterator();
  }

  /**
   * Clears the set.
   */
  public void clear() {
    ids.clear();
  }

  /**
   * Returns the number of contained gradoop ids
   *
   * @return number of elements in the set
   */
  public int size() {
    return ids.size();
  }

  /**
   * Returns the byte representation of that set.
   *
   * @return byte array representation
   */
  public byte[] toByteArray() {
    return writeIds(ids);
  }

  @Override
  public void write(DataOutputView out) throws IOException {
    if (isEmpty()) {
      out.writeInt(0);
    } else {
      out.writeInt(size());
      out.write(writeIds(ids));
    }
  }

  @Override
  public void read(DataInputView in) throws IOException {
    int n = in.readInt();
    byte[] bytes = new byte[n * GradoopId.ID_SIZE];
    in.readFully(bytes);
    this.ids = readIds(bytes);
  }

  @Override
  public boolean equals(Object o) {
    boolean equal = this == o;

    if (!equal && o instanceof GradoopIdSet) {
      GradoopIdSet that = (GradoopIdSet) o;
      // same number of ids
      equal = this.size() == that.size();

      if (equal) {
        // same ids
        equal = this.ids.equals(that.ids);
      }
    }

    return equal;
  }

  @Override
  public int hashCode() {
    return ids.hashCode();
  }

  @Override
  public String toString() {
    if (isEmpty()) {
      return "[]";
    }

    Iterator<GradoopId> it = iterator();
    StringBuilder sb = new StringBuilder();
    sb.append('[');

    for (;;) {
      GradoopId id = it.next();
      sb.append(id);
      if (!it.hasNext()) {
        return sb.append(']').toString();
      }
      sb.append(',').append(' ');
    }
  }
}
