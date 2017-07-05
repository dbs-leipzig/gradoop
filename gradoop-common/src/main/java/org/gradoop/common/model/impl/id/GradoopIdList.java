/**
 * Copyright © 2014 - 2017 Leipzig University (Database Research Group)
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
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;

/**
 * Represents a list of {@link GradoopId} instances, possibly containing duplicates.
 *
 * Note that by implementing {@link java.util.List} Flink uses the Kryo serializer for
 * (de-)serializing the list.
 *
 * @see GradoopId
 */
public class GradoopIdList implements Iterable<GradoopId>, Value {
  /**
   * Contains the serialized representation of gradoop ids.
   */
  private byte[] bytes;

  /**
   * Required default constructor for instantiation by serialization logic.
   */
  public GradoopIdList() { }

  /**
   * Initializes the list with the given byte array.
   *
   * @param bytes bytes representing multiple gradoop ids
   */
  private GradoopIdList(byte[] bytes) {
    this.bytes = bytes;
  }

  /**
   * Creates a new instance from multiple GradoopIDs.
   *
   * @param ids array of gradoop ids
   * @return gradoop id set
   */
  public static GradoopIdList fromExisting(GradoopId... ids) {
    return fromExisting(Arrays.asList(ids));
  }

  /**
   * Creates a new instance from multiple GradoopIDs.
   *
   * @param ids given ids
   * @return gradoop id set
   */
  public static GradoopIdList fromExisting(Collection<GradoopId> ids) {
    byte[] bytes = new byte[ids.size() * GradoopId.ID_SIZE];

    int i = 0;
    for (GradoopId id : ids) {
      System.arraycopy(id.toByteArray(), 0, bytes, i * GradoopId.ID_SIZE, GradoopId.ID_SIZE);
      i++;
    }

    return new GradoopIdList(bytes);
  }

  /**
   * Creates a new instance from multiple GradoopIDs represented as byte array.
   *
   * @param bytes byte array representing multiple gradoop ids
   * @return gradoop id set
   */
  public static GradoopIdList fromByteArray(byte[] bytes) {
    return new GradoopIdList(bytes);
  }

  /**
   * Adds the given gradoop id to the list.
   *
   * @param id the id to add
   */
  public void add(GradoopId id) {
    if (isEmpty()) {
      bytes = id.toByteArray();
    } else {
      byte[] extended = Arrays.copyOf(bytes, bytes.length + GradoopId.ID_SIZE);
      System.arraycopy(id.toByteArray(), 0, extended, bytes.length, GradoopId.ID_SIZE);
      this.bytes = extended;
    }
  }

  /**
   * Adds the given gradoop ids to the list.
   *
   * @param ids the ids to add
   */
  public void addAll(GradoopIdList ids) {
    if (isEmpty()) {
      bytes = ids.toByteArray();
    } else {
      byte[] extended = Arrays.copyOf(bytes, bytes.length + ids.bytes.length);
      System.arraycopy(ids.bytes, 0, extended, bytes.length, ids.bytes.length);
      this.bytes = extended;
    }
  }

  /**
   * Adds the given gradoop ids to the list.
   *
   * @param ids the ids to add
   */
  public void addAll(Collection<GradoopId> ids) {
    byte[] bytesArray = new byte[ids.size() * GradoopId.ID_SIZE];
    int i = 0;
    for (GradoopId id : ids) {
      System.arraycopy(id.toByteArray(), 0, bytesArray, i * GradoopId.ID_SIZE, GradoopId.ID_SIZE);
      i++;
    }
    addAll(new GradoopIdList(bytesArray));
  }

  /**
   * Checks if the given id is contained in the list.
   *
   * @param identifier the id to look for
   * @return true, iff the given id is in the list
   */
  public boolean contains(GradoopId identifier) {
    if (isEmpty()) {
      return false;
    }
    byte[] idBytes = identifier.toByteArray();
    for (int i = 0; i < bytes.length / GradoopId.ID_SIZE; i++) {
      if (GradoopId.equals(bytes, idBytes, i * GradoopId.ID_SIZE, 0)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Checks if the specified ids are contained in the list.
   *
   * @param ids the ids to look for
   * @return true, iff all specified ids are contained in the list
   */
  public boolean containsAll(GradoopIdList ids) {
    for (GradoopId id : ids) {
      if (!this.contains(id)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Checks if the specified ids are contained in the set.
   *
   * @param ids the ids to look for
   * @return true, iff all specified ids are contained in the list
   */
  public boolean containsAll(Collection<GradoopId> ids) {
    for (GradoopId id : ids) {
      if (!this.contains(id)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Checks if any of the specified ids is contained in the list.
   *
   * @param ids the ids to look for
   * @return true, iff any of the specified ids is contained in the list
   */
  public boolean containsAny(GradoopIdList ids) {
    for (GradoopId id : ids) {
      if (this.contains(id)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Checks if any of the specified ids is contained in the list.
   *
   * @param ids the ids to look for
   * @return true, iff any of the specified ids is contained in the list
   */
  public boolean containsAny(Collection<GradoopId> ids) {
    for (GradoopId id : ids) {
      if (this.contains(id)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Checks if the list is empty.
   *
   * @return true, iff the list contains no elements
   */
  public boolean isEmpty() {
    return bytes == null || bytes.length == 0;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Iterator<GradoopId> iterator() {
    return new GradoopIdListIterator(this.bytes);
  }

  /**
   * Clears the set.
   */
  public void clear() {
    bytes = null;
  }

  /**
   * Returns the number of contained gradoop ids
   *
   * @return number of elements in the list
   */
  public int size() {
    return bytes != null ? bytes.length / GradoopId.ID_SIZE : 0;
  }

  /**
   * Returns the byte representation of that list.
   *
   * @return byte array representation
   */
  public byte[] toByteArray() {
    return bytes != null ? bytes : new byte[0];
  }

  @Override
  public void write(DataOutputView out) throws IOException {
    if (bytes == null) {
      out.writeInt(0);
    } else {
      out.writeInt(bytes.length);
      out.write(bytes);
    }
  }

  @Override
  public void read(DataInputView in) throws IOException {
    int n = in.readInt();
    bytes = new byte[n];
    in.readFully(bytes);
  }

  @Override
  public boolean equals(Object o) {
    boolean equal = this == o;

    if (!equal && o instanceof GradoopIdList) {
      GradoopIdList that = (GradoopIdList) o;
      // same number of ids
      equal = this.size() == that.size();

      if (equal) {
        // same ids
        equal = Objects.deepEquals(this.bytes, that.bytes);
      }
    }

    return equal;
  }

  @Override
  public int hashCode() {
    int h = 0;
    for (GradoopId id : this) {
      h += id.hashCode();
    }
    return h;
  }

  @Override
  public String toString() {
    if (bytes == null || size() == 0) {
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

  /**
   * Iterates through the byte array and returns {@link GradoopId} instances.
   */
  private static class GradoopIdListIterator implements Iterator<GradoopId> {
    /**
     * current index
     */
    private int i;
    /**
     * gradoop ids
     */
    private final byte[] bytes;
    /**
     * number of gradoop ids
     */
    private final int size;

    /**
     * Creates a new iterator.
     *
     * @param bytes byte representation of a {@link GradoopIdList}
     */
    GradoopIdListIterator(byte[] bytes) {
      this.i = 0;
      this.bytes = bytes;
      this.size = this.bytes != null ? bytes.length / GradoopId.ID_SIZE : 0;
    }

    @Override
    public boolean hasNext() {
      return i < size;
    }

    @Override
    public GradoopId next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      int from = GradoopId.ID_SIZE * i;
      int to = from + GradoopId.ID_SIZE;
      i++;
      return GradoopId.fromByteArray(Arrays.copyOfRange(bytes, from, to));
    }
  }
}
