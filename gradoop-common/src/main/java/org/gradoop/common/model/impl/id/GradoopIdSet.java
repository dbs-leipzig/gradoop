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

package org.gradoop.common.model.impl.id;

import com.google.common.collect.Sets;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.Set;

/**
 * Represents a set of GradoopIds.
 *
 * @see GradoopId
 */
public class GradoopIdSet implements Iterable<GradoopId>,
  WritableComparable<GradoopIdSet>, Serializable {

  /**
   * Holds the identifiers.
   */
  private Set<GradoopId> identifiers;

  /**
   * Creates a new instance.
   */
  public GradoopIdSet() {
    identifiers = Sets.newTreeSet();
  }

  /**
   * Creates a new instance from multiple GradoopIDs.
   *
   * @param ids array of gradoop ids
   * @return gradoop ids
   */
  public static GradoopIdSet fromExisting(GradoopId... ids) {
    return fromExisting(Arrays.asList(ids));
  }

  /**
   * Creates a new instance from multiple GradoopIDs.
   *
   * @param ids given ids
   * @return gradoop ids
   */
  public static GradoopIdSet fromExisting(Collection<GradoopId> ids) {
    GradoopIdSet gradoopIdSet = new GradoopIdSet();

    for (GradoopId id : ids) {
      gradoopIdSet.add(id);
    }

    return gradoopIdSet;
  }

  /**
   * Adds a GradoopId to the set.
   *
   * @param identifier gradoop identifier
   */
  public void add(GradoopId identifier) {
    identifiers.add(identifier);
  }

  /**
   * Checks if the given GradoopId is contained in the set.
   *
   * @param identifier gradoop identifier
   * @return true, iff the given identifier is in the set
   */
  public boolean contains(GradoopId identifier) {
    return identifiers.contains(identifier);
  }

  /**
   * adds existing gradoop ids
   *
   * @param gradoopIds ids to add
   */
  public void addAll(Collection<GradoopId> gradoopIds) {
    identifiers.addAll(gradoopIds);
  }

  /**
   * adds existing gradoop ids
   * @param gradoopIdSet ids to add
   */
  public void addAll(GradoopIdSet gradoopIdSet) {
    addAll(gradoopIdSet.identifiers);
  }

  /**
   * checks, if all gradoop ids are contained
   * @param others gradoop ids
   * @return true, if all contained
   */
  public boolean containsAll(GradoopIdSet others) {
    return this.identifiers.containsAll(others.identifiers);
  }

  /**
   * checks, if all ids of a collection are contained
   * @param identifiers id collection
   * @return true, if all contained
   */
  public boolean containsAll(Collection<GradoopId> identifiers) {
    return this.identifiers.containsAll(identifiers);
  }

  /**
   * Checks, if any of the given ids is contained
   * @param others id collection
   * @return true, if any id is contained
   */
  public boolean containsAny(GradoopIdSet others) {
    boolean result = false;
    for (GradoopId id : others) {
      if (this.identifiers.contains(id)) {
        result = true;
        break;
      }
    }
    return result;
  }

  /**
   * Checks, if any of the given ids is contained
   * @param identifiers id collection
   * @return true, if any id is contained
   */
  public boolean containsAny(Collection<GradoopId> identifiers) {
    GradoopIdSet ids = new GradoopIdSet();
    ids.addAll(identifiers);
    return containsAny(ids);
  }


  /**
   * checks if empty
   * @return true, if empty
   */
  public boolean isEmpty() {
    return this.identifiers.isEmpty();
  }

  /**
   * returns the contained identifiers as a collection
   * @return collection of identifiers
   */
  public Collection<GradoopId> toCollection() {
    return identifiers;
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    dataOutput.writeInt(identifiers.size());
    for (GradoopId id : identifiers) {
      id.write(dataOutput);
    }
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    int count = dataInput.readInt();
    identifiers = Sets.newTreeSet();

    for (int i = 0; i < count; i++) {
      GradoopId id = new GradoopId();
      id.readFields(dataInput);
      identifiers.add(id);
    }
  }

  @Override
  public Iterator<GradoopId> iterator() {
    return identifiers.iterator();
  }

  /**
   * drops all contained gradoop ids
   */
  public void clear() {
    identifiers.clear();
  }

  /**
   * returns the number of contained gradoop ids
   * @return size
   */
  public int size() {
    return identifiers.size();
  }

  @Override
  public int compareTo(GradoopIdSet other) {

    int comparison = this.identifiers.size() - other.identifiers.size();

    if (comparison == 0) {
      Iterator<GradoopId> thisIterator = this.identifiers.iterator();
      Iterator<GradoopId> otherIterator = other.identifiers.iterator();

      while (comparison == 0 && thisIterator.hasNext()) {
        comparison = thisIterator.next().compareTo(otherIterator.next());
      }
    }
    return comparison;
  }

  @Override
  public boolean equals(Object other) {
    return other == this || other instanceof GradoopIdSet &&
      this.identifiers.equals(((GradoopIdSet) other).identifiers);
  }

  @Override
  public int hashCode() {
    return this.identifiers.hashCode();
  }

  @Override
  public String toString() {
    return identifiers.toString();
  }
}
