/*
 * This file is part of gradoop.
 *
 * gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with gradoop. If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.model.impl.id;

import com.google.common.collect.Sets;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * Represents a set of GradoopIds.
 *
 * @see GradoopId
 */
public class GradoopIds implements Iterable<GradoopId>, Writable {

  /**
   * Holds the identifiers.
   */
  private Set<GradoopId> identifiers;

  /**
   * Creates a new instance.
   */
  public GradoopIds() {
    identifiers = Sets.newHashSet();
  }

  GradoopIds(Long[] ids) {
    for(Long id : ids) {
      identifiers.add(new GradoopId(id));
    }
  }

  public GradoopIds(GradoopId... ids) {
    for(GradoopId id : ids) {
      identifiers.add(id);
    }
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
    identifiers = Sets.newHashSetWithExpectedSize(count);

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

  public void clear() {
    identifiers.clear();
  }

  public int size() {
    return identifiers.size();
  }

  public static GradoopIds fromLongs(Long... ids) {
    return new GradoopIds(ids);
  }

  public void addAll(Collection<GradoopId> gradoopIds) {
    identifiers.addAll(gradoopIds);
  }

  public boolean containsAll(GradoopIds other) {
    return this.identifiers.containsAll(other.identifiers);
  }

  public void addAll(GradoopIds gradoopIds) {
    addAll(gradoopIds.identifiers);
  }

  public boolean isEmpty() {
    return this.identifiers.isEmpty();
  }

  public boolean containsAll(Collection<GradoopId> identifiers) {
    return identifiers.containsAll(identifiers);
  }

  public Collection<GradoopId> toCollection() {
    return identifiers;
  }
}
