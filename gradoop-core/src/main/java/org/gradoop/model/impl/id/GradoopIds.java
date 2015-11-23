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

/**
 * TODO: remove
 */
public class GradoopIds {

  public static final GradoopId MAX_VALUE = fromLong(Long.MAX_VALUE);

  /**
   * TODO: remove
   */
  public static GradoopId fromLong(long sequence) {
    return new GradoopId(sequence, 0, Context.RUNTIME);
  }

  /**
   * TODO: remove
   */
  public static GradoopId fromLongString(String longString) {
    return fromLong(Long.parseLong(longString));
  }

  /**
   * TODO: remove
   */
  public static GradoopId min(GradoopId id1, GradoopId id2) {
    int compare = id1.compareTo(id2);
    return (compare == 0)
      ? id1
      : compare == -1
        ? id1
        : id2;
  }
}
