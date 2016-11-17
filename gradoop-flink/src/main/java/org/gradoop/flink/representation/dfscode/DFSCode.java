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

package org.gradoop.flink.representation.dfscode;

import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.util.Iterator;
import java.util.List;

/**
 * Represents a graph by the log of a depth first search.
 *
 * @param <C> vertex and edge value type
 */
public class DFSCode<C extends Comparable<C>> implements Serializable, Comparable<DFSCode<C>> {

  /**
   * Included edges.
   */
  private final List<DFSExtension<C>> extensions;

  /**
   * Default constructor.
   */
  public DFSCode() {
    this.extensions = Lists.newArrayList();
  }

  /**
   * Constructor.
   *
   * @param extension initial extensions
   */
  public DFSCode(DFSExtension<C> extension) {
    this.extensions = Lists.newArrayListWithExpectedSize(1);
    this.extensions.add(extension);
  }

  /**
   * Constructor.
   *
   * @param parent parent DFS-code
   */
  public DFSCode(DFSCode<C> parent) {
    this.extensions = Lists.newArrayList(parent.getExtensions());
  }

  public List<DFSExtension<C>> getExtensions() {
    return extensions;
  }

  @Override
  public String toString() {
    return StringUtils.join(extensions, ',');
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    DFSCode code = (DFSCode) o;

    return extensions.equals(code.extensions);
  }

  @Override
  public int hashCode() {
    return extensions.hashCode();
  }

  @Override
  public int compareTo(DFSCode<C> that) {

    int comparison;

    boolean thisIsRoot = this.getExtensions().isEmpty();
    boolean thatIsRoot = that.getExtensions().isEmpty();

    if (thisIsRoot && ! thatIsRoot) {
      comparison = -1;

    } else if (thatIsRoot && !thisIsRoot) {
      comparison = 1;

    } else {
      comparison = 0;

      Iterator<DFSExtension<C>> thisIterator = this.getExtensions().iterator();
      Iterator<DFSExtension<C>> thatIterator = that.getExtensions().iterator();

      // if two DFS-Codes share initial extensions,
      // the first different extension will decide about comparison
      while (comparison == 0 && thisIterator.hasNext() && thatIterator.hasNext())
      {
        comparison = thisIterator.next().compareTo(thatIterator.next());
      }

      // DFS-Codes are equal or one is parent of other
      if (comparison == 0) {

        // this is child, cause it has further extensions
        if (thisIterator.hasNext()) {
          comparison = 1;

          // that is child, cause it has further extensions
        } else if (thatIterator.hasNext()) {
          comparison = -1;

        }
      }
    }

    return comparison;
  }
}
