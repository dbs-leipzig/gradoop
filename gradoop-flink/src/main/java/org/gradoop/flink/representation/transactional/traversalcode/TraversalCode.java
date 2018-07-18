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
package org.gradoop.flink.representation.transactional.traversalcode;

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
public class TraversalCode<C extends Comparable<C>> implements Serializable, Comparable<TraversalCode<C>> {

  /**
   * Included edges.
   */
  private final List<Traversal<C>> traversals;

  /**
   * Default constructor.
   */
  public TraversalCode() {
    this.traversals = Lists.newArrayList();
  }

  /**
   * Constructor.
   *
   * @param traversal initial traversals
   */
  public TraversalCode(Traversal<C> traversal) {
    this.traversals = Lists.newArrayListWithExpectedSize(1);
    this.traversals.add(traversal);
  }

  /**
   * Constructor.
   *
   * @param parent parent DFS-code
   */
  public TraversalCode(TraversalCode<C> parent) {
    this.traversals = Lists.newArrayList(parent.getTraversals());
  }

  public List<Traversal<C>> getTraversals() {
    return traversals;
  }

  @Override
  public String toString() {
    return StringUtils.join(traversals, ',');
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    TraversalCode code = (TraversalCode) o;

    return traversals.equals(code.traversals);
  }

  @Override
  public int hashCode() {
    return traversals.hashCode();
  }

  @Override
  public int compareTo(TraversalCode<C> that) {

    int comparison;

    boolean thisIsRoot = this.getTraversals().isEmpty();
    boolean thatIsRoot = that.getTraversals().isEmpty();

    if (thisIsRoot && ! thatIsRoot) {
      comparison = -1;

    } else if (thatIsRoot && !thisIsRoot) {
      comparison = 1;

    } else {
      comparison = 0;

      Iterator<Traversal<C>> thisIterator = this.getTraversals().iterator();
      Iterator<Traversal<C>> thatIterator = that.getTraversals().iterator();

      // if two DFS-Codes share initial traversals,
      // the first different traversal will decide about comparison
      while (comparison == 0 && thisIterator.hasNext() && thatIterator.hasNext())
      {
        comparison = thisIterator.next().compareTo(thatIterator.next());
      }

      // DFS-Codes are equal or one is parent of other
      if (comparison == 0) {

        // this is child, cause it has further traversals
        if (thisIterator.hasNext()) {
          comparison = 1;

          // that is child, cause it has further traversals
        } else if (thatIterator.hasNext()) {
          comparison = -1;

        }
      }
    }

    return comparison;
  }
}
