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

package org.gradoop.flink.io.reader.parsers;

import java.util.Iterator;

/**
 * Defines an interator that could be rewinded.
 * Convenience class used when two Iterables are already defined and cannot be overridden
 */
public interface ResettableIterator<K> extends Iterator<K> {
  /**
   * Iterator's initialization
   */
  void start();

  /**
   * Backwarding the iterator. To be used to implement the Iterable from the current iterator
   */
  void rewind();

  /**
   * Implementing the iterable element
   * @return Iterable element
   */
  default Iterable<K> asIterable() {
    Iterator<K> meme = this;
    rewind();
    start();
    return new Iterable<K>() {
      @Override
      public Iterator<K> iterator() {
        return meme;
      }
    };
  }
}
