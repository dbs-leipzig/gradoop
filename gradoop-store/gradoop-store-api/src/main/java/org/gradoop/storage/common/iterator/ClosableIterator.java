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
package org.gradoop.storage.common.iterator;

import javax.annotation.Nonnull;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Common Closable Iterator.
 *
 * @param <E> element template
 */
public interface ClosableIterator<E> extends Closeable, Iterator<E> {

  /**
   * Read elements from closable iterator to list,
   * both reading end or reading to max block size will return
   *
   * @param size max reading size
   * @return read elements result
   */
  @Nonnull
  default List<E> read(int size) {
    List<E> result = new ArrayList<>();
    for (int i = 0; i < size && hasNext(); i++) {
      result.add(next());
    }
    return result;
  }

  /**
   * Read all remains element in closable iterator and close
   *
   * @return read elements result
   */
  @Nonnull
  default List<E> readRemainsAndClose() {
    try {
      return read(Integer.MAX_VALUE);
    } finally {
      try {
        close();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

}
