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
package org.gradoop.storage.impl.accumulo.iterator.client;

import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.gradoop.common.model.api.entities.EPGMElement;
import org.gradoop.common.model.impl.pojo.Element;
import org.gradoop.storage.common.api.EPGMGraphOutput;
import org.gradoop.storage.common.api.EPGMGraphPredictableOutput;
import org.gradoop.storage.common.iterator.ClosableIterator;
import org.gradoop.storage.impl.accumulo.handler.AccumuloRowHandler;
import org.gradoop.storage.impl.accumulo.iterator.tserver.BaseElementIterator;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Accumulo client closable iterator.
 * This Iterator will be created in client runtime, when executing a store output query. A client
 * closable iterator will transform accumulo result entry into required EPGM element type. The
 * query result will always be fetch block by block for better performance. And the
 * block size for result is called cache size.
 *
 * @param <R> EPGM Element type for reading result
 * @param <E> EPGM Element type for reading source (remote definition)
 * @see EPGMGraphOutput
 * @see EPGMGraphPredictableOutput
 */
public class ClientClosableIterator<R extends Element, E extends EPGMElement>
  implements ClosableIterator<R> {

  /**
   * accumulo batch scanner instance
   */
  private final BatchScanner scanner;

  /**
   * scanner iterator
   */
  private final Iterator<Map.Entry<Key, Value>> inner;

  /**
   * row decoder
   */
  private final BaseElementIterator<E> codec;

  /**
   * max record cache size
   */
  private final int cacheSize;

  /**
   * accumulo row handler
   */
  private final AccumuloRowHandler<R, E> handler;

  /**
   * element cache size
   */
  private List<E> cache = new ArrayList<>();

  /**
   * cache closable iterator constructor
   *
   * @param scanner accumulo batch scanner
   * @param codec iterator decoder
   * @param handler result element row handler
   * @param cacheSize result cache size
   */
  public ClientClosableIterator(
    @Nonnull BatchScanner scanner,
    @Nonnull BaseElementIterator<E> codec,
    @Nonnull AccumuloRowHandler<R, E> handler,
    int cacheSize
  ) {
    this.codec = codec;
    this.scanner = scanner;
    this.cacheSize = cacheSize;
    this.handler = handler;
    this.inner = scanner.iterator();
  }

  @Override
  public void close() {
    scanner.close();
  }

  @Override
  public boolean hasNext() {
    if (!cache.isEmpty()) {
      //cache is not empty
      return true;

    } else if (inner.hasNext()) {
      //cache is empty, read elements to cache
      while (inner.hasNext() && cache.size() < cacheSize) {
        E next = null;
        try {
          next = codec.fromRow(inner.next());
        } catch (IOException e) {
          e.printStackTrace();
        }
        cache.add(next);
      }
      return hasNext();

    } else {
      //cache is empty and iterator has no element any more
      return false;

    }
  }

  @Override
  public R next() {
    E row = cache.remove(0);
    return handler.readRow(row);
  }

}
