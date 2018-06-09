/**
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

package org.gradoop.common.storage.impl.accumulo.iterator.tserver;

import javafx.util.Pair;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.gradoop.common.storage.impl.accumulo.row.ElementRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

/**
 * accumulo iterator which will be execute from tserver
 * @param <T> gradoop element
 */
public abstract class BaseElementIterator<T extends ElementRow> implements
  SortedKeyValueIterator<Key, Value> {

  /**
   * iterator logger
   */
  protected static final Logger LOG = LoggerFactory.getLogger(BaseElementIterator.class);

  /**
   * origin accumulo data source
   */
  private SortedKeyValueIterator<Key, Value> source;

  /**
   * mapped element iterator
   */
  private Iterator<T> seekIterator;

  /**
   * serialized top element
   */
  private Pair<Key, Value> top;

  /**
   * deserialize from key-value pair
   * @param pair k-v pair from accumulo row
   * @return gradoop element instance
   */
  @Nonnull
  public abstract T fromRow(@Nonnull Map.Entry<Key, Value> pair);

  /**
   * serialize record to key-value pair
   * @param record gradoop element instance
   * @return k-v pair as accumulo row
   */
  @Nonnull
  public abstract Pair<Key, Value> toRow(@Nonnull T record) throws IOException;

  /**
   * do seek from accumulo store implements
   * @param source store source
   * @param range seek range
   * @return element instance list
   */
  @Nonnull
  public abstract Iterator<T> doSeek(
    /*origin iterator source*/SortedKeyValueIterator<Key, Value> source,
    /*origin seek range*/
    Range range
  ) throws IOException;

  @Override
  public void init(
    /*origin iterator source*/final SortedKeyValueIterator<Key, Value> source,
    /*option*/
    final Map<String, String> options,
    /*iterator env*/
    final IteratorEnvironment env
  ) {
    this.source = source;
  }

  @Override
  public boolean hasTop() {
    return top != null;
  }

  @Override
  public void next() throws IOException {
    T topElement = seekIterator.hasNext() ? seekIterator.next() : null;
    top = topElement == null ? null : toRow(topElement);
  }

  @Override
  public void seek(
    /*origin seek range*/final Range range,
    /*cf include/exclude*/
    final Collection<ByteSequence> columnFamilies,
    /*include flag*/
    final boolean inclusive
  ) throws IOException {
    LOG.info("seek range {}", range);
    source.seek(range, new ArrayList<>(), false);
    seekIterator = doSeek(source, range);
    next();
  }

  @Override
  public Key getTopKey() {
    return top == null ? null : top.getKey();
  }

  @Override
  public Value getTopValue() {
    return top == null ? null : top.getValue();
  }

  @Override
  public SortedKeyValueIterator<Key, Value> deepCopy(IteratorEnvironment env) {
    throw new UnsupportedOperationException("deep copy is not supported!");
  }
}
