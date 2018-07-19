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
package org.gradoop.storage.impl.accumulo.iterator.tserver;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.util.Pair;
import org.gradoop.common.model.api.entities.EPGMElement;
import org.gradoop.storage.impl.accumulo.constants.AccumuloTables;
import org.gradoop.storage.impl.accumulo.predicate.filter.api.AccumuloElementFilter;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

/**
 * Accumulo Tablet Server Iterator
 * This Iterator will be created in accumulo tablet server runtime, when executing a partition
 * range query. A Gradoop Element iterator will decode query options as query filter, transform
 * multi-rows into epgm element and check if this element should be return by predicate. Each
 * element that fulfill the predicate will be serialized into one row.
 *
 * @param <E> gradoop epgm element
 * @see <a href="https://accumulo.apache.org/1.9/accumulo_user_manual.html#_iterator_design">
 *   accumulo iterator design</a>
 */
public abstract class BaseElementIterator<E extends EPGMElement> implements
  SortedKeyValueIterator<Key, Value> {

  /**
   * Origin accumulo data source
   */
  private SortedKeyValueIterator<Key, Value> source;

  /**
   * Mapped element iterator, which will handle "next fulfilled element" instrumentation
   */
  private InnerIterator seekIterator;

  /**
   * Serialized top row
   */
  private Pair<Key, Value> top;

  /**
   * Element filter predicate
   */
  private AccumuloElementFilter<E> filter;

  /**
   * Deserialize from key-value pair
   *
   * @param pair k-v pair from accumulo row
   * @return gradoop element instance
   */
  @Nonnull
  public abstract E fromRow(@Nonnull Map.Entry<Key, Value> pair) throws IOException;

  /**
   * Serialize record to key-value pair
   *
   * @param record gradoop element instance
   * @return k-v pair as accumulo row
   */
  @Nonnull
  public abstract Pair<Key, Value> toRow(@Nonnull E record) throws IOException;

  /**
   * Read read next element definition from accumulo store.
   * Return null if next element does not fulfill filter formula
   *
   * @param source origin store source
   * @return decoded epgm element
   */
  @Nullable
  public abstract E readLine(
    @Nonnull SortedKeyValueIterator<Key, Value> source
  ) throws IOException;

  /**
   * Get element filter predicate
   *
   * @return iterator element filter
   */
  protected AccumuloElementFilter<E> getFilter() {
    return filter;
  }

  @Override
  public void init(
    final SortedKeyValueIterator<Key, Value> source,
    final Map<String, String> options,
    final IteratorEnvironment env
  ) {
    this.source = source;
    //read filter predicate
    if (options != null && !options.isEmpty() &&
      options.containsKey(AccumuloTables.KEY_PREDICATE)) {
      this.filter = AccumuloElementFilter.decode(options.get(AccumuloTables.KEY_PREDICATE));
    } else {
      this.filter = (AccumuloElementFilter<E>) t -> true;
    }
  }

  @Override
  public boolean hasTop() {
    return top != null;
  }

  @Override
  public void next() throws IOException {
    E topElement = seekIterator.hasNext() ? seekIterator.next() : null;
    top = topElement == null ? null : toRow(topElement);
  }

  @Override
  public void seek(
    final Range range,
    final Collection<ByteSequence> columnFamilies,
    final boolean inclusive
  ) throws IOException {
    //LOG.info("seek range {}", range);
    source.seek(range, new ArrayList<>(), false);
    seekIterator = new InnerIterator();
    next();
  }

  @Override
  public Key getTopKey() {
    return top == null ? null : top.getFirst();
  }

  @Override
  public Value getTopValue() {
    return top == null ? null : top.getSecond();
  }

  @Override
  public SortedKeyValueIterator<Key, Value> deepCopy(IteratorEnvironment env) {
    throw new UnsupportedOperationException("deep copy is not supported!");
  }

  /**
   * Logical iterator for seeking fulfilled element
   */
  private class InnerIterator implements Iterator<E> {

    /**
     * Next element head
     */
    private E head;

    /**
     * Create a new inner iterator instance
     */
    private InnerIterator() {
      this.head = readHead();
    }

    /**
     * Read next head element
     *
     * @return edge row head
     */
    private E readHead() {
      E next;
      do {
        try {
          next = readLine(source);
          if (next != null && !getFilter().test(next)) {
            next = null;
          }
        } catch (IOException err) {
          throw new RuntimeException(err);
        }
      } while (source.hasTop() && next == null);
      return next;
    }

    @Override
    public boolean hasNext() {
      return head != null;
    }

    @Override
    public E next() {
      E result = head;
      head = readHead();
      return result;
    }
  }

}
