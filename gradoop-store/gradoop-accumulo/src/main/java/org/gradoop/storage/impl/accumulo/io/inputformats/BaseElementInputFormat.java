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
package org.gradoop.storage.impl.accumulo.io.inputformats;

import com.google.common.collect.Lists;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.flink.api.common.io.GenericInputFormat;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.core.io.GenericInputSplit;
import org.gradoop.common.model.impl.pojo.Element;
import org.gradoop.storage.impl.accumulo.constants.AccumuloTables;
import org.gradoop.storage.impl.accumulo.constants.GradoopAccumuloProperty;
import org.gradoop.storage.impl.accumulo.predicate.query.AccumuloQueryHolder;
import org.gradoop.storage.utils.GradoopAccumuloUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;


/**
 * Common Abstract {@link InputFormat} for gradoop accumulo store
 *
 * @param <T> element define in gradoop
 */
public abstract class BaseElementInputFormat<T extends Element> extends GenericInputFormat<T> {

  /**
   * serialize id
   */
  private static final int serialVersionUID = 0x1;

  /**
   * accumulo properties
   */
  private final Properties properties;

  /**
   * predicate filter
   */
  private final AccumuloQueryHolder<T> predicate;

  /**
   * accumulo batch scanner
   */
  private transient BatchScanner scanner;

  /**
   * accumulo row iterator
   */
  private transient Iterator<Map.Entry<Key, Value>> iterator;

  /**
   * Create a new input format for gradoop element
   *
   * @param properties accumulo properties
   * @param predicate scan predicate filter
   */
  BaseElementInputFormat(
    Properties properties,
    AccumuloQueryHolder<T> predicate
  ) {
    this.properties = properties;
    this.predicate = predicate;
  }

  /**
   * Invoke hook after connector initiate
   */
  protected abstract void initiate();

  /**
   * Get table by table prefix definition
   *
   * @param prefix prefix defined in store configuration
   * @return table name
   */
  protected abstract String getTableName(String prefix);

  /**
   * Attach scanner iterator setting
   *
   * @param scanner accumulo batch scanner
   * @param iteratorSettingPriority iterator priority from properties
   * @param options query options
   */
  protected abstract void attachIterator(
    BatchScanner scanner,
    int iteratorSettingPriority,
    Map<String, String> options
  );

  /**
   * Decode record instance from result entry
   * This is a pure client behavior for result decode
   *
   * @param row accumulo result
   * @return gradoop element
   */
  protected abstract T mapRow(Map.Entry<Key, Value> row) throws IOException;

  @Override
  public void open(GenericInputSplit split) throws IOException {
    super.open(split);
    try {
      String tableName = getTableName(
        GradoopAccumuloProperty.ACCUMULO_TABLE_PREFIX.get(properties));
      Authorizations auth = GradoopAccumuloProperty.ACCUMULO_AUTHORIZATIONS.get(properties);
      int batchSize = GradoopAccumuloProperty.GRADOOP_BATCH_SCANNER_THREADS.get(properties);
      int iteratorPriority = GradoopAccumuloProperty.GRADOOP_ITERATOR_PRIORITY.get(properties);

      Connector conn = GradoopAccumuloUtils.createConnector(properties);
      List<Range> ranges = GradoopAccumuloUtils.getSplits(
        split.getTotalNumberOfSplits(),
        tableName,
        properties,
        predicate == null ? null : predicate.getQueryRanges());

      Map<String, String> options = new HashMap<>();
      if (predicate != null && predicate.getReduceFilter() != null) {
        options.put(AccumuloTables.KEY_PREDICATE, predicate.getReduceFilter().encode());
      }
      if (split.getSplitNumber() + 1 > ranges.size()) {
        scanner = null;
        iterator = new ArrayList<Map.Entry<Key, Value>>().iterator();
      } else {
        scanner = conn.createBatchScanner(tableName, auth, batchSize);
        attachIterator(scanner, iteratorPriority, options);
        scanner.setRanges(Lists.newArrayList(ranges.get(split.getSplitNumber())));
        iterator = scanner.iterator();
      }

      initiate();
    } catch (AccumuloException | AccumuloSecurityException | TableNotFoundException e) {
      throw new IOException(e);
    }
  }

  @Override
  public boolean reachedEnd() {
    return !iterator.hasNext();
  }

  @Override
  public T nextRecord(T reuse) throws IOException {
    return mapRow(iterator.next());
  }

  @Override
  public void close() throws IOException {
    super.close();
    if (scanner != null) {
      scanner.close();
    }
  }
}
