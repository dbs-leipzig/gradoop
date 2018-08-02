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
import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.mapred.AccumuloRowInputFormat;
import org.apache.accumulo.core.client.mapred.RangeInputSplit;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.flink.api.common.io.GenericInputFormat;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.core.io.GenericInputSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.gradoop.common.model.impl.pojo.Element;
import org.gradoop.storage.config.GradoopAccumuloConfig;
import org.gradoop.storage.impl.accumulo.constants.AccumuloDefault;
import org.gradoop.storage.impl.accumulo.constants.AccumuloTables;
import org.gradoop.storage.impl.accumulo.predicate.query.AccumuloQueryHolder;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Common Abstract {@link InputFormat} for gradoop accumulo store
 *
 * @param <T> element define in gradoop
 */
public abstract class BaseInputFormat<T extends Element> extends GenericInputFormat<T> {

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
  BaseInputFormat(
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
      String user = (String) properties
        .getOrDefault(GradoopAccumuloConfig.ACCUMULO_USER, AccumuloDefault.USER);
      String password = (String) properties
        .getOrDefault(GradoopAccumuloConfig.ACCUMULO_PASSWD, AccumuloDefault.PASSWORD);
      String instance = (String) properties
        .getOrDefault(GradoopAccumuloConfig.ACCUMULO_INSTANCE, AccumuloDefault.INSTANCE);
      String zkHosts = (String) properties
        .getOrDefault(GradoopAccumuloConfig.ZOOKEEPER_HOSTS, AccumuloDefault.INSTANCE);
      String tableName = getTableName((String) properties
        .getOrDefault(GradoopAccumuloConfig.ACCUMULO_TABLE_PREFIX, AccumuloDefault.TABLE_PREFIX));
      Authorizations auth = (Authorizations) properties
        .getOrDefault(GradoopAccumuloConfig.ACCUMULO_AUTHORIZATIONS,
          AccumuloDefault.AUTHORIZATION);
      int batchSize = (int) properties
        .getOrDefault(GradoopAccumuloConfig.GRADOOP_BATCH_SCANNER_THREADS,
          AccumuloDefault.BATCH_SCANNER_THREADS);
      int iteratorPriority = (int) properties
        .getOrDefault(GradoopAccumuloConfig.GRADOOP_ITERATOR_PRIORITY,
          AccumuloDefault.ITERATOR_PRIORITY);

      Connector conn = new ZooKeeperInstance(instance, zkHosts)
        .getConnector(user, new PasswordToken(password));

      List<Range> ranges = doSplits(
        split.getTotalNumberOfSplits(),
        tableName,
        user,
        password,
        instance,
        zkHosts,
        auth);

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

  /**
   * Split table into ranges according to {@link AccumuloRowInputFormat#getSplits} suggest
   *
   * @param maxSplit max split size
   * @param tableName split table name
   * @param user accumulo user
   * @param password accumulo password
   * @param instance accumulo instance name
   * @param zkHosts zookeeper hosts
   * @param auth accumulo access authorization
   * @return split range collections
   */
  @Nonnull
  private List<Range> doSplits(
    int maxSplit,
    @Nonnull String tableName,
    @Nonnull String user,
    @Nonnull String password,
    @Nonnull String instance,
    @Nonnull String zkHosts,
    @Nonnull Authorizations auth
  ) throws IOException, AccumuloSecurityException {
    AccumuloRowInputFormat format = new AccumuloRowInputFormat();
    JobConf conf = new JobConf();
    AccumuloRowInputFormat.setInputTableName(conf, tableName);
    AccumuloRowInputFormat.setConnectorInfo(conf, user, new PasswordToken(password));
    AccumuloRowInputFormat.setZooKeeperInstance(conf, ClientConfiguration.create()
      .withInstance(instance)
      .withZkHosts(zkHosts));
    AccumuloRowInputFormat.setScanAuthorizations(conf, auth);
    if (predicate != null && predicate.getQueryRanges() != null) {
      AccumuloRowInputFormat.setRanges(conf, predicate.getQueryRanges());
    }
    InputSplit[] splits = format.getSplits(conf, maxSplit);
    return Stream.of(splits)
      .map(it -> (RangeInputSplit) it)
      .map(RangeInputSplit::getRange)
      .collect(Collectors.toList());
  }

}
