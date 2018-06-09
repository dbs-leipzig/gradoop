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

package org.gradoop.flink.io.impl.accumulo.inputformats;

import com.google.common.collect.Lists;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.flink.api.common.io.GenericInputFormat;
import org.apache.flink.core.io.GenericInputSplit;
import org.apache.hadoop.io.Text;
import org.gradoop.common.config.GradoopAccumuloConfig;
import org.gradoop.common.model.impl.pojo.Element;
import org.gradoop.common.storage.impl.accumulo.constants.AccumuloDefault;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * abstract input format for accumulo data source
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
   * accumulo client connector
   */
  private transient Connector conn;

  /**
   * accumulo batch scanner
   */
  private transient BatchScanner scanner;

  /**
   * accumulo row iterator
   */
  private transient Iterator<Map.Entry<Key, Value>> iterator;

  /**
   * bas input format constructor
   *
   * @param properties accumulo properties
   */
  BaseInputFormat(Properties properties) {
    this.properties = properties;
  }

  /**
   * after connector initiate
   */
  protected abstract void initiate();

  /**
   * get table name with prefix configuration
   *
   * @param prefix prefix configuration
   * @return table name
   */
  protected abstract String getTableName(String prefix);

  /**
   * attach iterator setting
   *
   * @param scanner accumulo batch scanner
   * @param iteratorSettingPriority iterator priority from properties
   */
  protected abstract void attachIterator(
    BatchScanner scanner,
    int iteratorSettingPriority
  );

  /**
   * decode record from remote
   *
   * @param row accumulo result
   * @return gradoop element
   */
  protected abstract T mapRow(Map.Entry<Key, Value> row);

  @Override
  public void open(GenericInputSplit split) throws IOException {
    super.open(split);
    try {
      conn = new ZooKeeperInstance(
        /*instannce*/
        (String) properties
          .getOrDefault(GradoopAccumuloConfig.ACCUMULO_INSTANCE, AccumuloDefault.INSTANCE),
        /*zookeepers*/
        (String) properties
          .getOrDefault(GradoopAccumuloConfig.ZOOKEEPER_HOSTS, AccumuloDefault.ZOOKEEPERS))
        .getConnector(
          /*user*/
          (String) properties
            .getOrDefault(GradoopAccumuloConfig.ACCUMULO_USER, AccumuloDefault.USER),
          /*password*/
          new PasswordToken((String) properties
            .getOrDefault(GradoopAccumuloConfig.ACCUMULO_PASSWD, AccumuloDefault.PASSWORD)));

      String tableName = /*table name*/getTableName((String) properties
        .getOrDefault(GradoopAccumuloConfig.ACCUMULO_TABLE_PREFIX, AccumuloDefault.TABLE_PREFIX));

      List<Range> ranges = doSplits(split.getTotalNumberOfSplits(), tableName);
      if (split.getSplitNumber() + 1 > ranges.size()) {
        scanner = null;
        iterator = new ArrayList<Map.Entry<Key, Value>>().iterator();
      } else {
        scanner = conn.createBatchScanner(tableName,
          /*authorizations*/
          (Authorizations) properties.getOrDefault(GradoopAccumuloConfig.ACCUMULO_AUTHORIZATIONS,
            AccumuloDefault.AUTHORIZATION),
          /*batch scanner threads*/
          (int) properties.getOrDefault(GradoopAccumuloConfig.GRADOOP_BATCH_SCANNER_THREADS,
            AccumuloDefault.BATCH_SCANNER_THREADS));
        attachIterator(scanner,
          /*iterator priority*/
          (int) properties.getOrDefault(GradoopAccumuloConfig.GRADOOP_ITERATOR_PRIORITY,
            AccumuloDefault.ITERATOR_PRIORITY));
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
  public T nextRecord(T reuse) {
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
   * split table range by accumulo tserver
   *
   * @param maxSplit max split size
   * @param tableName split table name
   * @return split range collections
   * @throws TableNotFoundException if table not exists
   * @throws AccumuloSecurityException bad authorizations
   * @throws AccumuloException other error
   */
  @Nonnull
  private List<Range> doSplits(
    int maxSplit,
    String tableName
  ) throws TableNotFoundException, AccumuloSecurityException, AccumuloException {
    TableOperations operations = conn.tableOperations();
    Collection<Range> range = operations.splitRangeByTablets(tableName,
      new Range((Text) null, null), maxSplit);
    return range.stream().sorted(Comparator.comparing(Range::getStartKey)).collect(
      Collectors.toList());
  }
}
