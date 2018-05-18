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

import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.EdgeFactory;
import org.gradoop.common.storage.impl.accumulo.constants.AccumuloTables;
import org.gradoop.common.storage.impl.accumulo.handler.AccumuloEdgeHandler;
import org.gradoop.common.storage.impl.accumulo.iterator.tserver.GradoopEdgeIterator;
import org.gradoop.common.storage.impl.accumulo.row.EdgeRow;

import java.util.Map;
import java.util.Properties;

/**
 * gradoop edge input format
 */
public class EdgeInputFormat extends BaseInputFormat<Edge> {

  /**
   * serialize id
   */
  private static final int serialVersionUID = 0x1;

  /**
   * gradoop graph head iterator(for row decode)
   */
  private transient GradoopEdgeIterator iterator;

  /**
   * graph head handler
   */
  private transient AccumuloEdgeHandler<Edge> handler;

  /**
   * edge input format constructor
   *
   * @param properties accumulo properties
   */
  public EdgeInputFormat(Properties properties) {
    super(properties);
  }

  @Override
  protected void initiate() {
    iterator = new GradoopEdgeIterator();
    handler = new AccumuloEdgeHandler<>(new EdgeFactory());
  }

  @Override
  protected String getTableName(String prefix) {
    return String.format("%s%s", prefix, AccumuloTables.EDGE);
  }

  @Override
  protected void attachIterator(
    BatchScanner scanner,
    int iteratorSettingPriority
  ) {
    scanner.addScanIterator(
      new IteratorSetting(iteratorSettingPriority, GradoopEdgeIterator.class));
  }

  @Override
  protected Edge mapRow(Map.Entry<Key, Value> row) {
    EdgeRow decoded = iterator.fromRow(row);
    return handler.readRow(GradoopId.fromString(row.getKey().getRow().toString()), decoded);
  }

}
