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
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.pojo.VertexFactory;
import org.gradoop.common.storage.impl.accumulo.constants.AccumuloTables;
import org.gradoop.common.storage.impl.accumulo.handler.AccumuloVertexHandler;
import org.gradoop.common.storage.impl.accumulo.iterator.tserver.GradoopVertexIterator;
import org.gradoop.common.storage.impl.accumulo.row.VertexRow;

import java.util.Map;
import java.util.Properties;

/**
 * gradoop accumulo vertex input format
 */
public class VertexInputFormat extends BaseInputFormat<Vertex> {

  /**
   * serialize id
   */
  private static final int serialVersionUID = 0x1;

  /**
   * gradoop vertex iterator(for row decode)
   */
  private transient GradoopVertexIterator iterator;

  /**
   * graph head handler
   */
  private transient AccumuloVertexHandler<Vertex> handler;

  /**
   * vertex input format constructor
   *
   * @param properties accumulo properties
   */
  public VertexInputFormat(Properties properties) {
    super(properties);
  }

  @Override
  protected void initiate() {
    iterator = new GradoopVertexIterator();
    handler = new AccumuloVertexHandler<>(new VertexFactory());
  }

  @Override
  protected String getTableName(String prefix) {
    return String.format("%s%s", prefix, AccumuloTables.VERTEX);
  }

  @Override
  protected void attachIterator(
    BatchScanner scanner,
    int iteratorSettingPriority
  ) {
    scanner.addScanIterator(
      new IteratorSetting(iteratorSettingPriority, GradoopVertexIterator.class));
  }

  @Override
  protected Vertex mapRow(Map.Entry<Key, Value> row) {
    VertexRow decoded = iterator.fromRow(row);
    return handler.readRow(GradoopId.fromString(row.getKey().getRow().toString()), decoded);
  }

}
