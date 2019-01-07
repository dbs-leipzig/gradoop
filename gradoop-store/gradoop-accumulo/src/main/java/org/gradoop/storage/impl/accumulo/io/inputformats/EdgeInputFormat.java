/*
 * Copyright © 2014 - 2019 Leipzig University (Database Research Group)
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

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.gradoop.common.model.api.entities.EPGMEdge;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.EdgeFactory;
import org.gradoop.storage.impl.accumulo.constants.AccumuloTables;
import org.gradoop.storage.impl.accumulo.handler.AccumuloEdgeHandler;
import org.gradoop.storage.impl.accumulo.iterator.tserver.GradoopEdgeIterator;
import org.gradoop.storage.impl.accumulo.predicate.query.AccumuloQueryHolder;

import java.io.IOException;
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
  private transient AccumuloEdgeHandler handler;

  /**
   * edge input format constructor
   *
   * @param properties accumulo properties
   * @param predicate accumulo query predicate
   */
  public EdgeInputFormat(
    @NonNull Properties properties,
    @Nullable AccumuloQueryHolder<Edge> predicate
  ) {
    super(properties, predicate);
  }

  @Override
  protected void initiate() {
    iterator = new GradoopEdgeIterator();
    handler = new AccumuloEdgeHandler(new EdgeFactory());
  }

  @Override
  protected String getTableName(String prefix) {
    return String.format("%s%s", prefix, AccumuloTables.EDGE);
  }

  @Override
  protected void attachIterator(
    BatchScanner scanner,
    int iteratorSettingPriority,
    Map<String, String> options
  ) {
    scanner.addScanIterator(
      new IteratorSetting(iteratorSettingPriority, GradoopEdgeIterator.class, options));
  }

  @Override
  protected Edge mapRow(Map.Entry<Key, Value> row) throws IOException {
    EPGMEdge decoded = iterator.fromRow(row);
    return handler.readRow(decoded);
  }

}
