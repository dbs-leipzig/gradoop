/*
 * Copyright © 2014 - 2020 Leipzig University (Database Research Group)
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
package org.gradoop.storage.accumulo.impl.io.inputformats;

import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.gradoop.common.model.api.entities.Vertex;
import org.gradoop.common.model.impl.pojo.EPGMVertex;
import org.gradoop.common.model.impl.pojo.EPGMVertexFactory;
import org.gradoop.storage.accumulo.impl.iterator.tserver.GradoopVertexIterator;
import org.gradoop.storage.accumulo.impl.predicate.query.AccumuloQueryHolder;
import org.gradoop.storage.accumulo.impl.constants.AccumuloTables;
import org.gradoop.storage.accumulo.impl.handler.AccumuloVertexHandler;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;

/**
 * gradoop accumulo vertex input format
 */
public class VertexInputFormat extends BaseInputFormat<EPGMVertex> {

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
  private transient AccumuloVertexHandler handler;

  /**
   * vertex input format constructor
   *
   * @param properties accumulo properties
   * @param predicate accumulo query predicate
   */
  public VertexInputFormat(
    @Nonnull Properties properties,
    @Nullable AccumuloQueryHolder<EPGMVertex> predicate
  ) {
    super(properties, predicate);
  }

  @Override
  protected void initiate() {
    iterator = new GradoopVertexIterator();
    handler = new AccumuloVertexHandler(new EPGMVertexFactory());
  }

  @Override
  protected String getTableName(String prefix) {
    return String.format("%s%s", prefix, AccumuloTables.VERTEX);
  }

  @Override
  protected void attachIterator(
    BatchScanner scanner,
    int iteratorSettingPriority,
    Map<String, String> options
  ) {
    scanner.addScanIterator(
      new IteratorSetting(iteratorSettingPriority, GradoopVertexIterator.class, options));
  }

  @Override
  protected EPGMVertex mapRow(Map.Entry<Key, Value> row) throws IOException {
    Vertex decoded = iterator.fromRow(row);
    return handler.readRow(decoded);
  }

}
