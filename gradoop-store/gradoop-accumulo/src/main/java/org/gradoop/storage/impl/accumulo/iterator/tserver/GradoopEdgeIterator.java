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
package org.gradoop.storage.impl.accumulo.iterator.tserver;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.util.Pair;
import org.gradoop.common.model.api.entities.EPGMEdge;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.EdgeFactory;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.storage.impl.accumulo.constants.AccumuloTables;
import org.gradoop.storage.utils.KryoUtils;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Map;
import java.util.Objects;

/**
 * Accumulo gradoop edges iterator
 */
public class GradoopEdgeIterator extends BaseElementIterator<EPGMEdge> {

  /**
   * Edge factory
   */
  private final EdgeFactory factory = new EdgeFactory();

  @Nonnull
  @Override
  public Edge fromRow(@Nonnull Map.Entry<Key, Value> pair) throws IOException {
    //map from serialize content
    Edge content = KryoUtils.loads(pair.getValue().get(), Edge.class);
    content.setId(GradoopId.fromString(pair.getKey().getRow().toString()));
    //read from content
    return content;
  }

  @Nonnull
  @Override
  public Pair<Key, Value> toRow(@Nonnull EPGMEdge record) throws IOException {
    //write to content
    return new Pair<>(new Key(record.getId().toString()),
      new Value(KryoUtils.dumps(factory.initEdge(
        record.getId(),
        record.getLabel(),
        record.getSourceId(),
        record.getTargetId(),
        record.getProperties(),
        record.getGraphIds()))));
  }

  /**
   * Read read next element definition from accumulo store.
   * Return null if next element does not fulfill filter formula
   *
   * @param source origin store source
   * @return decoded epgm element
   */
  @Nullable
  public Edge readLine(
    @Nonnull SortedKeyValueIterator<Key, Value> source
  ) throws IOException {
    Edge row = new Edge();
    if (!source.hasTop()) {
      return null;
    }
    row.setId(GradoopId.fromString(source.getTopKey().getRow().toString()));
    row.setGraphIds(new GradoopIdSet());
    while (source.hasTop()) {
      Key key = source.getTopKey();
      Value value = source.getTopValue();

      if (!Objects.equals(row.getId().toString(), key.getRow().toString())) {
        break;
      }

      switch (key.getColumnFamily().toString()) {
      case AccumuloTables.KEY.LABEL:
        row.setLabel(value.toString());
        break;
      case AccumuloTables.KEY.SOURCE:
        row.setSourceId(GradoopId.fromString(value.toString()));
        break;
      case AccumuloTables.KEY.TARGET:
        row.setTargetId(GradoopId.fromString(value.toString()));
        break;
      case AccumuloTables.KEY.PROPERTY:
        row.setProperty(
          key.getColumnQualifier().toString(),
          PropertyValue.fromRawBytes(value.get())
        );
        break;
      case AccumuloTables.KEY.GRAPH:
        row.getGraphIds().add(GradoopId.fromString(key.getColumnQualifier().toString()));
        break;
      default:
        break;
      }
      source.next();
    }

    return row.getLabel() == null ? null : row;
  }
}
