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
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.gradoop.common.storage.impl.accumulo.constants.AccumuloTables;
import org.gradoop.common.storage.impl.accumulo.row.EdgeRow;
import org.gradoop.common.utils.JsonUtils;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;

/**
 * accumulo gradoop edges iterator
 */
public class GradoopEdgeIterator extends BaseElementIterator<EdgeRow> {

  @Nonnull
  @Override
  public EdgeRow fromRow(@Nonnull Map.Entry<Key, Value> pair) {
    //TODO: use kryo instead of json for better performance
    //map from serialize content
    EdgeRow content = JsonUtils.loads(pair.getValue().get(), EdgeRow.class);
    content.setId(pair.getKey().getRow().toString());
    //read from content
    return content;
  }

  @Nonnull
  @Override
  public Pair<Key, Value> toRow(@Nonnull EdgeRow record) {
    //write to content
    return new Pair<>(new Key(record.getId()), new Value(JsonUtils.dumps(record)));
  }

  @Nonnull
  @Override
  public Iterator<EdgeRow> doSeek(
    SortedKeyValueIterator<Key, Value> source,
    Range range
  ) throws IOException {
    return new Iterator<EdgeRow>() {
      private EdgeRow head = readLine(source);

      @Override
      public boolean hasNext() {
        return head != null;
      }

      @Override
      public EdgeRow next() {
        try {
          EdgeRow next = head;
          head = readLine(source);
          return next;
        } catch (IOException e) {
          throw new IllegalStateException(e);
        }
      }
    };
  }

  /**
   * read next edge element from table store
   * @param source origin accumulo source
   * @return edge element
   * @throws IOException io err
   */
  @Nullable
  private EdgeRow readLine(SortedKeyValueIterator<Key, Value> source) throws IOException {
    EdgeRow row = new EdgeRow();
    if (!source.hasTop()) {
      return null;
    }
    row.setId(source.getTopKey().getRow().toString());
    while (source.hasTop()) {
      Key key = source.getTopKey();
      Value value = source.getTopValue();

      if (!Objects.equals(row.getId(), key.getRow().toString())) {
        break;
      }

      switch (key.getColumnFamily().toString()) {
      case AccumuloTables.KEY.LABEL:
        row.setLabel(value.toString());
        break;
      case AccumuloTables.KEY.SOURCE:
        row.setSource(value.toString());
        break;
      case AccumuloTables.KEY.TARGET:
        row.setTarget(value.toString());
        break;
      case AccumuloTables.KEY.PROPERTY:
        row.getProperties().put(key.getColumnQualifier().toString(), value.toString());
        break;
      case AccumuloTables.KEY.GRAPH:
        row.getGraphs().add(key.getColumnQualifier().toString());
        break;
      default:
        break;
      }
      source.next();
    }

    LOG.info(String.format("[edge]readLine=>%s", JsonUtils.dumps(row)));
    return row;
  }
}
