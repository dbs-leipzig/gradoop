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

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.util.Pair;
import org.gradoop.common.model.api.entities.EPGMVertex;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.pojo.VertexFactory;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.common.storage.impl.accumulo.constants.AccumuloTables;
import org.gradoop.common.utils.KryoUtils;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;

/***
 * accumulo gradoop vertex iterator
 */
public class GradoopVertexIterator extends BaseElementIterator<EPGMVertex> {

  /**
   * vertex factory
   */
  private final VertexFactory factory = new VertexFactory();

  @Nonnull
  @Override
  public Vertex fromRow(@Nonnull Map.Entry<Key, Value> pair) throws IOException {
    //map from serialize content
    Vertex content = KryoUtils.loads(pair.getValue().get(), Vertex.class);
    content.setId(GradoopId.fromString(pair.getKey().getRow().toString()));
    //read from content
    return content;
  }

  @Nonnull
  @Override
  public Pair<Key, Value> toRow(@Nonnull EPGMVertex record) throws IOException {
    //write to content
    return new Pair<>(new Key(record.getId().toString()),
      new Value(KryoUtils.dumps(factory.initVertex(
        record.getId(),
        record.getLabel(),
        record.getProperties(),
        record.getGraphIds()))));
  }

  @Nonnull
  @Override
  public Iterator<EPGMVertex> doSeek(
    SortedKeyValueIterator<Key, Value> source,
    Range range
  ) {
    return new Iterator<EPGMVertex>() {

      private Vertex head = readHead();

      @Override
      public boolean hasNext() {
        return head != null;
      }

      @Override
      public Vertex next() {
        Vertex result = head;
        head = readHead();
        return result;
      }

      /**
       * read head element
       *
       * @return edge row head
       */
      @SuppressWarnings("Duplicates")
      private Vertex readHead() {
        Vertex next;
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
    };
  }

  /**
   * read next edge element from table store
   *
   * @param source origin accumulo source
   * @return edge element
   * @throws IOException io err
   */
  @Nullable
  private Vertex readLine(SortedKeyValueIterator<Key, Value> source) throws IOException {
    if (!source.hasTop()) {
      return null;
    }

    Vertex row = new Vertex();
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
      case AccumuloTables.KEY.PROPERTY:
        row.setProperty(key.getColumnQualifier().toString(),
          PropertyValue.fromRawBytes(value.get()));
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