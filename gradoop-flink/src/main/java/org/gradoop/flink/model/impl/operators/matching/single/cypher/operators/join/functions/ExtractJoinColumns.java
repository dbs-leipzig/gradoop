/*
 * This file is part of Gradoop.
 *
 * Gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Gradoop. If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.join.functions;

import org.apache.flink.api.java.functions.KeySelector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.common.pojos.Embedding;

import java.util.List;

/**
 * Computes a combines hash value from given columns.
 */
public class ExtractJoinColumns implements KeySelector<Embedding, byte[]> {
  /**
   * Columns to create hash code from.
   */
  private final List<Integer> columns;
  /**
   * Number of columns to select
   */
  private final int columnCount;
  /**
   * Resulting column values
   */
  private final byte[] keys;

  /**
   * Creates the key selector
   *
   * @param columns columns to create hash code from
   */
  public ExtractJoinColumns(List<Integer> columns) {
    this.columns     = columns;
    this.columnCount = columns.size();
    this.keys        = new byte[columnCount * GradoopId.ID_SIZE];
  }

  /**
   * Returns the internal representation.
   *
   * This getter is necessary since the find bugs annotation is not respected when added to getKey
   *
   * @return the internal byte array
   */
  @SuppressWarnings("EI_EXPOSE_REP")
  private byte[] getKeysInternal() {
    return keys;
  }

  /**
   * Combines the selected columns to a byte array and returns it.
   *
   * Note, that since the byte array is never modified, we can suppress the findbugs error here.
   *
   * @param embedding embedding to select key from
   * @return key possibly representing multiple columns in the embedding
   * @throws Exception
   */
  @Override
  public byte[] getKey(Embedding embedding) throws Exception {
    byte[] idBytes; // stores the raw bytes of a single GradoopId
    int offset; // offset to write in the final byte array
    int k; // index to access the single bytes in idBytes

    for (int i = 0; i < columnCount; i++) {
      idBytes = embedding.getEntry(columns.get(i)).getId().getRawBytes();
      offset = i * GradoopId.ID_SIZE;
      k = 0;
      for (int j = offset; j < offset + GradoopId.ID_SIZE; j++) {
        this.keys[j] = idBytes[k++];
      }
    }
    return getKeysInternal();
  }
}
