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
package org.gradoop.flink.model.impl.operators.matching.single.cypher.utils;

import com.google.common.collect.Lists;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.embeddings.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.embeddings.IdEntry;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.embeddings.IdListEntry;

import java.util.List;

/**
 * Represents an intermediate result for the expand operator
 */
public class ExpandIntermediateResult extends Tuple3<Embedding, GradoopId[], GradoopId> {

  /**
   * Create a new ExpandIntermediateResult
   */
  public ExpandIntermediateResult() {
    super();
  }

  /**
   * Create a new expand intermediate result
   * @param base the base part
   * @param path the path along we expanded
   */
  public ExpandIntermediateResult(Embedding base, GradoopId[] path) {
    super(base, ArrayUtils.subarray(path, 0, path.length - 1), path[path.length - 1]);
  }

  /**
   * Returns the base part
   * @return the base part
   */
  public Embedding getBase() {
    return f0;
  }

  /**
   * Returns the path
   * @return the path
   */
  public GradoopId[] getPath() {
    return f1;
  }

  /**
   * Returns the end element
   * @return the end element
   */
  public GradoopId getEnd() {
    return f2;
  }

  /**
   * Expand along the given edge
   * @param edge the edge along which we expand
   * @return new expanded intermediate result
   */
  public ExpandIntermediateResult grow(Embedding edge) {
    List<GradoopId> newPath = Lists.newArrayList(f1);
    newPath.addAll(Lists.newArrayList(f2, edge.getEntry(1).getId(), edge.getEntry(2).getId()));

    return new ExpandIntermediateResult(
      f0,
      ArrayUtils.addAll(f1, f2, edge.getEntry(1).getId(), edge.getEntry(2).getId())
    );
  }

  /**
   * Size of the path
   * @return path size
   */
  public int pathSize() {
    return f1.length;
  }

  /**
   * Turns the intermediate result into an embedding
   * @return embedding representation of the expand intermediate result
   */
  public Embedding toEmbedding() {
    Embedding embedding = new Embedding();

    embedding.addEntries(f0.getEntries());
    embedding.addEntry(new IdListEntry(Lists.newArrayList(f1)));
    embedding.addEntry(new IdEntry(f2));

    return embedding;
  }
}
