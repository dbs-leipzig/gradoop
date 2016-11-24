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

import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.embeddings.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.embeddings.IdEntry;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.embeddings.IdListEntry;

import java.util.List;

public class ExpandEmbedding {
  private final Embedding base;
  private final List<GradoopId> path;

  public ExpandEmbedding(Embedding base, List<GradoopId> path) {
    this.base = base;
    this.path = path;
  }

  public void grow(List<GradoopId> ids) {
    path.addAll(ids);
  }

  public Embedding toEmbedding() {
    Embedding embedding = new Embedding();

    embedding.addEntries(base.getEntries());
    embedding.addEntry(new IdListEntry(path.subList(0,path.size())));
    embedding.addEntry(new IdEntry(path.get(path.size() - 1)));

    return embedding;
  }
}
