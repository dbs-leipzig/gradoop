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

package org.gradoop.flink.model.impl.operators.matching.single.cypher.embeddings;

import java.util.ArrayList;
import java.util.List;

public class Embedding {
  private List<EmbeddingEntry> entries;

  public Embedding() {
    entries = new ArrayList<>();
  }

  public EmbeddingEntry getEntry(int index) {
    return entries.get(index);
  }

  public void addEntry(EmbeddingEntry entry) {
    entries.add(entry);
  }

  public void setEntry(Integer column, EmbeddingEntry entry) { entries.set(column,entry); }

  public int size() {
    return entries.size();
  }
}
