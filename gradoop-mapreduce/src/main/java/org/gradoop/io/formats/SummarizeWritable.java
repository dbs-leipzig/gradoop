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
 * along with Gradoop.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.io.formats;

import com.google.common.collect.Lists;
import org.apache.hadoop.io.Writable;
import org.gradoop.model.Edge;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

/**
 *
 */
public class SummarizeWritable implements Writable {

  /**
   * vertexIdentifier
   */
  private Long vertexIdentifier;

  /**
   * targets
   */
  private List<Long> targets;

  /**
   * Default constructor needed for deserialization.
   */
  public SummarizeWritable() {
  }

  /**
   * Creates a SummarizeWritable based on the given Params
   *
   * @param vertexID vertexID
   * @param edges    all outgoing edges
   */
  public SummarizeWritable(Long vertexID, List<Edge> edges) {
    this.vertexIdentifier = vertexID;
    this.targets = Lists.newArrayListWithExpectedSize(edges.size());
    if (edges.size() != 0) {
      for (Edge edge : edges) {
        targets.add(edge.getOtherID());
      }
    }
  }

  public Long getVertexIdentifier() {
    return vertexIdentifier;
  }

  public List<Long> getTargets() {
    return targets;
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    dataOutput.writeLong(vertexIdentifier);
    dataOutput.writeInt(targets.size());
    for (Long tar : targets) {
      dataOutput.writeLong(tar);
    }
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    this.vertexIdentifier = dataInput.readLong();
    int arrayLengh = dataInput.readInt();
    if (arrayLengh > 0) {
      this.targets = Lists.newArrayListWithExpectedSize(arrayLengh);
      for (int i = 0; i < arrayLengh; i++) {
        targets.add(dataInput.readLong());
      }
    }
  }
}
