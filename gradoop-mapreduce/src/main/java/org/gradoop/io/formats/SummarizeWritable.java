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
   * @param edges    all edges
   */
  public SummarizeWritable(Long vertexID, List<Edge> edges) {
    this.vertexIdentifier = vertexID;
    this.targets = Lists.newArrayListWithExpectedSize(edges.size());
    if (edges.size() != 0) {
      for (int i = 0; i < edges.size(); i++) {
        targets.add(edges.get(i).getOtherID());
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
