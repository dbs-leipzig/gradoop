package org.gradoop.io.formats;

import com.google.common.collect.Sets;
import org.gradoop.model.GraphElement;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;
import java.util.Set;

/**
 * Stores the information of an EPG vertex: labels, properties and graphs that
 * vertex belongs to.
 */
public class EPGVertexValueWritable extends EPGMultiLabeledAttributedWritable
  implements GraphElement {

  /**
   * The set of graphs that vertex belongs to.
   */
  private Set<Long> graphs;

  /**
   * Default constructor is necessary for object deserialization.
   */
  public EPGVertexValueWritable() {
  }

  /**
   * Creates a vertex value based on the given parameters.
   *
   * @param labels     labels of that vertex (can be {@code null})
   * @param properties key-value-map (can be {@code null})
   * @param graphs     graphs that vertex belongs to (can be {@code null} and is
   *                   stores as a set
   */
  public EPGVertexValueWritable(Iterable<String> labels,
                                Map<String, Object> properties,
                                Iterable<Long> graphs) {
    super(labels, properties);
    initGraphs(graphs);
  }

  @Override
  public Iterable<Long> getGraphs() {
    return this.graphs;
  }

  @Override
  public void addToGraph(Long graph) {
    initGraphs();
    this.graphs.add(graph);
  }

  private void initGraphs() {
    initGraphs(null);
  }

  private void initGraphs(Iterable<Long> graphs) {
    if (graphs == null) {
      this.graphs = Sets.newHashSet();
    } else {
      this.graphs = Sets.newHashSet(graphs);
    }
  }

  @Override
  public void write(DataOutput dataOutput)
    throws IOException {
    super.write(dataOutput);
    if (graphs == null || graphs.isEmpty()) {
      dataOutput.writeInt(0);
    } else {
      dataOutput.writeInt(graphs.size());
      for (Long graph : graphs) {
        dataOutput.writeLong(graph);
      }
    }
  }

  @Override
  public void readFields(DataInput dataInput)
    throws IOException {
    super.readFields(dataInput);
    final int graphCount = dataInput.readInt();
    if (graphCount > 0) {
      initGraphs();
    }
    for (int i = 0; i < graphCount; i++) {
      graphs.add(dataInput.readLong());
    }
  }


}
