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
public class EPGVertexValueWritable extends
  EPGLabeledAttributedWritable implements GraphElement {

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
   * @param label      label of that vertex (can not be {@code null})
   * @param properties key-value-map (can be {@code null})
   * @param graphs     graphs that vertex belongs to (can be {@code null} and is
   *                   stores as a set
   */
  public EPGVertexValueWritable(String label, Map<String, Object> properties,
    Iterable<Long> graphs) {
    super(label, properties);
    initGraphs(graphs);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Iterable<Long> getGraphs() {
    return this.graphs;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void addGraph(Long graph) {
    if (this.graphs.isEmpty()) {
      initGraphs();
    }
    this.graphs.add(graph);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void resetGraphs() {
    initGraphs();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void addGraphs(Iterable<Long> graphs) {
    if (this.graphs.isEmpty()) {
      initGraphs();
    }
    for (Long g : graphs) {
      this.graphs.add(g);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int getGraphCount() {
    return (graphs != null) ? graphs.size() : 0;
  }

  /**
   * Initializes the internal graph set.
   */
  private void initGraphs() {
    initGraphs(null);
  }

  /**
   * Initializes the internal graph set with the given graphs.
   *
   * @param graphs initial graphs
   */
  private void initGraphs(Iterable<Long> graphs) {
    if (graphs == null) {
      this.graphs = Sets.newHashSet();
    } else {
      this.graphs = Sets.newHashSet(graphs);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void write(DataOutput dataOutput) throws IOException {
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

  /**
   * {@inheritDoc}
   */
  @Override
  public void readFields(DataInput dataInput) throws IOException {
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
