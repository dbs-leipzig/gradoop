/*
 * Copyright © 2014 - 2019 Leipzig University (Database Research Group)
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
package org.gradoop.flink.io.impl.dot.functions;

import org.apache.flink.api.java.io.TextOutputFormat;
import org.gradoop.common.model.api.entities.EPGMElement;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.flink.model.impl.layouts.transactional.tuples.GraphTransaction;

/**
 * Base class for implementing a dot formatting.
 */
public abstract class AbstractDotFileFormat
  implements TextOutputFormat.TextFormatter<GraphTransaction> {

  /**
   * .DOT vertex identifier prefix
   */
  static final String VERTEX_ID_PREFIX = "v";

  /**
   * id that needs to be update upon changes to this class' structure.
   */
  private static final long serialVersionUID = 1L;

  /**
   * flag to print graph head information to dot
   */
  private boolean printGraphHead;



  @Override
  public String format(GraphTransaction transaction) {

    StringBuilder builder = new StringBuilder();

    //--------------------------------------------------------------------------
    // write DOT head lines and open block
    //--------------------------------------------------------------------------

    GradoopId id = transaction.getGraphHead().getId();

    builder.append("subgraph cluster_g")
      .append(id)
      .append("{\n");

    //--------------------------------------------------------------------------
    // write DOT body
    //--------------------------------------------------------------------------

    if (printGraphHead) {
      writeGraphHead(transaction, builder);
    }

    writeVertices(transaction, builder, id.toString());

    writeEdges(transaction, builder, id.toString());

    //--------------------------------------------------------------------------
    // close DOT block
    //--------------------------------------------------------------------------

    builder.append("}\n");

    return builder.toString();
  }

  /**
   * Adds graph head information to the specified builder.
   *
   * Output: label="label";
   * @param transaction graph transaction
   * @param builder string builder to append
   */
  private void writeGraphHead(GraphTransaction transaction, StringBuilder builder) {
    GraphHead graphHead = transaction.getGraphHead();

    writeLabel(builder, graphHead);

    builder.append(";\n");
  }

  /**
   * Adds vertex information to the specified builder.
   *
   * vertexId [label="label", property1="value1", ...];
   *
   * @param transaction graph transaction
   * @param builder string builder to append
   * @param suffix id suffix specific for the current {@link GraphTransaction}
   */
  abstract void writeVertices(GraphTransaction transaction, StringBuilder builder, String suffix);

  /**
   * Adds edge information to the specified builder
   *
   * sourceId->targetId [label="label", property1="value1", ...];
   *
   * @param transaction graph transaction
   * @param builder string builder to append
   * @param suffix id suffix specific for the current {@link GraphTransaction}
   */
  private void writeEdges(GraphTransaction transaction, StringBuilder builder, String suffix) {
    for (Edge edge: transaction.getEdges()) {

      builder.append(VERTEX_ID_PREFIX)
        .append(edge.getSourceId())
        .append(suffix)
        .append("->")
        .append(VERTEX_ID_PREFIX)
        .append(edge.getTargetId())
        .append(suffix)
        .append(" [");
      // write dot attributes if existent
      writeLabel(builder, edge);
      builder.append("];\n");
    }
  }

  /**
   * Writes the specified label and properties.
   *
   * @param builder string builder to append
   * @param element graph element with id, label and properties
   */
  abstract void writeLabel(StringBuilder builder, EPGMElement element);

  void setPrintGraphHead(boolean printGraphHead) {
    this.printGraphHead = printGraphHead;
  }
}
