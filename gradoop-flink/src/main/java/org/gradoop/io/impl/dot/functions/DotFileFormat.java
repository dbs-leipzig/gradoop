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


package org.gradoop.io.impl.dot.functions;

import com.google.common.collect.Lists;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.java.io.TextOutputFormat;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.impl.properties.Property;
import org.gradoop.model.impl.tuples.GraphTransaction;

import java.util.Collection;


/**
 * Converts a GraphTransaction to the following .dot format:
 * <p>
 *   digraph 0
 *   {
 *   gradoopId1 [label="person", name="Bob", age="20", ...];
 *   gradoopId2 [label="person", name="Alice", age="20", ...];
 *   gradoopID3
 *   gradoopID4
 *   gradoopId1->gradoopId2 [label="knows", since="2003", ...];
 *   gradoopId2->gradoopId1 [label="knows", since="2003", ...];
 *   gradoopId3->gradoopId4
 *   }
 * </p>
 *
 * @param <G> EPGM graph head type
 * @param <V> EPGM vertex type
 * @param <E> EPGM edge type
 */
public class DotFileFormat
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  implements TextOutputFormat.TextFormatter<GraphTransaction<G, V, E>> {
  /**
   * .DOT header string
   */
  private static final String DOT_DIGRAPH_HEADER = "digraph ";
  /**
   * .DOT block open string
   */
  private static final String DOT_BLOCK_OPEN = "{";
  /**
   * .DOT block close string
   */
  private static final String DOT_BLOCK_CLOSE = "}";
  /**
   * .DOT directed edge string
   */
  private static final String DOT_OUT_EDGE = "->";
  /**
   * .DOT attributes open string
   */
  private static final String DOT_ATTRIBUTES_OPEN = " [label=\"";
  /**
   * .DOT attributes close string
   */
  private static final String DOT_ATTRIBUTES_CLOSE = "]";
  /**
   * .DOT line ending string
   */
  private static final String DOT_LINE_ENDING = ";";
  /**
   * .DOT attribute separator string
   */
  private static final String DOT_ATTRIBUTE_SEPARATOR = ", ";
  /**
   * .DOT attribute open string
   */
  private static final String DOT_ATTRIBUTE_OPEN = "=\"";
  /**
   * .DOT attribute close string
   */
  private static final String DOT_ATTRIBUTE_CLOSE = "\"";

  @Override
  public String format(GraphTransaction<G, V, E> transaction) {
    Collection<String> lines = Lists.newArrayList();
    //--------------------------------------------------------------------------
    // write dot head lines and open block
    //--------------------------------------------------------------------------

    String graphHeadId = transaction.getGraphHead()
      .getId().toString().replace("-", "");

    lines.add(String.format("%s%s%n%s",
      DOT_DIGRAPH_HEADER,
      graphHeadId,
      DOT_BLOCK_OPEN));

    //--------------------------------------------------------------------------
    // write vertex lines
    // gradoopId1 [label="label", property1="value1", ...];
    //--------------------------------------------------------------------------

    for (V vertex: transaction.getVertices()) {

      String vertexId = vertex.getId().toString().replace("-", "");

      String vertexLine = String.format("%s%s%s%s",
        vertexId,
        DOT_ATTRIBUTES_OPEN,
        vertex.getLabel(),
        DOT_ATTRIBUTE_CLOSE);

      StringBuilder vertexBuilder = new StringBuilder(vertexLine);

      // write properties
      if (vertex.getProperties().size() > 0) {
        for (Property property : vertex.getProperties()) {

          vertexBuilder.append(String.format("%s%s%s%s%s",
            DOT_ATTRIBUTE_SEPARATOR,
            property.getKey(),
            DOT_ATTRIBUTE_OPEN,
            property.getValue(),
            DOT_ATTRIBUTE_CLOSE));
        }
      }

      vertexBuilder.append(String.format("%s%s",
        DOT_ATTRIBUTES_CLOSE,
        DOT_LINE_ENDING));

      lines.add(vertexBuilder.toString());
    }

    //--------------------------------------------------------------------------
    // write edge lines
    // gradoopId1->gradoopId2 [label="label", property1="value1", ...];
    //--------------------------------------------------------------------------

    for (E edge: transaction.getEdges()) {

      String sourceId = edge.getSourceId().toString().replace("-", "");
      String targetId = edge.getTargetId().toString().replace("-", "");

      String edgeLine = String.format("%s%s%s%s%s%s",
        sourceId,
        DOT_OUT_EDGE,
        targetId,
        DOT_ATTRIBUTES_OPEN,
        edge.getLabel(),
        DOT_ATTRIBUTE_CLOSE);

      StringBuilder edgeBuilder = new StringBuilder(edgeLine);

      // write properties
      if (edge.getProperties().size() > 0) {
        for (Property property :edge.getProperties()) {

          edgeBuilder.append(String.format("%s%s%s%s%s",
            DOT_ATTRIBUTE_SEPARATOR,
            property.getKey(),
            DOT_ATTRIBUTE_OPEN,
            property.getValue(),
            DOT_ATTRIBUTE_CLOSE));
        }
      }

      edgeBuilder.append(String.format("%s%s",
        DOT_ATTRIBUTES_CLOSE,
        DOT_LINE_ENDING));

      lines.add(edgeBuilder.toString());
    }

    //--------------------------------------------------------------------------
    // close dot block
    //--------------------------------------------------------------------------

    lines.add(String.format("%s%n", DOT_BLOCK_CLOSE));
    return StringUtils.join(lines, "\n") + "\n";
  }
}
