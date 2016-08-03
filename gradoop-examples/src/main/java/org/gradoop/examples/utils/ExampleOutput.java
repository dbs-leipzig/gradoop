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

package org.gradoop.examples.utils;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.CrossFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.gradoop.flink.model.impl.GraphCollection;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.operators.tostring.CanonicalAdjacencyMatrixBuilder;
import org.gradoop.flink.model.impl.operators.tostring.functions.EdgeToDataString;
import org.gradoop.flink.model.impl.operators.tostring.functions.GraphHeadToDataString;
import org.gradoop.flink.model.impl.operators.tostring.functions.VertexToDataString;

import java.util.ArrayList;

/**
 * Allows to collect and print intermediate results of example programs.
 */
public class ExampleOutput {

  /**
   * Flink dataset, collecting the output lines
   */
  private DataSet<ArrayList<String>> outSet;

  /**
   * add graph to output
   * @param caption output caption
   * @param graph graph
   */
  public void add(String caption, LogicalGraph graph) {
    add(caption, GraphCollection.fromGraph(graph));
  }

  /**
   * add collection to output
   * @param caption output caption
   * @param collection collection
   */
  public void add(String caption, GraphCollection collection) {

    if (outSet == null) {
      outSet = collection
        .getConfig().getExecutionEnvironment()
        .fromElements(new ArrayList<String>());
    }

    DataSet<String> captionSet =
      collection
        .getConfig().getExecutionEnvironment()
        .fromElements("\n*** " + caption + " ***\n");

    DataSet<String> graphStringSet =
      new CanonicalAdjacencyMatrixBuilder(
        new GraphHeadToDataString(),
        new VertexToDataString(),
        new EdgeToDataString(), true).execute(collection);

    outSet = outSet
      .cross(captionSet)
      .with(new OutputAppender())
      .cross(graphStringSet)
      .with(new OutputAppender());
  }

  /**
   * print output
   * @throws Exception
   */
  public void print() throws Exception {
    outSet
      .map(new LineCombiner())
      .print();
  }

  /**
   * Flink function to append output data set.
   */
  private static class OutputAppender implements
    CrossFunction<ArrayList<String>, String, ArrayList<String>> {

    @Override
    public ArrayList<String> cross(ArrayList<String> out, String line) throws
      Exception {

      out.add(line);

      return out;
    }
  }

  /**
   * Flink function to combine output lines.
   */
  private static class LineCombiner implements
    MapFunction<ArrayList<String>, String> {

    @Override
    public String map(ArrayList<String> lines) throws Exception {

      return StringUtils.join(lines, "\n");
    }
  }
}
