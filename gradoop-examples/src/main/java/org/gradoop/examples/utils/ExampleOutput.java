/*
 * Copyright © 2014 - 2018 Leipzig University (Database Research Group)
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
package org.gradoop.examples.utils;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.CrossFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.gradoop.flink.model.api.epgm.GraphCollection;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
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
    add(caption, graph.getConfig().getGraphCollectionFactory().fromGraph(graph));
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

    DataSet<String> captionSet = collection
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
