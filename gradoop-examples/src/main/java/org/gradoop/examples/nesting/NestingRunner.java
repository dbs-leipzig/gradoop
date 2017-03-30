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

package org.gradoop.examples.nesting;

import org.apache.commons.cli.CommandLine;
import org.apache.flink.api.common.ProgramDescription;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.examples.AbstractRunner;
import org.gradoop.flink.model.impl.GraphCollection;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.operators.nest.NestingWithDisjunctive;

/**
 * A dedicated program for Graph Nesting over the EPGM model.
 */
public class NestingRunner extends AbstractRunner implements
  ProgramDescription {

  /**
   * Main program to run the example. Arguments are the available options.
   *
   * @param args program arguments
   * @throws Exception
   */
  @SuppressWarnings("unchecked")
  public static void main(String[] args) throws Exception {
    // read arguments from command line
    final String inputPath = args[0];
    final String collectionPath = args[1];
    final String outputPath = args[2];

    // initialize graphs
    LogicalGraph graphDatabase = readLogicalGraph(inputPath,  true);
    GraphCollection graphCollection = readGraphCollection(collectionPath, true);

    LogicalGraph nestedGraph = new NestingWithDisjunctive(GradoopId.get())
      .execute(graphDatabase, graphCollection);

    if (nestedGraph != null) {
      writeLogicalGraph(nestedGraph, outputPath);
    } else {
      System.err.println("wrong parameter constellation");
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getDescription() {
    return NestingRunner.class.getName();
  }
}
