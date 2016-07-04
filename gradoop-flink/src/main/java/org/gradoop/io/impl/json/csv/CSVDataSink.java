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

package org.gradoop.io.impl.json.csv;


import org.gradoop.io.api.DataSink;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.GraphTransactions;
import org.gradoop.model.impl.LogicalGraph;
import org.gradoop.util.GradoopFlinkConfig;

import java.io.IOException;

public class CSVDataSink<G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  extends CSVBase<G, V, E>
  implements DataSink<G, V, E> {

  public CSVDataSink(GradoopFlinkConfig<G, V, E> config, String csvPath) {
    super(config, csvPath);
  }

  @Override
  public void write(LogicalGraph<G, V, E> logicalGraph) throws IOException {

  }

  @Override
  public void write(GraphCollection<G, V, E> graphCollection) throws
    IOException {

  }

  @Override
  public void write(GraphTransactions<G, V, E> graphTransactions) throws
    IOException {

  }
}
