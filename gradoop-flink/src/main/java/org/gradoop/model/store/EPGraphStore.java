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

package org.gradoop.model.store;

import org.gradoop.model.EdgeData;
import org.gradoop.model.GraphData;
import org.gradoop.model.VertexData;
import org.gradoop.model.impl.EPGraph;
import org.gradoop.model.impl.EPGraphCollection;

public interface EPGraphStore<VD extends VertexData, ED extends EdgeData, GD
  extends GraphData> {

  EPGraph<VD, ED, GD> getDatabaseGraph();

  EPGraphCollection<VD, ED, GD> getCollection();

  EPGraph<VD, ED, GD> getGraph(Long graphID) throws Exception;

  void writeAsJson(final String vertexFile, final String egeFile,
    final String graphFile) throws Exception;
}
