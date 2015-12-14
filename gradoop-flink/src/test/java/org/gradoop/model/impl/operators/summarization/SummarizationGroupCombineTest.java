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

package org.gradoop.model.impl.operators.summarization;

import org.gradoop.model.impl.pojo.EdgePojo;
import org.gradoop.model.impl.pojo.GraphHeadPojo;
import org.gradoop.model.impl.pojo.VertexPojo;

import java.util.List;

public class SummarizationGroupCombineTest extends SummarizationTestBase {

  @Override
  public Summarization<GraphHeadPojo, VertexPojo, EdgePojo>
  getSummarizationImpl(
    List<String> vertexGroupingKeys, boolean useVertexLabel,
    List<String> edgeGroupingKeys, boolean useEdgeLabel) {
    return new SummarizationGroupCombine<>(vertexGroupingKeys, edgeGroupingKeys,
      useVertexLabel, useEdgeLabel);
  }
}
