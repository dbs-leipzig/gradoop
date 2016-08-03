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

package org.gradoop.flink.io.impl.hbase.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.api.entities.EPGMGraphHead;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.storage.api.PersistentGraphHead;
import org.gradoop.common.storage.api.PersistentGraphHeadFactory;

/**
 * Creates persistent graph data from graph data and vertex/edge identifiers.
 *
 * @param <G> EPGM graph head type
 */
public class BuildPersistentGraphHead<G extends EPGMGraphHead>
  implements JoinFunction
  <Tuple3<GradoopId, GradoopIdSet, GradoopIdSet>, G, PersistentGraphHead> {

  /**
   * Persistent graph data factory.
   */
  private PersistentGraphHeadFactory<G> graphHeadFactory;

  /**
   * Creates join function.
   *
   * @param graphHeadFactory persistent graph data factory
   */
  public BuildPersistentGraphHead(
    PersistentGraphHeadFactory<G> graphHeadFactory) {
    this.graphHeadFactory = graphHeadFactory;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public PersistentGraphHead join(
    Tuple3<GradoopId, GradoopIdSet, GradoopIdSet> longSetSetTuple3, G graphHead)
      throws Exception {
    return graphHeadFactory.createGraphHead(graphHead, longSetSetTuple3.f1,
      longSetSetTuple3.f2);
  }
}
