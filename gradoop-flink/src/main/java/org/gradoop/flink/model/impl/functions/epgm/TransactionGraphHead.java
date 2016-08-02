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

package org.gradoop.flink.model.impl.functions.epgm;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.api.entities.EPGMGraphHead;
import org.gradoop.common.model.api.entities.EPGMEdge;
import org.gradoop.common.model.api.entities.EPGMVertex;

import java.util.Set;

/**
 * (graphHead, {vertex,..}, {edge,..}) => graphHead
 */
public class TransactionGraphHead implements
  MapFunction<Tuple3<EPGMGraphHead, Set<EPGMVertex>, Set<EPGMEdge>>, EPGMGraphHead> {

  @Override
  public EPGMGraphHead map(Tuple3<EPGMGraphHead, Set<EPGMVertex>, Set<EPGMEdge>> triple)
    throws Exception {
    return triple.f0;
  }
}
