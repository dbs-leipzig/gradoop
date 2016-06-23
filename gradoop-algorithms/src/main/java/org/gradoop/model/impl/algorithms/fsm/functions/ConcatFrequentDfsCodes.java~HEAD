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

package org.gradoop.model.impl.algorithms.fsm.functions;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.util.Collector;
import org.gradoop.model.impl.algorithms.fsm.tuples.CompressedDFSCode;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * [(CompressedDFSCode, Support),..] => CompressedDFSCode[]
 */
public class ConcatFrequentDfsCodes implements
  GroupReduceFunction<CompressedDFSCode, Collection<CompressedDFSCode>> {

  @Override
  public void reduce(Iterable<CompressedDFSCode> iterable,
    Collector<Collection<CompressedDFSCode>> collector) throws Exception {

    List<CompressedDFSCode> codes = new ArrayList<>();

    for (CompressedDFSCode code : iterable) {
      System.out.println(code);
      codes.add(code);
    }

    collector.collect(codes);
  }
}
