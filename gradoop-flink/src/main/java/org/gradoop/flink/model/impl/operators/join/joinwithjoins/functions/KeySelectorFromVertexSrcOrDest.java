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

package org.gradoop.flink.model.impl.operators.join.joinwithjoins.functions;

import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.functions.KeySelector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;

import java.io.Serializable;

/**
 * Acts as a SourceId<> or as a TargetId<> dependingly on the operand position.
 *
 * Created by vasistas on 14/02/17.
 */
@FunctionAnnotation.ForwardedFields("sourceId->*; targetId->*")
public class KeySelectorFromVertexSrcOrDest implements KeySelector<Edge, GradoopId>, Serializable {

  public final boolean isLeft;

  public KeySelectorFromVertexSrcOrDest(boolean elem) {
    isLeft = elem;
  }

  @Override
  public GradoopId getKey(Edge e1) throws Exception {
    return isLeft ? e1.getSourceId() : e1.getTargetId();
  }
}
