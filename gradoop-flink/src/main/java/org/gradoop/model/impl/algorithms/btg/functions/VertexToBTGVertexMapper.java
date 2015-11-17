/*
 * This file is part of gradoop.
 *
 * gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with gradoop. If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.model.impl.algorithms.btg.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.graph.Vertex;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.impl.algorithms.btg.BTG;
import org.gradoop.model.impl.algorithms.btg.pojos.BTGVertexValue;
import org.gradoop.model.impl.algorithms.btg.utils.BTGVertexType;
import org.gradoop.model.impl.id.GradoopId;

import java.util.ArrayList;
import java.util.List;

/**
 * Maps EPGM vertices to a BTG specific representation.
 *
 * @param <VD> EPGM vertex type
 * @see BTGVertexValue
 */
public class VertexToBTGVertexMapper<VD extends EPGMVertex>
  implements MapFunction<VD, Vertex<GradoopId, BTGVertexValue>> {
  /**
   * {@inheritDoc}
   */
  @Override
  public Vertex<GradoopId, BTGVertexValue> map(VD logicalVertex
  ) throws Exception {

    BTGVertexValue btgValue = createNewVertexValue(logicalVertex);
    return new Vertex<>(logicalVertex.getId(), btgValue);
  }

  /**
   * Method to create a new BTG vertex value from a given vertex
   *
   * @param vertex actual vertex
   * @return BTGVertexValue
   */
  private BTGVertexValue createNewVertexValue(VD vertex) {
    BTGVertexType type = BTGVertexType.values()[Integer.parseInt(
      (String) vertex.getProperty(BTG.VERTEX_TYPE_PROPERTYKEY))];
    double value = Double.parseDouble(
      (String) vertex.getProperty(BTG.VERTEX_VALUE_PROPERTYKEY));
    List<GradoopId> btgIDs = getBTGIDs(
      (String) vertex.getProperty(BTG.VERTEX_BTGIDS_PROPERTYKEY));
    return new BTGVertexValue(type, value, btgIDs);
  }

  /**
   * Method to return the BTG IDs from a given epGraph
   *
   * @param btgIDs String of BTGIDs
   * @return List of BTGIDs
   */
  private static List<GradoopId> getBTGIDs(String btgIDs) {
    if (btgIDs.length() == 0) {
      return new ArrayList<>();
    } else {
      List<GradoopId> btgList = new ArrayList<>();
      String[] btgIDArray = btgIDs.split(",");
      for (String btgId : btgIDArray) {
        btgList.add(GradoopId.fromLongString(btgId));
      }
      return btgList;
    }
  }
}
