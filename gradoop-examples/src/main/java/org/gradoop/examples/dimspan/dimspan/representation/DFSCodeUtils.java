package org.gradoop.examples.dimspan.dimspan.representation;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.flink.api.java.tuple.Tuple1;

public class DFSCodeUtils extends GraphUtilsBase {

  public static int[] grow(int[] dfsCode,
    int fromTime, int fromLabel, boolean outgoing, int edgeLabel, int toTime, int toLabel) {

    return ArrayUtils.addAll(
      dfsCode, getEdge(fromTime, fromLabel, outgoing, edgeLabel, toTime, toLabel));
  }


  public static boolean isChildOf(int[] child, int[] parent) {
    boolean isChild = parent.length >= child.length;

    if (isChild) {
      for (int i = 0; i < child.length; i++) {
        if (child[i] != parent[i]) {
          isChild = false;
          break;
        }
      }
    }

    return isChild;
  }

  public static int[] getFirstExtension(int[] dfsCode) {
    return ArrayUtils.subarray(dfsCode, 0, EDGE_LENGTH);
  }



  public static void print(int[] pattern) {
    System.out.println(new Tuple1<>(pattern));
  }
}
