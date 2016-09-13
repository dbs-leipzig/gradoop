//package org.gradoop.flink.algorithms.fsm;
//
//import com.google.common.collect.Lists;
//import com.google.common.collect.Maps;
//import com.google.common.collect.Sets;
//import org.apache.flink.api.common.functions.FlatMapFunction;
//import org.apache.flink.util.Collector;
//import org.gradoop.common.model.impl.id.GradoopId;
//import org.gradoop.common.model.impl.id.GradoopIdSet;
//import org.gradoop.common.model.impl.pojo.Edge;
//import org.gradoop.flink.model.impl.tuples.GraphTransaction;
//
//import java.util.Collections;
//import java.util.List;
//import java.util.Map;
//import java.util.Set;
//
///**
// * Created by peet on 13.09.16.
// */
//public class DistinctConnectedSubgraphs
//  implements FlatMapFunction<GraphTransaction, GraphTransaction> {
//
//  @Override
//  public void flatMap(GraphTransaction transaction,
//    Collector<GraphTransaction> out) throws Exception {
//
//    Map<GradoopId, Edge> edgeIndex = Maps.newHashMap();
//    List<GradoopId> edgeIds = Lists.newArrayList();
//
//
//
//    Set<GradoopIdSet> candidates = Sets.newHashSet();
//    Set<GradoopIdSet> distinctConnectedEdgeSets = Sets.newHashSet();
//
//    for (Edge edge : transaction.getEdges()) {
//      GradoopId edgeId = edge.getId();
//      edgeIndex.put(edgeId, edge);
//      edgeIds.add(edgeId);
//      candidates.add(GradoopIdSet.fromExisting(edgeId));
//    }
//
//    Collections.sort(edgeIds);
//
//    while (! lastEdgeSets.isEmpty()) {
//      distinctConnectedEdgeSets.addAll(lastEdgeSets);
//
//      for (Set<Edg>)
//    }
//
//  }
//}
