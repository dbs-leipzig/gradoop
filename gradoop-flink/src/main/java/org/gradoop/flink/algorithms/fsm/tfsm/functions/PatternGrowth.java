package org.gradoop.flink.algorithms.fsm.tfsm.functions;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.util.Collector;
import org.gradoop.flink.algorithms.fsm.common.config.FSMConfig;
import org.gradoop.flink.algorithms.fsm.common.functions.JoinEmbeddings;
import org.gradoop.flink.algorithms.fsm.common.pojos.Embedding;
import org.gradoop.flink.algorithms.fsm.common.pojos.FSMEdge;
import org.gradoop.flink.algorithms.fsm.common.pojos.FSMGraph;
import org.gradoop.flink.algorithms.fsm.common.pojos.Union;
import org.gradoop.flink.algorithms.fsm.common.tuples.SubgraphEmbeddings;

import java.util.Collection;
import java.util.Map;
import java.util.Set;


public class PatternGrowth<G extends FSMGraph, SE extends SubgraphEmbeddings>
  extends JoinEmbeddings<SE> implements FlatJoinFunction<SE, G, SE> {

  public PatternGrowth(FSMConfig fsmConfig) {
    super(fsmConfig);
  }

  @Override
  public void join(SE embeddings,
    G graph, Collector<SE> out) throws
    Exception {

    Set<Union> unions = Sets.newHashSet();
    Map<String, Collection<Embedding>> subgraphEmbeddings = Maps.newHashMap();

    for (Embedding parent : embeddings.getEmbeddings()) {
      for (Map.Entry<Integer, FSMEdge> entry : graph.getEdges().entrySet()) {

        int edgeId = entry.getKey();

        if (! parent.getEdges().containsKey(edgeId)) {
          FSMEdge edge = entry.getValue();

          int sourceId = edge.getSourceId();
          boolean containsSourceId =
            parent.getVertices().containsKey(sourceId);

          int targetId = edge.getTargetId();
          boolean containsTargetId =
            parent.getVertices().containsKey(targetId);

          if (containsSourceId ||  containsTargetId) {
            Union union = new Union(parent.getEdgeIds(), edgeId);

            if (! unions.contains(union)) {
              unions.add(union);

              Embedding child = parent.deepCopy();
              child.getEdges().put(edgeId, edge);

              if (! containsSourceId) {
                child.getVertices()
                  .put(sourceId, graph.getVertices().get(sourceId));
              }
              if (! containsTargetId) {
                child.getVertices()
                  .put(targetId, graph.getVertices().get(targetId));
              }

              String canonicalLabel = canonicalLabeler.label(child);

              Collection<Embedding> siblings =
                subgraphEmbeddings.get(canonicalLabel);

              if (siblings == null) {
                siblings = Lists.newArrayList(child);
                subgraphEmbeddings.put(canonicalLabel, siblings);
              } else {
                siblings.add(child);
              }
            }
          }
        }
      }
    }

    embeddings.setSize(embeddings.getSize() + 1);
    collect(embeddings, out, subgraphEmbeddings);
  }
}
