package org.gradoop.flink.algorithms.fsm.util;

import com.google.common.collect.Lists;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.RichGroupCombineFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Either;
import org.apache.flink.util.Collector;
import org.gradoop.flink.algorithms.fsm.config.BroadcastNames;
import org.gradoop.flink.algorithms.fsm.gspan.comparators.DFSCodeComparator;
import org.gradoop.flink.algorithms.fsm.gspan.pojos.DFSStep;
import org.gradoop.flink.algorithms.fsm.gspan.pojos.DFSCode;
import org.gradoop.flink.algorithms.fsm.gspan.pojos.CompressedDFSCode;
import org.gradoop.flink.model.impl.tuples.WithCount;

import java.util.Collections;
import java.util.List;

public class GradoopFSMTestUtils {

  public static void printDifference(
    DataSet<WithCount<CompressedDFSCode>> left,
    DataSet<WithCount<CompressedDFSCode>> right) throws Exception {

    left.fullOuterJoin(right)
      .where(0).equalTo(0)
    .with(new JoinDifference())
    .print();
    ;


  }

  public static void sortTranslateAndPrint(
    DataSet<WithCount<CompressedDFSCode>> iResult,
    DataSet<List<String>> vertexLabelDictionary,
    DataSet<List<String>> edgeLabelDictionary) throws Exception {

    System.out.println(
      iResult
        .combineGroup(new SortAndTranslate())
        .withBroadcastSet(
          vertexLabelDictionary, BroadcastNames.VERTEX_DICTIONARY)
        .withBroadcastSet(
          edgeLabelDictionary, BroadcastNames.EDGE_DICTIONARY)
        .collect()
        .get(0)
    );
  }

  private static class SortAndTranslate
    extends
    RichGroupCombineFunction<WithCount<CompressedDFSCode>, String> {

    private List<String> vertexDictionary;
    private List<String> edgeDictionary;


    @Override
    public void open(Configuration parameters) throws Exception {
      super.open(parameters);
      this.vertexDictionary = getRuntimeContext()
        .<List<String>>getBroadcastVariable(BroadcastNames.VERTEX_DICTIONARY)
        .get(0);
      this.edgeDictionary = getRuntimeContext()
        .<List<String>>getBroadcastVariable(BroadcastNames.EDGE_DICTIONARY)
        .get(0);
    }

    @Override
    public void combine(Iterable<WithCount<CompressedDFSCode>> iterable,
      Collector<String> collector) throws Exception {

      List<DFSCode> subgraphs = Lists.newArrayList();
      List<String> strings = Lists.newArrayList();


      for(WithCount<CompressedDFSCode> subgraphWithCount : iterable) {

        subgraphs.add(subgraphWithCount.getObject().getDfsCode());
      }

      Collections.sort(subgraphs, new DFSCodeComparator(true));


      for(DFSCode subgraph : subgraphs) {
        int lastToTime = -1;
        StringBuilder builder = new StringBuilder();

        for(DFSStep step : subgraph.getSteps()) {
          int fromTime = step.getFromTime();
          String fromLabel = vertexDictionary.get(step.getFromLabel());
          boolean outgoing = step.isOutgoing();
          String edgeLabel = edgeDictionary.get(step.getEdgeLabel());
          int toTime = step.getToTime();
          String toLabel = vertexDictionary.get(step.getToLabel());

          if (lastToTime != fromTime) {
            builder
              .append(" ")
              .append(formatVertex(fromTime, fromLabel));
          }
          builder
            .append(formatEdge(outgoing, edgeLabel))
            .append(formatVertex(toTime, toLabel));

          lastToTime = toTime;
        }

        strings.add(builder.toString());
      }


      collector.collect(StringUtils.join(strings, "\n"));
    }


  }

  private static String formatVertex(int id, String label) {
    return "(" + id + ":" + label + ")";
  }

  private static String formatEdge(boolean outgoing, String edgeLabel) {
    return outgoing ?
      "-" + edgeLabel + "->" : "<-" + edgeLabel + "-";
  }

  private static class JoinDifference
    implements FlatJoinFunction<WithCount<CompressedDFSCode>, WithCount<CompressedDFSCode>,
    Either<WithCount<CompressedDFSCode>, WithCount<CompressedDFSCode>>> {

    @Override
    public void join(
      WithCount<CompressedDFSCode> left,
      WithCount<CompressedDFSCode> right,
      Collector<Either<WithCount<CompressedDFSCode>, WithCount<CompressedDFSCode>>> collector) throws
      Exception {

      if(left == null) {
        collector.collect(
          Either.<WithCount<CompressedDFSCode>, WithCount<CompressedDFSCode>>Right(right));
      } else if (right == null) {
        collector.collect(
          Either.<WithCount<CompressedDFSCode>, WithCount<CompressedDFSCode>>Left(left));
      } else if (left.getCount() != right.getCount()) {
        collector.collect(
          Either.<WithCount<CompressedDFSCode>, WithCount<CompressedDFSCode>>Left(left));
        collector.collect(
          Either.<WithCount<CompressedDFSCode>, WithCount<CompressedDFSCode>>Right(right));
      }

    }
  }
}
