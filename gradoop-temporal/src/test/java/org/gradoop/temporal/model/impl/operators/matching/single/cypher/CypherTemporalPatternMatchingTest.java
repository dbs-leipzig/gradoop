package org.gradoop.temporal.model.impl.operators.matching.single.cypher;

import org.gradoop.flink.model.api.epgm.BaseGraphCollection;
import org.gradoop.flink.model.impl.operators.matching.common.MatchStrategy;
import org.gradoop.flink.model.impl.operators.matching.common.statistics.GraphStatistics;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.CypherPatternMatching;
import org.gradoop.temporal.model.impl.TemporalGraph;
import org.gradoop.temporal.model.impl.TemporalGraphCollection;
import org.gradoop.temporal.model.impl.pojo.TemporalEdge;
import org.gradoop.temporal.model.impl.pojo.TemporalGraphHead;
import org.gradoop.temporal.model.impl.pojo.TemporalVertex;
import org.gradoop.temporal.util.TemporalGradoopTestBase;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

public class CypherTemporalPatternMatchingTest extends TemporalGradoopTestBase {


    TemporalGraph graph;

    {
        try {
            graph = toTemporalGraph(getTemporalSocialNetworkLoader().getLogicalGraph());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    GraphStatistics stats = new GraphStatistics(11, 24,
                11, 11);


    @Test
    public void test1() throws Exception {
        String query = "MATCH (n)-[e]->(m) WHERE n.tx.overlaps(m.tx)";
        issueQuery(query).print();
    }

    private TemporalGraphCollection issueQuery(String query){
        CypherTemporalPatternMatching matcher =
                new CypherTemporalPatternMatching(query, true,
                MatchStrategy.ISOMORPHISM, MatchStrategy.ISOMORPHISM, stats);
        return matcher.execute(graph);
    }


}
