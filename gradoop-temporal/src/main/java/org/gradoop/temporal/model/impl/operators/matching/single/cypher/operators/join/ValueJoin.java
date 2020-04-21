package org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.join;

import com.google.common.collect.Lists;
import org.apache.flink.api.common.operators.base.JoinOperatorBase;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.PhysicalTPGMOperator;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.join.functions.ExtractValueJoinColumns;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.join.functions.MergeTemporalEmbeddings;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.pojos.EmbeddingTPGM;

import java.util.Collections;
import java.util.List;

/**
 * This operator joins two possibly disjunct data sets by predicates only concerning properties.
 * The predicates must include at least one isolated equality predicate e.g.:
 * <br>
 * {@code MATCH (a:Department), (b)-[:X]->(c:Person (name: "Max") WHERE a.prop = b.prop AND a.prop2 = b.prop2}
 * <p>
 * The result is always a new embedding with the following constraints:
 * <ul>
 * <li>new entries of the right embedding are always appended to the left embedding</li>
 * <li>all properties from the right side are appended to the properties of the left side
 * <li> all time data from the right side are appended to the time data of the left side</li>
 * </ul>
 */
public class ValueJoin implements PhysicalTPGMOperator {

    /**
     * Left side embeddings
     */
    private final DataSet<EmbeddingTPGM> left;
    /**
     * Right side embeddings
     */
    private final DataSet<EmbeddingTPGM> right;
    /**
     * left property columns used for the join
     */
    private final List<Integer> leftJoinProperties;
    /**
     * right properties columns used for the join
     */
    private final List<Integer> rightJoinProperties;
    /**
     * left time data used for the join
     */
    private final List<Tuple2<Integer, Integer>> leftJoinTimeData;
    /**
     * right time data used for the join
     */
    private final List<Tuple2<Integer, Integer>> rightJoinTimeData;
    /**
     * Number of columns in the right embedding.
     */
    private final int rightColumns;
    /**
     * Columns that represent vertices in the left embedding which need to be distinct
     */
    private final List<Integer> distinctVertexColumnsLeft;
    /**
     * Columns that represent vertices in the right embedding which need to be distinct
     */
    private final List<Integer> distinctVertexColumnsRight;
    /**
     * Columns that represent edges in the left embedding which need to be distinct
     */
    private final List<Integer> distinctEdgeColumnsLeft;
    /**
     * Columns that represent edges in the right embedding which need to be distinct
     */
    private final List<Integer> distinctEdgeColumnsRight;
    /**
     * Flink join Hint
     */
    private final JoinOperatorBase.JoinHint joinHint;
    /**
     * Stores the operator name used for flink operator naming
     */
    private String name;

    /**
     * New value equi join operator
     *
     * @param left                left hand side data set
     * @param right               right hand side data set
     * @param leftJoinProperties  join criteria (properties)
     * @param rightJoinProperties join criteria (properties)
     * @param leftJoinTimeData     join criteria (time data)
     * @param rightJoinTimeData     join criteria (time data)
     * @param rightColumns        size of the right embedding
     */
    public ValueJoin(DataSet<EmbeddingTPGM> left, DataSet<EmbeddingTPGM> right,
                     List<Integer> leftJoinProperties, List<Integer> rightJoinProperties,
                     List<Tuple2<Integer, Integer>> leftJoinTimeData,
                     List<Tuple2<Integer, Integer>> rightJoinTimeData,
                     int rightColumns) {

        this(
                left, right,
                leftJoinProperties,
                rightJoinProperties,
                leftJoinTimeData,
                rightJoinTimeData,
                rightColumns,
                Collections.emptyList(),
                Collections.emptyList(),
                Collections.emptyList(),
                Collections.emptyList(),
                JoinOperatorBase.JoinHint.OPTIMIZER_CHOOSES
        );
    }

    /**
     * New value equi join operator
     *
     * @param left                       left hand side data set
     * @param right                      right hand side data set
     * @param leftJoinProperties            join criteria (properties)
     * @param rightJoinProperties           join criteria (properties)
     * @param leftJoinTimeData              join criteria (time data)
     * @param rightJoinTimeData             join criteria (time data)
     * @param rightColumns               size of the right embedding
     * @param distinctVertexColumnsLeft  distinct vertex columns of the left embedding
     * @param distinctVertexColumnsRight distinct vertex columns of the right embedding
     * @param distinctEdgeColumnsLeft    distinct edge columns of the left embedding
     * @param distinctEdgeColumnsRight   distinct edge columns of the right embedding
     * @param joinHint                   join hint
     */
    public ValueJoin(DataSet<EmbeddingTPGM> left, DataSet<EmbeddingTPGM> right,
                     List<Integer> leftJoinProperties, List<Integer> rightJoinProperties,
                     List<Tuple2<Integer, Integer>> leftJoinTimeData,
                     List<Tuple2<Integer, Integer>> rightJoinTimeData,
                     int rightColumns,
                     List<Integer> distinctVertexColumnsLeft, List<Integer> distinctVertexColumnsRight,
                     List<Integer> distinctEdgeColumnsLeft, List<Integer> distinctEdgeColumnsRight,
                     JoinOperatorBase.JoinHint joinHint) {

        this.left = left;
        this.right = right;
        this.leftJoinProperties = leftJoinProperties;
        this.rightJoinProperties = rightJoinProperties;
        this.leftJoinTimeData = leftJoinTimeData;
        this.rightJoinTimeData = rightJoinTimeData;
        this.rightColumns = rightColumns;
        this.distinctVertexColumnsLeft = distinctVertexColumnsLeft;
        this.distinctVertexColumnsRight = distinctVertexColumnsRight;
        this.distinctEdgeColumnsLeft = distinctEdgeColumnsLeft;
        this.distinctEdgeColumnsRight = distinctEdgeColumnsRight;
        this.joinHint = joinHint;
        this.setName("ValueJoin");
    }

    @Override
    public DataSet<EmbeddingTPGM> evaluate() {
        return left.join(right, joinHint)
                .where(new ExtractValueJoinColumns(leftJoinProperties, leftJoinTimeData))
                .equalTo(new ExtractValueJoinColumns(rightJoinProperties, rightJoinTimeData))
                .with(new MergeTemporalEmbeddings(
                        rightColumns,
                        Lists.newArrayListWithCapacity(0),
                        distinctVertexColumnsLeft,
                        distinctVertexColumnsRight,
                        distinctEdgeColumnsLeft,
                        distinctEdgeColumnsRight
                ))
                .name(getName());
    }

    @Override
    public void setName(String newName) {
        this.name = newName;
    }

    @Override
    public String getName() {
        return this.name;
    }
}
