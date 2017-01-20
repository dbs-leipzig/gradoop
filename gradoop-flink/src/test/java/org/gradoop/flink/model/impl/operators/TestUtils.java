package org.gradoop.flink.model.impl.operators;

import java.util.HashMap;
import java.util.stream.Collectors;

/**
 * Created by Giacomo Bergami on 19/01/17.
 */
public class TestUtils {

    public static class GraphWithinDatabase {
        StringBuilder sb = null;
        protected TupleBuilder<GraphWithinDatabase> tp;

        private GraphWithinDatabase() {
            tp = new TupleBuilder<>();
        }

        public static TupleBuilder<GraphWithinDatabase> generateGraphName(String name) {
            GraphWithinDatabase g = new GraphWithinDatabase();
            return TupleBuilder.generateWithVariable(g,g.tp,name);
        }
        public static TupleBuilder<GraphWithinDatabase> generateGraphLabel(String name) {
            GraphWithinDatabase g = new GraphWithinDatabase();
            return TupleBuilder.generateWithType(g,g.tp,name);
        }
        public static TupleBuilder<GraphWithinDatabase> labelType(String name, String type) {
            GraphWithinDatabase g = new GraphWithinDatabase();
            return TupleBuilder.generateWithValueAndType(g,g.tp,name,type);
        }

        public PatternBuilder pat() {
            return new PatternBuilder(this);
        }

        @Override
        public String toString() {
            return tp.toString()+(sb==null ? "[]" : "[\n"+sb.toString()+"\n]");
        }

        GraphWithinDatabase addClosedPattern(PatternBuilder patternBuilder) {
            if (sb==null)
                sb = new StringBuilder();
            sb.append(patternBuilder);
            return this;
        }
    }

    public static class VertexBuilder<P> extends TupleBuilder<P> {
        public VertexBuilder() {
            super();
        }
        @Override
        public String toString() {
            return "("+super.toString()+")";
        }
    }

    public static class EdgeBuilder<P> extends TupleBuilder<P> {
        EdgeBuilder() {
            super();
        }
        @Override
        public String toString() {
            return "["+super.toString()+"]";
        }
    }

    public static class PatternBuilder {

        VertexBuilder<PatternBuilder> src, dst;
        EdgeBuilder<PatternBuilder> e = null;
        GraphWithinDatabase elem;

        PatternBuilder(GraphWithinDatabase x) {
            elem = x;
        }

        public VertexBuilder<PatternBuilder> from() {
            src = TupleBuilder.generateEmpty(this,new VertexBuilder<>());
            return src;
        }
        public VertexBuilder<PatternBuilder> fromType(String k) {
            src = TupleBuilder.generateWithType(this,new VertexBuilder<>(),k);
            return src;
        }
        public VertexBuilder<PatternBuilder> fromVariable(String k) {
            src =  TupleBuilder.generateWithVariable(this,new VertexBuilder<>(),k);
            return src;
        }
        public VertexBuilder<PatternBuilder> fromVariableKey(String v, String k) {
            src = TupleBuilder.generateWithValueAndType(this,new VertexBuilder<>(),v,k);
            return src;
        }

        public VertexBuilder<PatternBuilder> to() {
            dst = TupleBuilder.generateEmpty(this,new VertexBuilder<>());
            return dst;
        }
        public VertexBuilder<PatternBuilder> toType(String k) {
            dst = TupleBuilder.generateWithType(this,new VertexBuilder<>(),k);
            return dst;
        }
        public VertexBuilder<PatternBuilder> toVariable(String k) {
            dst =  TupleBuilder.generateWithVariable(this,new VertexBuilder<>(),k);
            return dst;
        }
        public VertexBuilder<PatternBuilder> toVariableKey(String v, String k) {
            dst =  TupleBuilder.generateWithValueAndType(this,new VertexBuilder<>(),v,k);
            return dst;
        }

        public EdgeBuilder<PatternBuilder> edgeKey(String k) {
            e = TupleBuilder.generateWithType(this,new EdgeBuilder<>(),k);
            return e;
        }
        public EdgeBuilder<PatternBuilder> edgeVariable(String k) {
            e =  TupleBuilder.generateWithVariable(this,new EdgeBuilder<>(),k);
            return e;
        }
        public EdgeBuilder<PatternBuilder> edgeVariableKey(String v, String k) {
            e =  TupleBuilder.generateWithValueAndType(this,new EdgeBuilder<>(),v,k);
            return e;
        }

        public GraphWithinDatabase done() {
            return elem.addClosedPattern(this);
        }

        @Override
        public String toString() {
            if (src==null && dst==null) {
                return "";
            } else if (src==null || dst==null) {
                return src != null ? src.toString() : dst.toString();
            } else {
                StringBuilder sb = new StringBuilder();
                sb.append(src.toString());
                sb.append('-');
                if (e!=null) {
                    sb.append(e.toString());
                }
                sb.append("->");
                return sb.append(dst.toString()).append('\n').toString();
            }
        }

    }

    public static class TupleBuilder<P> {

        String variableName;
        String typeName;
        PropList<P> propbuilder;
        P parent;

        TupleBuilder() {}

        public static <K extends TupleBuilder,P> K generateEmpty(P parent, K tb) {
            tb.variableName = "";
            tb.typeName = "";
            tb.propbuilder = null;
            tb.parent = parent;
            return tb;
        }

        public static <K extends TupleBuilder,P> K generateWithVariable(P parent, K tb, String str) {
            tb.variableName = str;
            tb.typeName = "";
            tb.propbuilder = null;
            tb.parent = parent;
            return tb;
        }

        public static <K extends TupleBuilder,P> K generateWithType(P parent, K tb, String str) {
            tb.variableName = "";
            tb.typeName = ":"+str;
            tb.propbuilder = null;
            tb.parent = parent;
            return tb;
        }

        public static <K extends TupleBuilder,P> K generateWithValueAndType(P parent, K tb, String v, String k) {
            tb.variableName = v;
            tb.typeName = ":"+k;
            tb.propbuilder = null;
            tb.parent = parent;
            return tb;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append(variableName).append(typeName);
            if (propbuilder!=null && (!propbuilder.isEmpty())) {
                sb.append(propbuilder.toString());
            }
            return sb.toString();
        }

        public P t() {
            return parent;
        }

        public PropList<P> propList() {
            this.propbuilder = new PropList<>(this);
            return this.propbuilder;
        }


    }

    public static class PropList<P> {
        private final HashMap<String,String> attrTo;
        private final TupleBuilder<P> finale;

        private boolean isEmpty() {
            return attrTo.isEmpty();
        }

        private PropList(TupleBuilder<P> finale) {
            attrTo = new HashMap<>();
            this.finale = finale;
        }

        public PropList<P> put(String key, String value) {
            attrTo.put(key,value);
            return this;
        }

        public PropList<P> put(String key) {
            attrTo.put(key,"NULL");
            return this;
        }

        @Override
        public String toString() {
            return attrTo.entrySet().stream()
                    .map(x -> x.getKey() + " : " + x.getValue())
            .collect(Collectors.joining(",", " {", "}"));
        }

        public P plEnd() {
            return this.finale.t();
        }
    }



    public static void main(String args[]) {














    }

}
