package org.gradoop.flink.model.impl.operators.utils;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by Giacomo Bergami on 19/01/17.
 */
public class GDLBuilder {

    public static class GraphWithinDatabase implements IWithDependencies {
        //StringBuilder sb = null;
        ArrayList<PatternBuilder> sb = null;
        private Set<String> dependencies = new HashSet<>();
        protected TupleBuilder<GraphWithinDatabase> tp;
        Object parent;

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
            return TupleBuilder.generateWithVariableAndType(g,g.tp,name,type);
        }


        public PatternBuilder pat() {
            return new PatternBuilder(this);
        }

        @Override
        public String toString() {
            return tp.toString()+(sb==null ? "[]" : "["+sb.toString()+"]");
        }

        public boolean hasElementPropertyValues() {
            return tp!=null && tp.propbuilder!=null && !tp.propbuilder.isEmpty();
        }

        GraphWithinDatabase addClosedPattern(Collection<String> references, PatternBuilder patternBuilder) {
            if (sb==null)
                sb = new ArrayList<>();
            dependencies.addAll(references);
            sb.add(patternBuilder);
            return this;
        }

        @Override
        public Set<String> getDependencies() {
            return dependencies;
        }

        @Override
        public String toString(boolean withVariableName) {
             return tp.toString()+(sb==null ? "[]" : "["+sb.stream().map(x-> x.toString(withVariableName)).collect(Collectors.joining(" "))+"]");
        }

        @Override
        public String compileWith(HashMap<String, IWithDependencies> infoToExpand, boolean withVariableName) {
            return tp.toString()+(sb==null ? "[]" : "["+sb.stream().map(x->x.compile(infoToExpand,withVariableName)).collect(Collectors.joining(" "))+"]");
        }
    }

    public static class VertexBuilder<P> extends TupleBuilderWithDependencies<P> {
        public VertexBuilder() {
            super();
        }
        @Override
        public String toString() {
            return "("+super.toString()+")";
        }

        @Override
        public String compileWith(HashMap<String, IWithDependencies> infoToExpand, boolean withVariableName) {
            return "("+super.compileWith(infoToExpand,withVariableName)+")";
        }
    }

    public static class TupleBuilderWithDependencies<P> extends TupleBuilder<P> implements IWithDependencies {

        public TupleBuilderWithDependencies() {
            super();
        }

        @Override
        public Set<String> getDependencies() {
            Set<String> elem = new HashSet<>();
            if (getVariableName()!=null) elem.add(variableName);
            return elem;
        }

        @Override
        public String toString(boolean withVariableName) {
            return super.toString(withVariableName);
        }

        @Override
        public String compileWith(HashMap<String, IWithDependencies> infoToExpand,boolean withVariableName) {
            return super.compile(infoToExpand,withVariableName);
        }

    }

    public static class EdgeBuilder<P> extends TupleBuilderWithDependencies<P> {
        public EdgeBuilder() {
            super();
        }
        @Override
        public String toString() {
            return "["+super.toString()+"]";
        }

        @Override
        public String compileWith(HashMap<String, IWithDependencies> infoToExpand, boolean withVariableName) {
             return "["+super.compileWith(infoToExpand,withVariableName)+"]";
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
            src = TupleBuilder.generateWithVariableAndType(this,new VertexBuilder<>(),v,k);
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
            dst =  TupleBuilder.generateWithVariableAndType(this,new VertexBuilder<>(),v,k);
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
            e =  TupleBuilder.generateWithVariableAndType(this,new EdgeBuilder<>(),v,k);
            return e;
        }

        public GraphWithinDatabase done() {
            Set<String> references = new HashSet<>();
            if (src!=null && src.getVariableName()!=null && src.getVariableName().length()>0)
                references.add(src.getVariableName());
            if (dst!=null && dst.getVariableName()!=null && dst.getVariableName().length()>0)
                references.add(dst.getVariableName());
            if (e!=null && e.getVariableName()!=null && e.getVariableName().length()>0)
                references.add(e.getVariableName());
            return elem.addClosedPattern(references,this);
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
                return sb.append(dst.toString()).append(" ").toString();
            }
        }

        public String toString(boolean b) {
            if (src==null && dst==null) {
                return "";
            } else if (src==null || dst==null) {
                return src != null ? src.toString(b) : dst.toString(b);
            } else {
                StringBuilder sb = new StringBuilder();
                sb.append(src.toString(b));
                sb.append('-');
                if (e!=null) {
                    sb.append(e.toString(b));
                }
                sb.append("->");
                return sb.append(dst.toString(b)).append(" ").toString();
            }
        }

        public String compile(HashMap<String,IWithDependencies> toAssociate, boolean withVariableName) {
            if (src==null && dst==null) {
                return "";
            } else if (src==null || dst==null) {
                return src != null ? src.compileWith(toAssociate,withVariableName).replace("((","(").replace("))",")") : dst.compileWith(toAssociate,withVariableName).replace("((","(").replace("))",")");
            } else {
                StringBuilder sb = new StringBuilder();
                sb.append(src.compileWith(toAssociate,withVariableName).replace("((","(").replace("))",")"));
                sb.append('-');
                if (e!=null) {
                    sb.append(e.compileWith(toAssociate,withVariableName).replace("[(","[").replace(")]","]"));
                }
                sb.append("->");
                return sb.append(dst.compileWith(toAssociate,withVariableName).replace("((","(").replace("))",")")).append(" ").toString();
            }
        }

    }

    public static class TupleBuilder<P> {

        String variableName;
        String typeName;
        PropList<P> propbuilder;
        P parent;

        public String getVariableName() {
            return variableName;
        }

        TupleBuilder() {
            variableName = null;
            typeName = null;
            parent = null;
            propbuilder = null;
        }

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

        public static <K extends TupleBuilder,P> K generateWithVariableAndType(P parent, K tb, String v, String k) {
            tb.variableName = v;
            tb.typeName = ":"+k;
            tb.propbuilder = null;
            tb.parent = parent;
            return tb;
        }

        @Override
        public String toString() {
            return toString(true);
        }

        public String toString(boolean withVariableName) {
            StringBuilder sb = new StringBuilder();
            if (withVariableName)
                sb.append(variableName);
            sb.append(typeName);
            if (propbuilder!=null && (!propbuilder.isEmpty())) {
                sb.append(propbuilder.toString());
            }
            return sb.toString();
        }

        public String compile(HashMap<String,IWithDependencies> toAssociate, boolean withVariableName) {
            if ((propbuilder==null || propbuilder.isEmpty())&&(variableName.length()>0)) {
                return toAssociate.get(variableName).toString(withVariableName);
            } else return toString();
        }

        public P t() {
            return parent;
        }

        public PropList<P> propList() {
            if (this.propbuilder==null)
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

        public PropList<P> put(String value,String key) {
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
                    .map(x -> (x.getKey() + " : " + (x.getValue().equals("NULL") ? "NULL" : "\""+x.getValue() + "\"")))
                    .collect(Collectors.joining(",", " {", "}"));
        }

        public P plEnd() {
            return this.finale.t();
        }
    }


}
