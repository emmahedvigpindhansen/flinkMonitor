package ch.ethz.infsec.monitor;
import ch.ethz.infsec.policy.Term;
import ch.ethz.infsec.policy.VariableID;
import ch.ethz.infsec.term.JavaConst;
import ch.ethz.infsec.term.JavaTerm;
import ch.ethz.infsec.term.JavaVar;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.util.Collector;
import scala.collection.Seq;
import scala.collection.*;
import java.util.*;
import java.util.Iterator;
import java.util.Set;
import java.util.stream.Collectors;

import static ch.ethz.infsec.term.JavaTerm.convert;
import ch.ethz.infsec.util.*;
import ch.ethz.infsec.monitor.visitor.*;

public final class MPred implements Mformula, FlatMapFunction<Fact, PipelineEvent> {
    String predName;
    ArrayList<JavaTerm<VariableID>> args;
    List<VariableID> freeVariablesInOrder;
    Integer numberProcessors;

    public MPred(String predName, Seq<Term<VariableID>> args, List<VariableID> fvio){
        List<Term<VariableID>> argsScala = new ArrayList(JavaConverters.seqAsJavaList(args));
        ArrayList<JavaTerm<VariableID>> argsJava = new ArrayList<>();
        for (Term<VariableID> variableIDTerm : argsScala) {
            argsJava.add(convert(variableIDTerm));
        }
        this.predName = predName;
        this.freeVariablesInOrder = fvio.stream().distinct().collect(Collectors.toList());
        this.args = argsJava;
    }

    public String getPredName(){
      return predName;
    }

    @Override
    public void setNumberProcessors(int numberProcessors) {
        this.numberProcessors = numberProcessors;
    }

    @Override
    public Integer getNumberProcessors() {
        return this.numberProcessors;
    }

    public void flatMap(Fact fact, Collector<PipelineEvent> out) throws Exception {

        // System.out.println(fact.toString());

        if(fact.isTerminator()){
            PipelineEvent terminator = PipelineEvent.terminator(fact.getTimestamp(),fact.getTimepoint());
            // terminator.key = this.freeVariablesInOrder;
            // terminator.key = new ArrayList<VariableID>();
            // terminator.key.add(this.freeVariablesInOrder.get(0));
            out.collect(terminator);
        }else{
            assert(fact.getName().equals(this.predName) );

            ArrayList<JavaTerm<VariableID>> argsFormula = new ArrayList<>(this.args);

            List<Object> ys = fact.getArguments();
            ArrayList<Object> argsEvent = new ArrayList<>(ys);
            //Events are parametrized by data values. Two events are said to match if the
            //corresponding data values are equal.
            Optional<HashMap<VariableID, Optional<Object>>> result = matchFV(argsFormula, argsEvent);
            if(result.isPresent()){
                Assignment list = new Assignment();
                List<VariableID> keys = new ArrayList<VariableID>();
                for (VariableID formulaVariable : this.freeVariablesInOrder) {
                    if(!result.get().containsKey(formulaVariable) || !result.get().get(formulaVariable).isPresent()){
                        list.addLast(Optional.empty());
                    }else{
                        list.addLast(result.get().get(formulaVariable));
                        keys.add(formulaVariable);
                    }
                }
                PipelineEvent event = PipelineEvent.event(fact.getTimestamp(),fact.getTimepoint(), list);
                // event.key = keys;
                out.collect(event);
            }
            //if there are no satisfactions, we simply don't put anything to the collector.
        }
    }

    @Override
    public <T> DataStream<PipelineEvent> accept(MformulaVisitor<T> v) {
        return (DataStream<PipelineEvent>) v.visit(this);
    }

    public static Optional<HashMap<VariableID, Optional<Object>>> matchFV(List<JavaTerm<VariableID>> ts, ArrayList<Object> ys){
        //ts: arguments of the formula
        //ys: arguments of the incoming fact
        if(ts.size() != ys.size()){
            return Optional.empty();
        }else if(ts.size() == 0 && ys.size() == 0) {
            HashMap<VariableID, Optional<Object>> emptyMap = new HashMap<>();
            return Optional.of(emptyMap);
        }else {
            if(ts.size() > 0 && ys.size() > 0 && (ts.get(0) instanceof JavaConst)) {

                if(ts.get(0).toString().equals(ys.get(0).toString())) {

                    JavaTerm<VariableID> t = ts.remove(0); //from formula
                    Object y = ys.remove(0); //from fact
                    Optional<HashMap<VariableID, Optional<Object>>> partialResult =  matchFV(ts, ys);
                    return partialResult;
                }else {
                    return Optional.empty();
                }
            }else if(ts.size() > 0 && ys.size() > 0 && (ts.get(0) instanceof JavaVar)) {

                JavaVar<VariableID> x =  (JavaVar<VariableID>) ts.remove(0);
                Object y = ys.remove(0);

                Optional<HashMap<VariableID, Optional<Object>>> recFunction = matchFV(ts, ys);
                if(!recFunction.isPresent()){
                    return Optional.empty();
                }else{
                    HashMap<VariableID, Optional<Object>> f = recFunction.get();
                    if(!f.containsKey(x) || !(f.get(x)).isPresent()){
                        f.put(x.variable(), Optional.of(y));
                        return Optional.of(f);
                    }else{
                        Object z = f.get(x).get();
                        if (y.equals(z)){
                            return Optional.of(f);
                        }else{
                            return Optional.empty();
                        }
                    }
                }
            }else{
                return Optional.empty();
            }
        }

    }
}
