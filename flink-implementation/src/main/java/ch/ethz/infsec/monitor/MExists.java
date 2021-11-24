package ch.ethz.infsec.monitor;
import ch.ethz.infsec.policy.VariableID;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Optional;
import ch.ethz.infsec.util.*;
import ch.ethz.infsec.monitor.visitor.*;

public class MExists implements Mformula, FlatMapFunction<PipelineEvent, PipelineEvent> {

    public Mformula formula;
    VariableID var;
    Integer numberProcessors;
    HashMap<Long, Long> timepointToTimestamp; //mapping from timepoint to timestamps for non-terminator events
    HashMap<Long, Long> terminators;//mapping from timepoint to timestamps for terminator events
    HashMap<Long, Integer> terminatorCount;

    public MExists(Mformula formula, VariableID var){
        this.formula = formula;
        this.var = var;
        this.timepointToTimestamp = new HashMap<>();
        this.terminators = new HashMap<>();
        this.terminatorCount = new HashMap<>();
    }

    @Override
    public <T> DataStream<PipelineEvent> accept(MformulaVisitor<T> v) {
        return (DataStream<PipelineEvent>) v.visit(this);
    }

    @Override
    public void setNumberProcessors(int numberProcessors) {
        this.numberProcessors = numberProcessors;
    }

    @Override
    public Integer getNumberProcessors() {
        return this.numberProcessors;
    }

    @Override
    public void flatMap(PipelineEvent event, Collector<PipelineEvent> out) throws Exception {
        //satisfaction list lengths need to be the same for implementations of joins, but they change for quantifier
        //operators. For the existential quantifier operator, the input free-variables array has length n+1 and the
        //output will have length n
        if( event.isPresent()) {
            Assignment satList = event.get();
            satList.remove(0); //aking the front of the list
            Optional<Assignment> output = Optional.of(satList);
            if (output.isPresent()) {
                PipelineEvent result = PipelineEvent.event(event.getTimestamp(),event.getTimepoint(), output.get());
                out.collect(result);
            }
        } else {
            if (!terminatorCount.containsKey(event.getTimepoint())) {
                terminatorCount.put(event.getTimepoint(), 1);
            } else {
                terminatorCount.put(event.getTimepoint(), terminatorCount.get(event.getTimepoint()) + 1);
            }
            // only add terminator when received correct amount
            if ((terminatorCount.get(event.getTimepoint()).equals(this.formula.getNumberProcessors()))) {
                terminators.put(event.getTimepoint(), event.getTimestamp());
            }
            // output terminators
            if(terminators.containsKey(event.getTimepoint() - 1)){
                if(event.getTimepoint() - 1 == 0L){
                    out.collect(PipelineEvent.terminator(terminators.get(0L), event.getTimepoint() - 1));
                }
                out.collect(PipelineEvent.terminator(event.getTimestamp(), event.getTimepoint()));
                terminators.remove(event.getTimepoint() - 1);
                terminatorCount.remove(event.getTimepoint() - 1);
                timepointToTimestamp.keySet().removeIf(tp -> tp <= event.getTimepoint() - 1);
            }
        }
    }

}