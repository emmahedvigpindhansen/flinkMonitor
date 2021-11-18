package ch.ethz.infsec.monitor;
import java.util.*;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.util.Collector;

import ch.ethz.infsec.util.*;
import ch.ethz.infsec.monitor.visitor.*;



public class MOr implements Mformula, CoFlatMapFunction<PipelineEvent, PipelineEvent, PipelineEvent> {

    public Mformula op1;
    public Mformula op2;
    public Integer indexOfCommonKey;
    HashSet<Long> terminatorLHS;
    HashSet<Long> terminatorRHS;
    HashMap<Long, Integer> terminatorCount1;
    HashMap<Long, Integer> terminatorCount2;
    Integer numberProcessors;

    public MOr(Mformula arg1, Mformula arg2, Integer indexOfCommonKey) {
        op1 = arg1;
        op2 = arg2;
        this.indexOfCommonKey = indexOfCommonKey;

        terminatorLHS = new HashSet<>();
        terminatorRHS = new HashSet<>();
        this.terminatorCount1 = new HashMap<>();
        this.terminatorCount2 = new HashMap<>();
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
    public void flatMap1(PipelineEvent event, Collector<PipelineEvent> collector) throws Exception {
        //here we have a streaming implementation. We can produce an output potentially for every event.
        //We don't buffer events until we receive a terminator event, contrary to what the Verimon algorithm does.
        if(!event.isPresent()){
            if (!terminatorCount1.containsKey(event.getTimepoint())) {
                terminatorCount1.put(event.getTimepoint(), 1);
            } else {
                terminatorCount1.put(event.getTimepoint(), terminatorCount1.get(event.getTimepoint()) + 1);
            }
            // only add terminator to LHS when received correct amount
            if (terminatorCount1.containsKey(event.getTimepoint())) {
                if (terminatorCount1.get(event.getTimepoint()).equals(this.op1.getNumberProcessors())) {
                    terminatorLHS.add(event.getTimepoint());
                }
            }
            if(terminatorRHS.contains(event.getTimepoint())){
                collector.collect(event);
                // terminatorRHS.remove(event.getTimepoint());
                // terminatorLHS.remove(event.getTimepoint());
            }
        }else{
            collector.collect(event);
        }
    }

    @Override
    public void flatMap2(PipelineEvent event, Collector<PipelineEvent> collector) throws Exception {
        //one terminator event will be sent out only once it is received on both incoming streams
        if(!event.isPresent()){
            if (!terminatorCount2.containsKey(event.getTimepoint())) {
                terminatorCount2.put(event.getTimepoint(), 1);
            } else {
                terminatorCount2.put(event.getTimepoint(), terminatorCount2.get(event.getTimepoint()) + 1);
            }

            // only add terminator to RHS when received correct amount
            if (terminatorCount2.containsKey(event.getTimepoint())) {
                if (terminatorCount2.get(event.getTimepoint()).equals(this.op2.getNumberProcessors())) {
                    terminatorRHS.add(event.getTimepoint());
                }
            }
            if(terminatorLHS.contains(event.getTimepoint())){
                collector.collect(event);
                // terminatorRHS.remove(event.getTimepoint());
                // terminatorLHS.remove(event.getTimepoint());
            }
        }else{
            collector.collect(event);
        }
    }


}
