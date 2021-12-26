package ch.ethz.infsec.monitor;
import java.util.*;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.util.Collector;

import ch.ethz.infsec.util.*;
import ch.ethz.infsec.monitor.visitor.*;



public class MOr implements Mformula, CoFlatMapFunction<PipelineEvent, PipelineEvent, PipelineEvent> {

    public Mformula formula1;
    public Mformula formula2;
    public Integer indexOfCommonKey;
    HashSet<Long> terminatorsLHS;
    HashSet<Long> terminatorsRHS;
    HashMap<Long, Integer> terminatorCountLHS;
    HashMap<Long, Integer> terminatorCountRHS;
    Integer numberProcessors;

    public MOr(Mformula arg1, Mformula arg2, Integer indexOfCommonKey) {
        formula1 = arg1;
        formula2 = arg2;
        this.indexOfCommonKey = indexOfCommonKey;

        terminatorsLHS = new HashSet<>();
        terminatorsRHS = new HashSet<>();
        this.terminatorCountLHS = new HashMap<>();
        this.terminatorCountRHS = new HashMap<>();
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
            if (!terminatorCountLHS.containsKey(event.getTimepoint())) {
                terminatorCountLHS.put(event.getTimepoint(), 1);
            } else {
                terminatorCountLHS.put(event.getTimepoint(), terminatorCountLHS.get(event.getTimepoint()) + 1);
            }
            // only add terminator to LHS when received correct amount
            if (terminatorCountLHS.containsKey(event.getTimepoint())) {
                if (terminatorCountLHS.get(event.getTimepoint()).equals(this.formula1.getNumberProcessors())) {
                    terminatorsLHS.add(event.getTimepoint());
                }
            }
            if(terminatorsRHS.contains(event.getTimepoint())){
                collector.collect(event);
            }
        }else{
            collector.collect(event);
        }
    }

    @Override
    public void flatMap2(PipelineEvent event, Collector<PipelineEvent> collector) throws Exception {
        //one terminator event will be sent out only once it is received on both incoming streams
        if(!event.isPresent()){
            if (!terminatorCountRHS.containsKey(event.getTimepoint())) {
                terminatorCountRHS.put(event.getTimepoint(), 1);
            } else {
                terminatorCountRHS.put(event.getTimepoint(), terminatorCountRHS.get(event.getTimepoint()) + 1);
            }

            // only add terminator to RHS when received correct amount
            if (terminatorCountRHS.containsKey(event.getTimepoint())) {
                if (terminatorCountRHS.get(event.getTimepoint()).equals(this.formula2.getNumberProcessors())) {
                    terminatorsRHS.add(event.getTimepoint());
                }
            }
            if(terminatorsLHS.contains(event.getTimepoint())){
                collector.collect(event);
            }
        }else{
            collector.collect(event);
        }
    }


}
