package ch.ethz.infsec.monitor;
import ch.ethz.infsec.policy.Interval;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.util.Collector;

import java.util.*;
import ch.ethz.infsec.util.*;
import ch.ethz.infsec.monitor.visitor.*;


public class MEventually implements Mformula, FlatMapFunction<PipelineEvent, PipelineEvent> {

    ch.ethz.infsec.policy.Interval interval;
    public Mformula formula;

    HashMap<Long, HashSet<Assignment>> buckets;
    HashMap<Long, Long> timepointToTimestamp;
    HashMap<Long, Long> terminators;
    Long largestInOrderTP;
    Long largestInOrderTS;
    Integer numberProcessors;
    HashMap<Long, Integer> terminatorCount;

    public MEventually(ch.ethz.infsec.policy.Interval interval, Mformula mform) {
        this.formula = mform;
        this.interval = interval;
        this.buckets = new HashMap<>();
        this.timepointToTimestamp = new HashMap<>();
        this.terminators = new HashMap<>();
        this.terminatorCount = new HashMap<>();
        largestInOrderTP = -1L;
        largestInOrderTS = -1L;
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

        if(!timepointToTimestamp.containsKey(event.getTimepoint())){
            timepointToTimestamp.put(event.getTimepoint(), event.getTimestamp());
        }

        if (event.isPresent()) {

            if(!buckets.containsKey(event.getTimepoint())){
                buckets.put(event.getTimepoint(), Table.one(event.get()));
            }else{
                buckets.get(event.getTimepoint()).add(event.get());
            }

        } else {
            if (!terminatorCount.containsKey(event.getTimepoint())) {
                terminatorCount.put(event.getTimepoint(), 1);
            } else {
                terminatorCount.put(event.getTimepoint(), terminatorCount.get(event.getTimepoint()) + 1);
            }
            if (!terminators.containsKey(event.getTimepoint())) {
                terminators.put(event.getTimepoint(), event.getTimestamp());
            }
        }

        if (event.isPresent()) {
            Long tp = event.getTimepoint();
            Long ts = event.getTimestamp();
            for(Long term : terminators.keySet()){
                if(IntervalCondition.mem2(ts - timepointToTimestamp.get(term) , interval)
                    && tp >= term){ // make sure that only previous terminators are output
                    PipelineEvent result = PipelineEvent.event(timepointToTimestamp.get(term), term, event.get());
                    out.collect(result);
                    // System.out.println("eventually result : " + result.toString());
                }
            }
        } else {
            Long termtp = event.getTimepoint();
            for(Long tp : buckets.keySet()){
                if(IntervalCondition.mem2(timepointToTimestamp.get(tp) - timepointToTimestamp.get(termtp), interval)
                    && tp >= termtp){ // make sure that only previous events are output
                    HashSet<Assignment> satisfEvents = buckets.get(tp);
                    for(Assignment pe : satisfEvents){
                        PipelineEvent result = PipelineEvent.event(timepointToTimestamp.get(termtp), termtp, pe);
                        out.collect(result);
                        // System.out.println("eventually result : " + result.toString());
                    }
                }
            }
            while(terminators.containsKey(largestInOrderTP + 1L)
                && terminatorCount.get(largestInOrderTP + 1L).equals(this.formula.getNumberProcessors())){
                largestInOrderTP++;
                largestInOrderTS = terminators.get(largestInOrderTP);
                // output terminator
                PipelineEvent terminator = PipelineEvent.terminator(largestInOrderTS, largestInOrderTP);
                out.collect(terminator);
            }
        }
        cleanUpDatastructures();
    }

    private void cleanUpDatastructures() {

       this.terminators.keySet().removeIf(tp -> timepointToTimestamp.get(tp).intValue() < largestInOrderTS.intValue() - (int) interval.upper().get());

       this.buckets.keySet().removeIf(tp -> timepointToTimestamp.get(tp).intValue() < largestInOrderTS.intValue() - (int) interval.upper().get());

       this.timepointToTimestamp.keySet().removeIf(tp -> timepointToTimestamp.get(tp).intValue() < largestInOrderTS.intValue() - (int) interval.upper().get());
    }
}

