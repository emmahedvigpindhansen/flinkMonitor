package ch.ethz.infsec.monitor;
import ch.ethz.infsec.Main;
import ch.ethz.infsec.policy.Interval;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.util.Collector;

import java.util.*;

import ch.ethz.infsec.util.*;
import ch.ethz.infsec.monitor.visitor.*;


public class MOnce implements Mformula, FlatMapFunction<PipelineEvent, PipelineEvent> {

    public ch.ethz.infsec.policy.Interval interval;
    public Mformula formula;

    HashMap<Long, HashSet<Assignment>> buckets; //indexed by timepoints, stores assignments that will be analyzed by future timepoints
    HashMap<Long, Long> timepointToTimestamp;
    HashMap<Long, Long> terminators;
    Long largestInOrderTP;
    Long largestInOrderTS;
    HashMap<Long, HashSet<Assignment>> outputted;
    HashMap<Long, Integer> terminatorCount;

    Integer numberProcessors;

    public MOnce(ch.ethz.infsec.policy.Interval interval, Mformula mform) {
        this.formula = mform;
        this.interval = interval;
        outputted = new HashMap<>();

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

        if(event.isPresent()){
            if(!buckets.containsKey(event.getTimepoint())){
                buckets.put(event.getTimepoint(), Table.one(event.get()));
            }else{
                buckets.get(event.getTimepoint()).add(event.get());
            }

        } else{

            if (!terminatorCount.containsKey(event.getTimepoint())) {
                terminatorCount.put(event.getTimepoint(), 1);
            } else {
                terminatorCount.put(event.getTimepoint(), terminatorCount.get(event.getTimepoint()) + 1);
            }
            // only add terminator when received correct amount
            if ((terminatorCount.get(event.getTimepoint()).equals(this.formula.getNumberProcessors()))) {
                terminators.put(event.getTimepoint(), event.getTimestamp());
            }
        }

        if (event.isPresent()) {
            for(Long term : terminators.keySet()){
                if(IntervalCondition.mem2(terminators.get(term) - event.getTimestamp(), interval)){
                    PipelineEvent result = PipelineEvent.event(terminators.get(term), term, event.get());
                    out.collect(result);
                }
            }

        } else {
            Long termtp = event.getTimepoint();
            for (Long tp : buckets.keySet()){
                if(IntervalCondition.mem2(terminators.get(termtp) - timepointToTimestamp.get(tp), interval)){
                    HashSet<Assignment> satisfEvents = buckets.get(tp);
                    for(Assignment pe : satisfEvents){
                        PipelineEvent result = PipelineEvent.event(terminators.get(termtp), termtp, pe);
                        out.collect(result);
                    }
                }
            }
            while(terminators.containsKey(largestInOrderTP + 1L)){
                largestInOrderTP++;
                largestInOrderTS = terminators.get(largestInOrderTP);
                // output terminator
                PipelineEvent terminator = PipelineEvent.terminator(terminators.get(largestInOrderTP), largestInOrderTP);
                out.collect(terminator);
            }
        }

        cleanUpDatastructures();

    }

    private void cleanUpDatastructures(){ // need to clean up terminators!

        buckets.keySet().removeIf(tp -> interval.upper().isDefined()
                && timepointToTimestamp.get(tp).intValue() + (int)interval.upper().get() < largestInOrderTS.intValue());


        HashSet<Long> toRemove = new HashSet<>();
        HashSet<Long> toRemoveTPTS = new HashSet<>();
        HashSet<Long> toRemoveBuckets = new HashSet<>();

        /*terminators.keySet().removeIf(tp -> terminators.containsKey(tp)
                && terminators.get(tp).intValue() + (int)interval.upper().get() < largestInOrderTS.intValue());
        */

        // terminatorCount.keySet().removeIf(tp -> tp < largestInOrderTP);


        /*timepointToTimestamp.keySet().removeIf(tp ->
                interval.upper().isDefined() && timepointToTimestamp.get(tp).intValue() + (int)interval.upper().get() < largestInOrderTS.intValue());
        */

        /*for(Long term : terminators.keySet()){

            System.out.println("term : " + term);

            //we only consider terminators and not buckets because we evaluate wrt largestInOrderTP
            if(terminators.containsKey(term) && terminators.get(term).intValue() + (int)interval.upper().get() <= largestInOrderTS.intValue() ){
                System.out.println("remove");
                toRemove.add(term);
            }

        }
        for(Long tp : toRemove){
            terminators.remove(tp);
            // terminatorCount.remove(tp);
        }*/

        /*for(Long buc : buckets.keySet()){
            if(interval.upper().isDefined() && timepointToTimestamp.get(buc).intValue() + (int)interval.upper().get() < largestInOrderTS.intValue()){
                toRemoveBuckets.add(buc);
                toRemoveTPTS.add(buc);
            }
        }

        for(Long tp : toRemoveBuckets){
            buckets.remove(tp);
        }

        for(Long tp : toRemoveTPTS){
            timepointToTimestamp.remove(tp);
        }*/

    }
}

