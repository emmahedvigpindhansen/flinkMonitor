package ch.ethz.infsec.monitor.visitor;

import ch.ethz.infsec.monitor.MOnce;
import ch.ethz.infsec.monitor.Mformula;
import ch.ethz.infsec.util.Assignment;
import ch.ethz.infsec.util.IntervalCondition;
import ch.ethz.infsec.util.PipelineEvent;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.HashSet;

public class MOnceEvaluator extends KeyedBroadcastProcessFunction<
        Long, PipelineEvent, PipelineEvent, PipelineEvent> {

    public ch.ethz.infsec.policy.Interval interval;
    public Mformula formula;

    HashMap<Long, HashSet<Assignment>> buckets; //indexed by timepoints, stores assignments that will be analyzed by future timepoints
    HashMap<Long, Long> timepointToTimestamp;
    HashMap<Long, Long> terminators;
    Long largestInOrderTP;
    Long largestInOrderTS;
    HashMap<Long, HashSet<Assignment>> outputted;

    // figure out how to use these correctly
    // handle for keyed state (per user)
    ValueState<PipelineEvent> prevActionState;
    // broadcast state descriptor
    MapStateDescriptor<Void, PipelineEvent> patternDesc;

    public MOnceEvaluator(MOnce f) {
        this.formula = f.formula;
        this.interval = f.interval;
        outputted = new HashMap<>();

        this.buckets = new HashMap<>();
        this.timepointToTimestamp = new HashMap<>();
        this.terminators = new HashMap<>();
        largestInOrderTP = -1L;
        largestInOrderTS = -1L;
    }

    // figure out how to use these correctly
    @Override
    public void open(Configuration conf) {
        // initialize keyed state
        prevActionState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("lastAction", PipelineEvent.class));
        patternDesc =
               new MapStateDescriptor<>("patterns", Types.VOID, Types.GENERIC(PipelineEvent.class));
    }

    /**
     * Called for each new non-terminator event.
     */
    @Override
    public void processElement(
            PipelineEvent event,
            ReadOnlyContext ctx,
            Collector<PipelineEvent> out) throws Exception {
        // get current terminator from broadcast state
        /*String terminator = ctx
                .getBroadcastState(this.patternDesc)
                // access MapState with null as VOID default value
                .get(null);*/
        // get previous action of current user from keyed state
        // PipelineEvent prevAction = prevActionState.value();
        /*if (pattern != null && prevAction != null) {
            // user had an action before, check if pattern matches
            if (pattern.firstAction.equals(prevAction) &&
                    pattern.secondAction.equals(action.action)) {
                // MATCH
                out.collect(new Tuple2<>(ctx.getCurrentKey(), pattern));
            }
        }*/
        //out.collect(event);
        // update keyed state and remember action for next pattern evaluation
        //prevActionState.update(event);

        if(!timepointToTimestamp.containsKey(event.getTimepoint())){
            timepointToTimestamp.put(event.getTimepoint(), event.getTimestamp());
        }
        if(!buckets.containsKey(event.getTimepoint())){
            HashSet<Assignment> set = new HashSet<>();
            set.add(event.get());
            buckets.put(event.getTimepoint(), set);
        }else{
            buckets.get(event.getTimepoint()).add(event.get());
        }

        for(Long term : terminators.keySet()){
            if(IntervalCondition.mem2(terminators.get(term) - event.getTimestamp(), interval)){
                out.collect(PipelineEvent.event(terminators.get(term), term, event.get()));
            }
        }
        handleBuffered(out);
    }

    /**
     * Called for each new terminator.
     */
    @Override
    public void processBroadcastElement(
            PipelineEvent terminator,
            Context ctx,
            Collector<PipelineEvent> out) throws Exception {
        // store the new pattern by updating the broadcast state
        // BroadcastState<Void, PipelineEvent> bcState = ctx.getBroadcastState(patternDesc);
        // storing in MapState with null as VOID default value
        // bcState.put(null, terminator);
        // patternDesc.update(terminator);
        //out.collect(terminator);

        if(!terminators.containsKey(terminator.getTimepoint())){
            terminators.put(terminator.getTimepoint(), terminator.getTimestamp());
        }else{
            throw new RuntimeException("cannot receive Terminator twice");
        }
        while(terminators.containsKey(largestInOrderTP + 1L)){
            largestInOrderTP++;
            largestInOrderTS = terminators.get(largestInOrderTP);
        }

        Long termtp = terminator.getTimepoint();
        for(Long tp : buckets.keySet()){
            if(IntervalCondition.mem2(terminators.get(termtp) - timepointToTimestamp.get(tp), interval)){
                HashSet<Assignment> satisfEvents = buckets.get(tp);
                for(Assignment pe : satisfEvents){
                    out.collect(PipelineEvent.event(terminators.get(termtp), termtp, pe));
                }
            }
        }
        handleBuffered(out);
    }

    public void handleBuffered(Collector collector){
        HashSet<Long> toRemove = new HashSet<>();
        HashSet<Long> toRemoveTPTS = new HashSet<>();
        HashSet<Long> toRemoveBuckets = new HashSet<>();

        for(Long term : terminators.keySet()){

            //we only consider terminators and not buckets because we evaluate wrt largestInOrderTP
            if(terminators.containsKey(term) && terminators.get(term).intValue() - interval.lower() <= largestInOrderTS.intValue() ){
                collector.collect(PipelineEvent.terminator(terminators.get(term), term));
                toRemove.add(term);
            }

        }
        for(Long tp : toRemove){
            terminators.remove(tp);
        }

        for(Long buc : buckets.keySet()){
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
        }

    }

}
