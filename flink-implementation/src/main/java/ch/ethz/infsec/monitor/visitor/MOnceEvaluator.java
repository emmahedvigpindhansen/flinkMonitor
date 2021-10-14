package ch.ethz.infsec.monitor.visitor;

import ch.ethz.infsec.util.PipelineEvent;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

public class MOnceEvaluator extends KeyedBroadcastProcessFunction<
        Long, PipelineEvent, PipelineEvent, PipelineEvent> {

    // handle for keyed state (per user)
    ValueState<PipelineEvent> prevActionState;
    // broadcast state descriptor
    MapStateDescriptor<Void, PipelineEvent> patternDesc;

    @Override
    public void open(Configuration conf) {
        // initialize keyed state
        prevActionState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("lastAction", PipelineEvent.class));
        patternDesc =
               new MapStateDescriptor<>("patterns", Types.VOID, Types.GENERIC(PipelineEvent.class));
    }

    /**
     * Called for each user action.
     * Evaluates the current pattern against the previous and
     * current action of the user.
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
        PipelineEvent prevAction = prevActionState.value();
        /*if (pattern != null && prevAction != null) {
            // user had an action before, check if pattern matches
            if (pattern.firstAction.equals(prevAction) &&
                    pattern.secondAction.equals(action.action)) {
                // MATCH
                out.collect(new Tuple2<>(ctx.getCurrentKey(), pattern));
            }
        }*/
        out.collect(event);
        // out.collect(terminator);
        // update keyed state and remember action for next pattern evaluation
        prevActionState.update(event);
    }

    /**
     * Called for each new pattern.
     * Overwrites the current pattern with the new pattern.
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
        out.collect(terminator);
    }

}
