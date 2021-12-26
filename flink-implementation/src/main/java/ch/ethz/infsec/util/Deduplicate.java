package ch.ethz.infsec.util;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

// https://stackoverflow.com/questions/56897367/filtering-unique-events-in-apache-flink

public class Deduplicate extends RichFlatMapFunction<PipelineEvent, PipelineEvent> {
    ValueState<Boolean> seen;

    @Override
    public void open(Configuration conf) {
        ValueStateDescriptor<Boolean> desc = new ValueStateDescriptor<>("seen", Types.BOOLEAN);
        seen = getRuntimeContext().getState(desc);
    }

    @Override
    public void flatMap(PipelineEvent event, Collector<PipelineEvent> out) throws Exception {
        if (seen.value() == null) {
            out.collect(event);
            seen.update(true);
        }
    }
}
