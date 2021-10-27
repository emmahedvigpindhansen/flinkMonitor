package ch.ethz.infsec.util;

import ch.ethz.infsec.slicer.ColissionlessKeyGenerator;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import scala.collection.JavaConverters;

import java.util.Map;

public class DuplicateTerminators implements FlatMapFunction<PipelineEvent, PipelineEvent> {

    int numberProcessors;
    Map<Object, Object> mapping;

    public DuplicateTerminators(int numberProcessors, Map<Object, Object> mapping) {
        this.numberProcessors = numberProcessors;
        this.mapping = mapping;
    }

    @Override
    public void flatMap(PipelineEvent event, Collector<PipelineEvent> out) throws Exception {

        if (!event.isPresent()) {
            for (int i = 0; i < this.numberProcessors; i++) {
                PipelineEvent temp = new PipelineEvent(event.getTimestamp(), event.getTimepoint(), true, event.get());
                temp.key = (int) this.mapping.get(i);
                out.collect(temp);
            }
        } else {
            event.key = (int) this.mapping.get(0); // randomize this?? if so do from MformulaVisitorFlink
            out.collect(event);
        }
    }
}
