package ch.ethz.infsec.util;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import java.util.concurrent.ThreadLocalRandom;

public class DuplicateTerminators implements FlatMapFunction<PipelineEvent, PipelineEvent> {

    int numberProcessors;
    boolean sameProcessor;
    int randomNumberSameProcessor;

    public DuplicateTerminators(int numberProcessors, boolean sameProcessor) {
        this.numberProcessors = numberProcessors;
        this.sameProcessor = sameProcessor;
        this.randomNumberSameProcessor = ThreadLocalRandom.current().nextInt(0, this.numberProcessors);
    }

    @Override
    public void flatMap(PipelineEvent event, Collector<PipelineEvent> out) throws Exception {

        if (!event.isPresent()) {
            for (int i = 0; i < this.numberProcessors; i++) {
                PipelineEvent temp = new PipelineEvent(event.getTimestamp(), event.getTimepoint(), true, event.get());
                temp.key = i; // change to temp.SetKey(i) in PipelineEvent
                out.collect(temp);
            }
        } else {
            if (sameProcessor) {
                event.key = this.randomNumberSameProcessor;
            }
            else {
                int randomNum = ThreadLocalRandom.current().nextInt(0, this.numberProcessors);
                event.key = randomNum;
            }
            out.collect(event);
        }
    }
}
