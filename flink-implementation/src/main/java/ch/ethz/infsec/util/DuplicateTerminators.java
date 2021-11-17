package ch.ethz.infsec.util;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import java.util.concurrent.ThreadLocalRandom;

public class DuplicateTerminators implements FlatMapFunction<PipelineEvent, PipelineEvent> {

    int numberProcessors;
    int randomNumberSameProcessor;
    Integer indexOfCommonKeys;

    public DuplicateTerminators(int numberProcessors, Integer indexOfCommonKeys) {
        this.numberProcessors = numberProcessors;
        this.indexOfCommonKeys = indexOfCommonKeys;
        this.randomNumberSameProcessor = ThreadLocalRandom.current().nextInt(0, this.numberProcessors);
    }

    @Override
    public void flatMap(PipelineEvent event, Collector<PipelineEvent> out) throws Exception {

        if (!event.isPresent()) {
            for (int i = 0; i < this.numberProcessors; i++) {
                PipelineEvent temp = new PipelineEvent(event.getTimestamp(), event.getTimepoint(), true, event.get());
                temp.setKey(i);
                out.collect(temp);
            }
        } else {
            if (indexOfCommonKeys != null) {
                int key = Integer.parseInt(event.get().get(this.indexOfCommonKeys).get().toString()) % numberProcessors;
                event.setKey(key);
            }
            else {
                int randomNum = ThreadLocalRandom.current().nextInt(0, this.numberProcessors);
                event.setKey(randomNum);
            }
            out.collect(event);
        }
    }
}
