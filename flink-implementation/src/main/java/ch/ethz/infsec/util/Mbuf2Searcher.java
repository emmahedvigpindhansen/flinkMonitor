package ch.ethz.infsec.util;

import org.apache.flink.util.Collector;

import java.util.HashMap;

public interface Mbuf2Searcher {

    Long updateTP(Long tp);

    boolean checkIntervalCondition(HashMap<Long, Long> timepointToTimestamp, PipelineEvent event, Long tp,
                                   ch.ethz.infsec.policy.Interval interval);

    default void addToSatisfactions(HashMap<Long, Table> satisfactions, Assignment assignment, Long tp) {
        if (satisfactions.containsKey(tp)) {
            satisfactions.get(tp).add(assignment);
        } else {
            satisfactions.put(tp, Table.one(assignment));
        }
    }

    default void outputJoinResult(Table result, Collector<PipelineEvent> collector, HashMap<Long, Table> satisfactions,
                                  HashMap<Long, Long> timepointToTimestamp, Long tp) {
        for (Assignment assignment: result) {
            collector.collect(PipelineEvent.event(timepointToTimestamp.get(tp), tp, assignment));
            addToSatisfactions(satisfactions, assignment, tp);
        }
    }

    default boolean outputJoinResult2(Table result, Collector<PipelineEvent> collector, HashMap<Long, Table> satisfactions,
                                      Table alphas, HashMap<Long, Long> timepointToTimestamp, Long tp) {
        Table result2 = Table.join(alphas, true, result);
        if (!result2.isEmpty()) {
            outputJoinResult(result2, collector, satisfactions, timepointToTimestamp, tp);
            return true;
        } else {
            return false; // we only want consecutive assignments
        }
    }

    default void searchMbuf2ForBeta(PipelineEvent event, Collector<PipelineEvent> collector,
                                    HashMap<Long, Table> satisfactions, HashMap<Long, Long> timepointToTimestamp,
                                    HashMap<Long, Table> alphas, ch.ethz.infsec.policy.Interval interval, Long tp, Long tp2) {
        if (satisfactions.containsKey(tp)) {
            Table evalSet = satisfactions.get(tp);
            Table result = Table.join(Table.one(event.get()), true, evalSet);
            if (!result.isEmpty()) {
                outputJoinResult(result, collector, satisfactions, timepointToTimestamp, tp);
                // search right in mbuf2 for consecutive (non-outputted) assignments
                outputConsecutiveAlphaAssignments(event, collector, result, satisfactions, timepointToTimestamp,
                        alphas, interval, tp2);
            }
        }
    }

    default void outputConsecutiveAlphaAssignments(PipelineEvent event, Collector<PipelineEvent> collector, Table result,
                                                   HashMap<Long, Table> satisfactions, HashMap<Long, Long> timepointToTimestamp,
                                                   HashMap<Long, Table> alphas, ch.ethz.infsec.policy.Interval interval, Long tp) {
        while (alphas.containsKey(tp) && checkIntervalCondition(timepointToTimestamp, event, tp, interval)) {
            if (outputJoinResult2(result, collector, satisfactions, alphas.get(tp), timepointToTimestamp, tp)) {
                tp = updateTP(tp);
            }
            else {
                break;
            }
        }
    }

    default void searchMbuf2ForAlpha(PipelineEvent event, Collector<PipelineEvent> collector, HashMap<Long, Table> satisfactions,
                                     HashMap<Long, Long> timepointToTimestamp, ch.ethz.infsec.policy.Interval interval,
                                     HashMap<Long, Table> alphas, Long tp) {
        while (alphas.containsKey(tp) && checkIntervalCondition(timepointToTimestamp, event, tp, interval)) {
            // check that assignments match
            Table result = Table.join(Table.one(event.get()), true, alphas.get(tp));
            if (!result.isEmpty()) {
                outputJoinResult(result, collector, satisfactions, timepointToTimestamp, tp);
                tp = updateTP(tp);
            } else { // break if no join result in any assignments (only want consecutive assignments)
                break;
            }
        }
    }
}
