package ch.ethz.infsec.util;

import ch.ethz.infsec.policy.VariableID;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class PrettyPrint implements FlatMapFunction<PipelineEvent, PipelineEvent> {

    List<VariableID> freeVariablesInOrder;
    List<VariableID> freeVariablesInOrderUnique;
    List<Integer> indexList;

    public PrettyPrint(List<VariableID> freeVariablesInOrder) {
        this.freeVariablesInOrder = freeVariablesInOrder;
        this.freeVariablesInOrderUnique = freeVariablesInOrder.stream().distinct().collect(Collectors.toList());
        indexList = new ArrayList<>();
        for (VariableID fvio: freeVariablesInOrderUnique) {
            indexList.add(freeVariablesInOrder.indexOf(fvio));
        }
    }

    @Override
    public void flatMap(PipelineEvent event, Collector<PipelineEvent> out) throws Exception {

        if (event.isPresent()) {
            if (this.freeVariablesInOrder.isEmpty()) {
                out.collect(PipelineEvent.event(event.getTimestamp(), event.getTimepoint(), Assignment.one(java.util.Optional.of(true))));
            }
            else {
                Assignment assignment = new Assignment();
                for (Integer index: indexList) {
                    assignment.add(event.get().get(index));
                }
                out.collect(PipelineEvent.event(event.getTimestamp(), event.getTimepoint(), assignment));
            }
        }
    }
}
