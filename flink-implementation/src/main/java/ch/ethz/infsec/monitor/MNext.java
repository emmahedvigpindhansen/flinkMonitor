package ch.ethz.infsec.monitor;
import ch.ethz.infsec.policy.Interval;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.util.Collector;
import java.util.*;
import ch.ethz.infsec.util.*;
import ch.ethz.infsec.monitor.visitor.*;

public class MNext implements Mformula, FlatMapFunction<PipelineEvent, PipelineEvent> {
    //Next tells us the satisfying assignments of the formula for the next position.
    //It anticipate the assignments that we receive by 1.
    // So we simply discard the first assignment that you receive

    ch.ethz.infsec.policy.Interval interval;
    public Mformula formula;
    boolean bool;
    HashMap<Long, HashSet<PipelineEvent>> A; //mapping from timepoint to set of assignments (set of PEs)
    HashMap<Long, Long> timepointToTimestamp; //mapping from timepoint to timestamps for non-terminator events
    HashMap<Long, Long> terminators;//mapping from timepoint to timestamps for terminator events
    Integer numberProcessors;
    HashMap<Long, Integer> terminatorCount;

    public MNext(ch.ethz.infsec.policy.Interval interval, Mformula mform, boolean bool, LinkedList<Long> tsList) {
        this.interval = interval;
        this.formula = mform;
        this.bool = bool;
        this.A = new HashMap<>();
        this.timepointToTimestamp = new HashMap<>();
        this.terminators = new HashMap<>();
        this.terminatorCount = new HashMap<>();
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

        if (!timepointToTimestamp.containsKey(event.getTimepoint())) {
            timepointToTimestamp.put(event.getTimepoint(), event.getTimestamp());
        }

        if (event.isPresent()) {
            if (timepointToTimestamp.keySet().contains(event.getTimepoint() - 1)) {
                if (IntervalCondition.mem2(event.getTimestamp() - timepointToTimestamp.get(event.getTimepoint() - 1), interval)) {
                    out.collect(PipelineEvent.event(timepointToTimestamp.get(event.getTimepoint() - 1),
                            event.getTimepoint() - 1, event.get()));
                }
            } else {
                if (A.keySet().contains(event.getTimepoint())) {
                    A.get(event.getTimepoint()).add(event);
                } else {
                    HashSet<PipelineEvent> hspe = new HashSet<>();
                    hspe.add(event);
                    A.put(event.getTimepoint(), hspe);
                }
            }

        } else {
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
        handleBuffered(event, out);
    }

    public void handleBuffered(PipelineEvent event, Collector<PipelineEvent> out) throws Exception {
        // check next events
        if (A.keySet().contains(event.getTimepoint() + 1)) {
            HashSet<PipelineEvent> eventsAtNext = A.get(event.getTimepoint() + 1);
            for (PipelineEvent buffAss : eventsAtNext){
                if(IntervalCondition.mem2( buffAss.getTimestamp() - event.getTimestamp(), interval)){
                    out.collect(PipelineEvent.event(event.getTimestamp(), event.getTimepoint(),  buffAss.get()));
                }
            }
            A.remove(event.getTimepoint() + 1);
        }
        // output terminators
        if(terminators.containsKey(event.getTimepoint() - 1)){
            if(event.getTimepoint() - 1 == 0L){
                out.collect(PipelineEvent.terminator(terminators.get(0L), event.getTimepoint() - 1));
            }
            out.collect(PipelineEvent.terminator(event.getTimestamp(), event.getTimepoint()));
            terminators.remove(event.getTimepoint() - 1);
            terminatorCount.remove(event.getTimepoint() - 1);
            timepointToTimestamp.keySet().removeIf(tp -> tp <= event.getTimepoint() - 1);
        }
    }

}