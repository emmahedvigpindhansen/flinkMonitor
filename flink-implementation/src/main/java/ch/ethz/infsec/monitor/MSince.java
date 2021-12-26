package ch.ethz.infsec.monitor;

import javafx.scene.control.Tab;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.util.Collector;

import ch.ethz.infsec.util.*;
import ch.ethz.infsec.monitor.visitor.*;
import java.util.*;

public class MSince implements Mformula, CoFlatMapFunction<PipelineEvent, PipelineEvent, PipelineEvent>, Mbuf2Searcher {

    boolean bool; //flag indicating whether left subformula is positive (non-negated)
    public Mformula formula1; //left subformula
    public Mformula formula2; //right subformula
    public Integer indexOfCommonKey;

    ch.ethz.infsec.policy.Interval interval;
    Tuple<HashMap<Long, Table>, HashMap<Long, Table>> mbuf2; //"buf" in Verimon
    HashMap<Long, Table> satisfactions;
    Long largestInOrderTP;
    Long largestInOrderTS;
    Integer numberProcessors;
    HashMap<Long, Long> terminatorsLHS;
    HashMap<Long, Long> terminatorsRHS;
    HashMap<Long, Integer> terminatorCountLHS;
    HashMap<Long, Integer> terminatorCountRHS;
    HashMap<Long, Long> timepointToTimestamp;

    public MSince(boolean b, Mformula accept, ch.ethz.infsec.policy.Interval interval, Mformula accept1, Integer indexOfCommonKey) {
        this.bool = b;
        this.formula1 = accept;
        this.formula2 = accept1;
        this.interval = interval;
        this.indexOfCommonKey = indexOfCommonKey;
        this.satisfactions = new HashMap<>();
        this.timepointToTimestamp = new HashMap<>();
        HashMap<Long, Table> fst = new HashMap<>();
        HashMap<Long, Table> snd = new HashMap<>();
        this.mbuf2 = new Tuple<>(fst, snd);
        this.terminatorsLHS = new HashMap<>();
        this.terminatorsRHS = new HashMap<>();
        largestInOrderTP = -1L;
        largestInOrderTS = -1L;
        this.terminatorCountLHS = new HashMap<>();
        this.terminatorCountRHS = new HashMap<>();
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
    public void flatMap1(PipelineEvent event, Collector<PipelineEvent> collector) throws Exception {

        if(!timepointToTimestamp.containsKey(event.getTimepoint())){
            timepointToTimestamp.put(event.getTimepoint(), event.getTimestamp());
        }

        if(event.isPresent()){

            if(mbuf2.fst().containsKey(event.getTimepoint())){
                mbuf2.fst().get(event.getTimepoint()).add(event.get());
            }else{
                mbuf2.fst().put(event.getTimepoint(), Table.one(event.get()));
            }

            if (this.bool) {
                // see if satisfaction at previous timepoint
                searchMbuf2ForBeta(event, collector, satisfactions, timepointToTimestamp,
                        mbuf2.fst, interval, event.getTimepoint() - 1L, event.getTimepoint() + 1L);
            }

        } else {

            if (!terminatorCountLHS.containsKey(event.getTimepoint())) {
                terminatorCountLHS.put(event.getTimepoint(), 1);
            } else {
                terminatorCountLHS.put(event.getTimepoint(), terminatorCountLHS.get(event.getTimepoint()) + 1);
            }
            // only add terminator when received correct amount
            if ((terminatorCountLHS.get(event.getTimepoint()).equals(this.formula1.getNumberProcessors()))) {
                terminatorsLHS.put(event.getTimepoint(), event.getTimestamp());
            }

            while(terminatorsLHS.containsKey(largestInOrderTP + 1L) && terminatorsRHS.containsKey(largestInOrderTP + 1L)){
                largestInOrderTP++;
                largestInOrderTS = terminatorsLHS.get(largestInOrderTP);
                if (!bool) {
                    // loop through beta events - output if not received corresponding alpha event at this tp
                    outputBetaForLargestInOrderTP(collector);
                }
                // output terminator
                PipelineEvent terminator = PipelineEvent.terminator(terminatorsLHS.get(largestInOrderTP), largestInOrderTP);
                collector.collect(terminator);
            }
        }
        cleanUpDatastructures();
    }


    @Override
    public void flatMap2(PipelineEvent event, Collector<PipelineEvent> collector) throws Exception {

        if(!timepointToTimestamp.containsKey(event.getTimepoint())){
            timepointToTimestamp.put(event.getTimepoint(), event.getTimestamp());
        }

        if(event.isPresent()){

            if(mbuf2.snd().containsKey(event.getTimepoint())){
                mbuf2.snd().get(event.getTimepoint()).add(event.get());
            }else{
                mbuf2.snd().put(event.getTimepoint(), Table.one(event.get()));
            }

            if (this.bool) {
                // always add beta to satisfactions
                addToSatisfactions(this.satisfactions, event.get(), event.getTimepoint());
                collector.collect(event);
                // if alpha (publish) received before beta (approve), check if alpha should be output
                // (search mbuf2 right)
                searchMbuf2ForAlpha(event, collector, satisfactions, timepointToTimestamp, interval,
                        mbuf2.fst, event.getTimepoint() + 1L);
            }

        } else {
            if (!terminatorCountRHS.containsKey(event.getTimepoint())) {
                terminatorCountRHS.put(event.getTimepoint(), 1);
            } else {
                terminatorCountRHS.put(event.getTimepoint(), terminatorCountRHS.get(event.getTimepoint()) + 1);
            }
            // only add terminator when received correct amount
            if ((terminatorCountRHS.get(event.getTimepoint()).equals(this.formula2.getNumberProcessors()))) {
                terminatorsRHS.put(event.getTimepoint(), event.getTimestamp());
            }
            while (terminatorsLHS.containsKey(largestInOrderTP + 1L) && terminatorsRHS.containsKey(largestInOrderTP + 1L)) {
                largestInOrderTP++;
                largestInOrderTS = terminatorsLHS.get(largestInOrderTP);
                if (!bool) {
                    // loop through beta events - output if not received corresponding alfa event at this tp
                    outputBetaForLargestInOrderTP(collector);
                }
                // output terminator
                PipelineEvent terminator = PipelineEvent.terminator(terminatorsLHS.get(largestInOrderTP), largestInOrderTP);
                collector.collect(terminator);
            }
        }
        cleanUpDatastructures();
    }

    private void cleanUpDatastructures(){
        if (interval.upper().isDefined()) {
            mbuf2.fst.keySet().removeIf(tp -> timepointToTimestamp.get(tp).intValue() < largestInOrderTS - (int) interval.upper().get());
            mbuf2.snd.keySet().removeIf(tp -> timepointToTimestamp.get(tp).intValue() < largestInOrderTS - (int) interval.upper().get());
            satisfactions.keySet().removeIf(tp -> timepointToTimestamp.get(tp).intValue() < largestInOrderTS - (int) interval.upper().get());
            timepointToTimestamp.keySet().removeIf(tp -> timepointToTimestamp.get(tp).intValue() < largestInOrderTS - (int) interval.upper().get());
        } else {
            mbuf2.fst.keySet().removeIf(tp -> largestInOrderTS - timepointToTimestamp.get(tp).intValue() < interval.lower());
            mbuf2.snd.keySet().removeIf(tp -> largestInOrderTS - timepointToTimestamp.get(tp).intValue() < interval.lower());
            satisfactions.keySet().removeIf(tp -> largestInOrderTS - timepointToTimestamp.get(tp).intValue() < interval.lower());
            timepointToTimestamp.keySet().removeIf(tp -> largestInOrderTS - timepointToTimestamp.get(tp).intValue() < interval.lower());
        }
        terminatorsLHS.keySet().removeIf(tp -> tp < largestInOrderTP);
        terminatorsRHS.keySet().removeIf(tp -> tp < largestInOrderTP);
    }

    @Override
    public Long updateTP(Long tp) { return tp + 1L;}

    @Override
    public boolean checkIntervalCondition(HashMap<Long, Long> timepointToTimestamp, PipelineEvent event, Long tp,
                                           ch.ethz.infsec.policy.Interval interval) {
        return IntervalCondition.mem2(timepointToTimestamp.get(tp) - event.getTimestamp(), interval);
    }

    private void outputBetaForLargestInOrderTP(Collector<PipelineEvent> collector) {
        for (Long tp : mbuf2.snd.keySet()) {
            if (IntervalCondition.mem2(largestInOrderTS - timepointToTimestamp.get(tp), interval)) {
                for (Assignment beta : mbuf2.snd.get(tp)) {
                    if (mbuf2.fst.containsKey(largestInOrderTP)) {
                        Table result = Table.join(Table.one(beta), bool, mbuf2.fst.get(largestInOrderTP));
                        if (!result.isEmpty()) {
                            collector.collect(PipelineEvent.event(largestInOrderTS, largestInOrderTP, beta));
                        } else {
                            // remove beta from mbuf2
                            mbuf2.snd.get(tp).removeIf(assignment -> assignment.equals(beta));
                        }
                    } else {
                        collector.collect(PipelineEvent.event(largestInOrderTS, largestInOrderTP, beta));
                    }
                }
            }
        }
    }

}


