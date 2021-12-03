package ch.ethz.infsec.monitor;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.util.Collector;

import ch.ethz.infsec.util.*;
import ch.ethz.infsec.monitor.visitor.*;
import java.util.*;

public class MSince implements Mformula, CoFlatMapFunction<PipelineEvent, PipelineEvent, PipelineEvent> {

    boolean pos; //flag indicating whether left subformula is positive (non-negated)
    public Mformula formula1; //left subformula
    public Mformula formula2; //right subformula
    public Integer indexOfCommonKey;

    ch.ethz.infsec.policy.Interval interval;
    Tuple<HashMap<Long, Table>, HashMap<Long, Table>> mbuf2; //"buf" in Verimon
    HashMap<Long, Table> satisfactions;
    Long largestInOrderTP;
    Long largestInOrderTS;
    Integer numberProcessors;
    HashMap<Long, Long> terminLeft;
    HashMap<Long, Long> terminRight;
    HashMap<Long, Integer> terminatorCount1;
    HashMap<Long, Integer> terminatorCount2;
    HashMap<Long, Long> timepointToTimestamp;

    public MSince(boolean b, Mformula accept, ch.ethz.infsec.policy.Interval interval, Mformula accept1, Integer indexOfCommonKey) {
        this.pos = b;
        this.formula1 = accept;
        this.formula2 = accept1;
        this.interval = interval;
        this.indexOfCommonKey = indexOfCommonKey;
        this.satisfactions = new HashMap<>();
        this.timepointToTimestamp = new HashMap<>();
        HashMap<Long, Table> fst = new HashMap<>();
        HashMap<Long, Table> snd = new HashMap<>();
        this.mbuf2 = new Tuple<>(fst, snd);
        this.terminLeft = new HashMap<>();
        this.terminRight = new HashMap<>();
        largestInOrderTP = -1L;
        largestInOrderTS = -1L;
        this.terminatorCount1 = new HashMap<>();
        this.terminatorCount2 = new HashMap<>();
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

        System.out.println("incoming 1 : " + event.toString());

        if(!timepointToTimestamp.containsKey(event.getTimepoint())){
            timepointToTimestamp.put(event.getTimepoint(), event.getTimestamp());
        }

        if(event.isPresent()){

            if(mbuf2.fst().containsKey(event.getTimepoint())){
                mbuf2.fst().get(event.getTimepoint()).add(event.get());
            }else{
                mbuf2.fst().put(event.getTimepoint(), Table.one(event.get()));
            }

            // see if satisfaction at previous timepoint
            Long tp = event.getTimepoint() - 1L;
            if (this.satisfactions.containsKey(tp)) {
                Table evalSet = this.satisfactions.get(tp);
                Table result = Table.join(Table.one(event.get()), pos, evalSet);
                if (!result.isEmpty()) {
                    for (Assignment assignment: result) {
                        collector.collect(PipelineEvent.event(this.timepointToTimestamp.get(tp), tp, assignment));
                        if (satisfactions.containsKey(tp)) {
                            satisfactions.get(tp).add(assignment);
                        } else {
                            satisfactions.put(tp, Table.one(assignment));
                        }
                    }
                    // search right in mbuf2 for consecutive (non-outputted) assignments
                    Long tp2 = event.getTimepoint() + 1L;
                    while (mbuf2.fst.containsKey(tp2)
                            && IntervalCondition.mem2(this.timepointToTimestamp.get(tp2) - event.getTimestamp(), interval)) {
                        Table evalSet2 = this.mbuf2.fst.get(tp2);
                        Table result2 = Table.join(evalSet2, pos, result);
                        if (!result2.isEmpty()) {
                            for (Assignment assignment2 : result2) {
                                collector.collect(PipelineEvent.event(this.timepointToTimestamp.get(tp2), tp2, assignment2));
                                if (satisfactions.containsKey(tp2)) {
                                    satisfactions.get(tp2).add(assignment2);
                                } else {
                                    satisfactions.put(tp2, Table.one(assignment2));
                                }
                            }
                            tp2 += 1L;
                        } else {
                            break;
                        }
                    }
                }
            }

        } else {

            if (!terminatorCount1.containsKey(event.getTimepoint())) {
                terminatorCount1.put(event.getTimepoint(), 1);
            } else {
                terminatorCount1.put(event.getTimepoint(), terminatorCount1.get(event.getTimepoint()) + 1);
            }
            // only add terminator when received correct amount
            if ((terminatorCount1.get(event.getTimepoint()).equals(this.formula1.getNumberProcessors()))) {
                terminLeft.put(event.getTimepoint(), event.getTimestamp());
            }
            while(terminLeft.containsKey(largestInOrderTP + 1L) && terminRight.containsKey(largestInOrderTP + 1L)){
                largestInOrderTP++;
                largestInOrderTS = terminLeft.get(largestInOrderTP);
                // output terminator
                PipelineEvent terminator = PipelineEvent.terminator(terminLeft.get(largestInOrderTP), largestInOrderTP);
                collector.collect(terminator);
            }
        }
        cleanUpDatastructures();
    }


    @Override
    public void flatMap2(PipelineEvent event, Collector<PipelineEvent> collector) throws Exception {

        System.out.println("incoming 2 : " + event.toString());

        if(!timepointToTimestamp.containsKey(event.getTimepoint())){
            timepointToTimestamp.put(event.getTimepoint(), event.getTimestamp());
        }

        if(event.isPresent()){

            if(mbuf2.snd().containsKey(event.getTimepoint())){
                mbuf2.snd().get(event.getTimepoint()).add(event.get());
            }else{
                mbuf2.snd().put(event.getTimepoint(), Table.one(event.get()));
            }

            // always add beta to satisfactions
            if(satisfactions.containsKey(event.getTimepoint())){
                satisfactions.get(event.getTimepoint()).add(event.get());
            }else{
                satisfactions.put(event.getTimepoint(), Table.one(event.get()));
            }
            collector.collect(PipelineEvent.event(event.getTimestamp(), event.getTimepoint(), event.get()));

            // if alfa (publish) received before beta (approve), check if alfa should be output
            // (search mbuf2 right)
            Long tp = event.getTimepoint() + 1L;
            while (mbuf2.fst.containsKey(tp)
                    && IntervalCondition.mem2(this.timepointToTimestamp.get(tp) - event.getTimestamp(), interval)) {
                // check that assignments match
                Table result = Table.join(Table.one(event.get()), pos, mbuf2.fst.get(tp));
                if (!result.isEmpty()) { // will result always only contain one entry?
                    for (Assignment assignment: result) {
                        collector.collect(PipelineEvent.event(this.timepointToTimestamp.get(tp), tp, assignment));
                        if (satisfactions.containsKey(tp)) {
                            satisfactions.get(tp).add(assignment);
                        } else {
                            satisfactions.put(tp, Table.one(assignment));
                        }
                    }
                    tp += 1L;
                } else { // break if no join result in any assignments (only want consecutive assignments)
                    break;
                }
            }

        } else {
            if (!terminatorCount2.containsKey(event.getTimepoint())) {
                terminatorCount2.put(event.getTimepoint(), 1);
            } else {
                terminatorCount2.put(event.getTimepoint(), terminatorCount2.get(event.getTimepoint()) + 1);
            }
            // only add terminator when received correct amount
            if ((terminatorCount2.get(event.getTimepoint()).equals(this.formula2.getNumberProcessors()))) {
                terminRight.put(event.getTimepoint(), event.getTimestamp());
            }
            while (terminLeft.containsKey(largestInOrderTP + 1L) && terminRight.containsKey(largestInOrderTP + 1L)) {
                largestInOrderTP++;
                largestInOrderTS = terminLeft.get(largestInOrderTP);
                // output terminator
                PipelineEvent terminator = PipelineEvent.terminator(terminLeft.get(largestInOrderTP), largestInOrderTP);
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
        terminLeft.keySet().removeIf(tp -> tp < largestInOrderTP);
        terminRight.keySet().removeIf(tp -> tp < largestInOrderTP);
    }

}


