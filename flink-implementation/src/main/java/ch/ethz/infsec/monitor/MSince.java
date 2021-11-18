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
    ch.ethz.infsec.policy.Interval interval;
    public Mformula formula2; //right subformula
    public Integer indexOfCommonKey;
    Tuple<HashMap<Long, Table>, HashMap<Long, Table>> mbuf2; //"buf" in Verimon

    //List<Long> tsList; //"nts" in Verimon
    HashMap<Long, Table> msaux;//"aux" in Verimon
    //for every timestamp, lists the satisfying assignments!
    HashMap<Long, Table> satisfactions;

    Long largestInOrderTPsub1;
    Long largestInOrderTPsub2;
    Long largestInOrderTP;
    Long largestInOrderTS;
    HashSet<Long> terminLeft;
    HashSet<Long> terminRight;
    HashMap<Long, Integer> terminatorCount1;
    HashMap<Long, Integer> terminatorCount2;

    HashMap<Long, Long> timepointToTimestamp;
    HashMap<Long, Long> timestampToTimepoint;

    Long startEvalTimepoint;
    Long startEvalTimestamp;
    Integer numberProcessors;
    Boolean updatedTP1 = false;
    Boolean updatedTP2 = false;


    public MSince(boolean b, Mformula accept, ch.ethz.infsec.policy.Interval interval, Mformula accept1, Integer indexOfCommonKey) {
        this.pos = b;
        this.formula1 = accept;
        this.formula2 = accept1;
        this.interval = interval;
        this.indexOfCommonKey = indexOfCommonKey;

        this.msaux = new HashMap<>();
        this.satisfactions = new HashMap<>();
        this.timepointToTimestamp = new HashMap<>();
        this.timestampToTimepoint = new HashMap<>();
        this.largestInOrderTPsub1 = -1L;
        this.largestInOrderTPsub2 = -1L;
        this.startEvalTimepoint = 0L;
        this.startEvalTimestamp = 0L;
        HashMap<Long, Table> fst = new HashMap<>();
        HashMap<Long, Table> snd = new HashMap<>();
        this.mbuf2 = new Tuple<>(fst, snd);
        this.terminLeft = new HashSet<>();
        this.terminRight = new HashSet<>();
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

        if(!timepointToTimestamp.containsKey(event.getTimepoint())){
            timepointToTimestamp.put(event.getTimepoint(), event.getTimestamp());
        }
        if(!timestampToTimepoint.containsKey(event.getTimestamp())){
            timestampToTimepoint.put(event.getTimestamp(), event.getTimepoint());
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
                for (Assignment assignment : evalSet) {
                    Optional<Assignment> result = Table.join1(event.get(), assignment, 0);
                    if (result.isPresent()) {
                        collector.collect(PipelineEvent.event(this.timepointToTimestamp.get(tp), tp, assignment));
                        if (satisfactions.containsKey(tp)) {
                            satisfactions.get(tp).add(assignment);
                        } else {
                            satisfactions.put(tp, Table.one(assignment));
                        }
                        // search right in mbuf2 for consecutive assignments to output
                        Long tp2 = event.getTimepoint() + 1L;
                        while (mbuf2.fst.containsKey(tp2)
                            && (timepointToTimestamp.get(tp2) - event.getTimestamp() <= (int) interval.upper().get())) {
                            Table evalSet2 = this.mbuf2.fst.get(tp2);
                            for (Assignment assignment2 : evalSet2) {
                                Optional<Assignment> result2 = Table.join1(result.get(), assignment2, 0);
                                if (result2.isPresent()) {
                                    collector.collect(PipelineEvent.event(timepointToTimestamp.get(tp2), tp2, assignment2));
                                    if (satisfactions.containsKey(tp2)) {
                                        satisfactions.get(tp2).add(assignment2);
                                    } else {
                                        satisfactions.put(tp2, Table.one(assignment2));
                                    }
                                }
                            }
                            tp2 += 1L;
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
            if (!terminLeft.contains(event.getTimepoint())) {
                terminLeft.add(event.getTimepoint());
            }
            // update startEvalTimepoint in order to clean up datastructures
            while(terminLeft.contains(largestInOrderTP + 1L) && terminRight.contains(largestInOrderTP + 1L)
                    && (terminatorCount1.get(largestInOrderTP + 1L).equals(this.formula1.getNumberProcessors()))
                    && (terminatorCount2.get(largestInOrderTP + 1L).equals(this.formula2.getNumberProcessors()))){
                largestInOrderTP++;
                updatedTP1 = true;
            }
            if (largestInOrderTP > -1L && updatedTP1) {
                largestInOrderTS = timepointToTimestamp.get(largestInOrderTP);
                startEvalTimestamp = largestInOrderTS - (int) interval.upper().get();
                // find timestamp nearest startEvalTimestamp (from below)
                double minDiff = Double.MAX_VALUE;
                Long nearest = null;
                for (long key : timestampToTimepoint.keySet()) {
                    double diff = startEvalTimestamp - key;
                    if (diff < minDiff && diff > 0) {
                        nearest = key;
                        minDiff = diff;
                    }
                }
                startEvalTimepoint = timestampToTimepoint.containsKey(nearest) ? timestampToTimepoint.get(nearest) : 0L;
                outputTerminators(collector);
            }
        }
        cleanUpDatastructures();
        updatedTP1 = false;
    }


    @Override
    public void flatMap2(PipelineEvent event, Collector<PipelineEvent> collector) throws Exception {

        if(!timepointToTimestamp.containsKey(event.getTimepoint())){
            timepointToTimestamp.put(event.getTimepoint(), event.getTimestamp());
        }
        if(!timestampToTimepoint.containsKey(event.getTimestamp())){
            timestampToTimepoint.put(event.getTimestamp(), event.getTimepoint());
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
                    && (this.timepointToTimestamp.get(tp) - event.getTimestamp() <= (int) interval.upper().get())) {
                // check that assignments match
                Table evalSet = mbuf2.fst.get(tp);
                for (Assignment assignment : evalSet){
                    Optional<Assignment> result = Table.join1(event.get(), assignment, 0);
                    if (result.isPresent()) {
                        collector.collect(PipelineEvent.event(this.timepointToTimestamp.get(tp), tp, assignment));
                        if (satisfactions.containsKey(tp)) {
                            satisfactions.get(tp).add(assignment);
                        } else {
                            satisfactions.put(tp, Table.one(assignment));
                        }
                    }
                }
                tp += 1L;
            }

        } else {
            if (!terminatorCount2.containsKey(event.getTimepoint())) {
                terminatorCount2.put(event.getTimepoint(), 1);
            } else {
                terminatorCount2.put(event.getTimepoint(), terminatorCount2.get(event.getTimepoint()) + 1);
            }
            if(!terminRight.contains(event.getTimepoint())){
                terminRight.add(event.getTimepoint());
            }
            // update startEvalTimestamp in order to clean up datastructures
            while (terminLeft.contains(largestInOrderTP + 1L) && terminRight.contains(largestInOrderTP + 1L)
                    && (terminatorCount1.get(largestInOrderTP + 1L).equals(this.formula1.getNumberProcessors()))
                    && (terminatorCount2.get(largestInOrderTP + 1L).equals(this.formula2.getNumberProcessors()))) {
                largestInOrderTP++;
                updatedTP2 = true;
            }
            if (largestInOrderTP > -1L && updatedTP2) {
                largestInOrderTS = timepointToTimestamp.get(largestInOrderTP);
                startEvalTimestamp = largestInOrderTS - (int) interval.upper().get();
                // find timestamp nearest startEvalTimestamp (from below)
                double minDiff = Double.MAX_VALUE;
                Long nearest = null;
                for (long key : timestampToTimepoint.keySet()) {
                    double diff = startEvalTimestamp - key;
                    if (diff < minDiff && diff > 0) {
                        nearest = key;
                        minDiff = diff;
                    }
                }
                startEvalTimepoint = timestampToTimepoint.containsKey(nearest) ? timestampToTimepoint.get(nearest) : 0L;
                outputTerminators(collector);
            }
        }
        cleanUpDatastructures();
        updatedTP2 = false;
    }

    private void outputTerminators(Collector<PipelineEvent> collector) {
        this.terminLeft.forEach(tp -> {
            if (tp <= this.largestInOrderTP) {
                collector.collect(new PipelineEvent(this.timepointToTimestamp.get(tp), tp, true, null));
            }
        });
        this.terminLeft.removeIf(tp -> tp <= this.largestInOrderTP);

        this.terminRight.forEach(tp -> {
            if (tp <= this.largestInOrderTP) {
                collector.collect(new PipelineEvent(this.timepointToTimestamp.get(tp), tp, true, null));
            }
        });
        this.terminRight.removeIf(tp -> tp <= this.largestInOrderTP);
    }

    private void cleanUpDatastructures(){
        mbuf2.fst.keySet().removeIf(tp -> tp < startEvalTimepoint);
        mbuf2.snd.keySet().removeIf(tp -> tp < startEvalTimepoint);
        satisfactions.keySet().removeIf(tp -> tp < startEvalTimepoint);
        timepointToTimestamp.keySet().removeIf(tp -> tp < startEvalTimepoint);
        timestampToTimepoint.keySet().removeIf(ts -> ts < startEvalTimestamp);
    }

}


