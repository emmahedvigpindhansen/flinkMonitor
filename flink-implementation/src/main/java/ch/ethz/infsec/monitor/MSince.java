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


    public MSince(boolean b, Mformula accept, ch.ethz.infsec.policy.Interval interval, Mformula accept1) {
        this.pos = b;
        this.formula1 = accept;
        this.formula2 = accept1;
        this.interval = interval;

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

            System.out.println("1 : " + event.toString());

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

            System.out.println("mbuf2 fst : ");
            System.out.println(mbuf2.fst.keySet());
            System.out.println(mbuf2.fst.values());
            System.out.println("mbuf2 snd: ");
            System.out.println(mbuf2.snd.keySet());
            System.out.println(mbuf2.snd.values());
            System.out.println("satisfactions: ");
            System.out.println(satisfactions.keySet());
            System.out.println(satisfactions.values());


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
            }
            /*largestInOrderTS = timepointToTimestamp.get(largestInOrderTP);
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
            startEvalTimepoint = timestampToTimepoint.get(nearest);*/

            /*while(terminLeft.contains(largestInOrderTPsub1 + 1L)){
                largestInOrderTPsub1++;
            }
            if(largestInOrderTPsub2 >= startEvalTimepoint && largestInOrderTPsub1>= startEvalTimepoint &&
                    !(largestInOrderTPsub2 == -1 || largestInOrderTPsub1 == -1)){
                Mbuf2take_function func = (Table r1,
                                           Table r2,
                                           Long t, HashMap<Long, Table> zsAux) -> {Table us_result = update_since(r1, r2, timepointToTimestamp.get(t));
                    HashMap<Long, Table> intermRes = new HashMap<>(zsAux); intermRes.put(t, us_result);
                    return intermRes;};

                HashMap<Long, Table> msaux_zs = mbuf2t_take(func,new HashMap<>(), startEvalTimepoint);

                Long outResultTP = startEvalTimepoint;
                while(msaux_zs.containsKey(outResultTP)){
                    Table evalSet = msaux_zs.get(outResultTP);
                    for(Assignment oa : evalSet){

                        collector.collect(PipelineEvent.event(timepointToTimestamp.get(outResultTP), outResultTP, oa));

                    }
                    collector.collect(PipelineEvent.terminator(timepointToTimestamp.get(outResultTP), outResultTP));
                    outResultTP++;
                }
                startEvalTimepoint += msaux_zs.size();
            }*/
        }
        // cleanUpDatastructures();

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

            System.out.println("2 : " + event.toString());

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

            System.out.println("mbuf2 fst : ");
            System.out.println(mbuf2.fst.keySet());
            System.out.println(mbuf2.fst.values());
            System.out.println("mbuf2 snd: ");
            System.out.println(mbuf2.snd.keySet());
            System.out.println(mbuf2.snd.values());
            System.out.println("satisfactions: ");
            System.out.println(satisfactions.keySet());
            System.out.println(satisfactions.values());


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
            }
            /*largestInOrderTS = timepointToTimestamp.get(largestInOrderTP);
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
            startEvalTimepoint = timestampToTimepoint.get(nearest);*/

            /*while(terminRight.contains(largestInOrderTPsub2 + 1L)){
                largestInOrderTPsub2++;
            }
            if(largestInOrderTPsub2 >= startEvalTimepoint && largestInOrderTPsub1>= startEvalTimepoint &&
                    !(largestInOrderTPsub2 == -1 || largestInOrderTPsub1 == -1)){
                Mbuf2take_function func = (Table r1,
                                           Table r2,
                                           Long t, HashMap<Long, Table> zsAux) -> {Table us_result = update_since(r1, r2, timepointToTimestamp.get(t));
                    HashMap<Long, Table> intermRes = new HashMap<>(zsAux);
                    intermRes.put(t, us_result);
                    return intermRes;};
                HashMap<Long, Table> msaux_zs = mbuf2t_take(func, new HashMap<>(), startEvalTimepoint);
                Long outResultTP = startEvalTimepoint;
                while(msaux_zs.containsKey(outResultTP)){
                    Table evalSet = msaux_zs.get(outResultTP);
                    for(Assignment oa : evalSet){

                        collector.collect(PipelineEvent.event(timepointToTimestamp.get(outResultTP), outResultTP, oa));

                    }
                    collector.collect(PipelineEvent.terminator(timepointToTimestamp.get(outResultTP), outResultTP));
                    outResultTP++;
                }
                startEvalTimepoint += msaux_zs.size();

            }*/

        }
        // cleanUpDatastructures();
    }
    // fig 2 in "formally verified, optimized..."
    public Table update_since(Table rel1, Table rel2, Long nt){
        HashMap<Long, Table> auxResult = new HashMap<>();
        HashMap<Long, Table> auxIntervalList = new HashMap<>();

        // EH : update auxIntervalList with rel1 join rel2
        for(Long t : msaux.keySet()){
            Table rel = msaux.get(t);
            Long subtr = nt - t;
            if(!interval.upper().isDefined() || (interval.upper().isDefined() && (subtr.intValue() <= ((int) interval.upper().get())))){
                auxIntervalList.put(t, Table.join(rel, pos, rel1));
            }
        }
        HashMap<Long, Table> auxIntervalList2 = new HashMap<>(auxIntervalList);
        if(auxIntervalList.size() == 0){
            auxResult.put(nt, rel2);
            auxIntervalList2.put(nt,rel2);
        }else{
            Table x = auxIntervalList.get(timepointToTimestamp.get(startEvalTimepoint));
            if(timepointToTimestamp.get(startEvalTimepoint).equals(nt)){
                Table unionSet = Table.fromTable(x);
                unionSet.addAll(rel2);
                auxIntervalList2.put(timepointToTimestamp.get(startEvalTimepoint), unionSet);
            }else{
                auxIntervalList2.put(timepointToTimestamp.get(startEvalTimepoint),x);
                auxIntervalList2.put(nt, rel2);
            }
        }

        Table bigUnion = new Table();
        for(Long t : auxIntervalList2.keySet()){
            Table rel = auxIntervalList2.get(t);
            if(nt - t >= interval.lower()){
                bigUnion.addAll(rel);
            }
        }
        msaux = new HashMap<>(auxIntervalList2);
        return bigUnion;
    }


    public HashMap<Long, Table> mbuf2t_take(Mbuf2take_function func,
                                            HashMap<Long, Table> z,
                                            Long currentTimepoint){
        if(mbuf2.fst().containsKey(currentTimepoint) && mbuf2.snd().containsKey(currentTimepoint) &&
                terminLeft.contains(currentTimepoint) && terminRight.contains(currentTimepoint)){
            Table x = mbuf2.fst().get(currentTimepoint);
            Table y = mbuf2.snd().get(currentTimepoint);
            HashMap<Long, Table> mbuf2t_output = func.run(x,y,currentTimepoint,z);
            currentTimepoint++;
            return mbuf2t_take(func, mbuf2t_output, currentTimepoint);
        }
        else{
            return z;
        }
    }

    private void outputTerminators() {
    }

    public void cleanUpDatastructures(){
        mbuf2.fst.keySet().removeIf(tp -> tp < startEvalTimepoint);
        mbuf2.snd.keySet().removeIf(tp -> tp < startEvalTimepoint);
        satisfactions.keySet().removeIf(tp -> tp < startEvalTimepoint);
        timepointToTimestamp.keySet().removeIf(tp -> tp < startEvalTimepoint);
        timestampToTimepoint.keySet().removeIf(ts -> ts < startEvalTimestamp);
    }

}

interface Mbuf2take_function{
    HashMap<Long, Table> run(Table t1,
                             Table t2,
                             Long ts, HashMap<Long, Table> zs);
}


