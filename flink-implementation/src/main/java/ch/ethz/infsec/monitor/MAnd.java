package ch.ethz.infsec.monitor;
import java.util.*;

import ch.ethz.infsec.policy.VariableID;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.util.Collector;

import ch.ethz.infsec.util.*;
import ch.ethz.infsec.monitor.visitor.*;

import static ch.ethz.infsec.util.Table.join1;

public class MAnd implements Mformula, CoFlatMapFunction<PipelineEvent, PipelineEvent, PipelineEvent> {

    boolean bool;
    public Mformula op1;
    public Mformula op2;
    public Integer indexOfCommonKey;
    Tuple<HashMap<Long, Table>,HashMap<Long, Table>> mbuf2;
    HashSet<Long> terminatorLHS;
    HashSet<Long> terminatorRHS;
    HashMap<Long, Integer> terminatorCount1;
    HashMap<Long, Integer> terminatorCount2;
    Integer numberProcessors;


    public MAnd(Mformula arg1, boolean bool, Mformula arg2, Integer indexOfCommonKey) {
        this.bool = bool;
        this.op1 = arg1;
        this.op2 = arg2;
        this.indexOfCommonKey = indexOfCommonKey;

        this.mbuf2 = new Tuple<>(new HashMap<>(), new HashMap<>());
        terminatorLHS = new HashSet<>();
        terminatorRHS = new HashSet<>();
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

        //here we have a streaming implementation. We can produce an output potentially for every event.
        //We don't buffer events until we receive a terminator event, contrary to what the Verimon algorithm does.

        if(!event.isPresent()){

            if (!terminatorCount1.containsKey(event.getTimepoint())) {
                terminatorCount1.put(event.getTimepoint(), 1);
            } else {
                terminatorCount1.put(event.getTimepoint(), terminatorCount1.get(event.getTimepoint()) + 1);
            }
            // only add terminator to LHS when received correct amount
            if (terminatorCount1.containsKey(event.getTimepoint())) {
                if (terminatorCount1.get(event.getTimepoint()).equals(this.op1.getNumberProcessors())) {
                    terminatorLHS.add(event.getTimepoint());
                }
            }

            // EH :  remove cleanup for parallelism to work - cleanup when time
            if(terminatorRHS.contains(event.getTimepoint())){
                //this.mbuf2.fst.remove(event.getTimepoint());
                //this.mbuf2.snd.remove(event.getTimepoint());
                // Only one terminator needs to be output
                collector.collect(event);
                //terminatorRHS.remove(event.getTimepoint());
                //terminatorLHS.remove(event.getTimepoint());
            }
        }else if(!terminatorLHS.contains(event.getTimepoint())){
            if(!this.mbuf2.fst.containsKey(event.getTimepoint())){
                this.mbuf2.fst.put(event.getTimepoint(), Table.empty());
            }
            this.mbuf2.fst.get(event.getTimepoint()).add(event.get());

            if(mbuf2.snd.containsKey(event.getTimepoint()) &&  !this.mbuf2.snd.get(event.getTimepoint()).isEmpty()){ //maybe it only contains a terminator :(
                for(Assignment rhs : this.mbuf2.snd.get(event.getTimepoint())){
                    Optional<Assignment> joinResult = join1(event.get(), rhs, 0);
                    if(joinResult.isPresent()){
                        PipelineEvent result = PipelineEvent.event(event.getTimestamp(),event.getTimepoint(), joinResult.get());
                        collector.collect(result);
                    }
                }
            }
        }
    }

    @Override
    public void flatMap2(PipelineEvent event, Collector<PipelineEvent> collector) throws Exception {
        //one terminator event has to be sent out once it is received on both incoming streams

        if(!event.isPresent()){

            if (!terminatorCount2.containsKey(event.getTimepoint())) {
                terminatorCount2.put(event.getTimepoint(), 1);
            } else {
                terminatorCount2.put(event.getTimepoint(), terminatorCount2.get(event.getTimepoint()) + 1);
            }

            // only add terminator to RHS when received correct amount
            if (terminatorCount2.containsKey(event.getTimepoint())) {
                if (terminatorCount2.get(event.getTimepoint()).equals(this.op2.getNumberProcessors())) {
                    terminatorRHS.add(event.getTimepoint());
                }
            }

            // EH :  remove cleanup for parallelism to work - cleanup when time
            if(terminatorLHS.contains(event.getTimepoint())){
                //this.mbuf2.fst.remove(event.getTimepoint());
                //this.mbuf2.snd.remove(event.getTimepoint());
                // Only one terminator needs to be output
                collector.collect(event);
                //terminatorRHS.remove(event.getTimepoint());
                //terminatorLHS.remove(event.getTimepoint());
            }

        }else if(!terminatorRHS.contains(event.getTimepoint())){
            if(!this.mbuf2.snd.containsKey(event.getTimepoint())){
                this.mbuf2.snd.put(event.getTimepoint(), Table.empty());
            }
            this.mbuf2.snd.get(event.getTimepoint()).add(event.get());

            if(mbuf2.fst.containsKey(event.getTimepoint()) && !this.mbuf2.fst.get(event.getTimepoint()).isEmpty()){
                for(Assignment lhs : this.mbuf2.fst.get(event.getTimepoint())){
                    Optional<Assignment> joinResult = join1(event.get(), lhs, 0);
                    if(joinResult.isPresent()){
                        PipelineEvent result = PipelineEvent.event(event.getTimestamp(),event.getTimepoint(), joinResult.get());
                        collector.collect(result);
                    }
                }
            }

        }
    }


}
