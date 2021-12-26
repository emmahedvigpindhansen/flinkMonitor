package ch.ethz.infsec.monitor;
import java.util.*;

import ch.ethz.infsec.policy.VariableID;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.util.Collector;

import ch.ethz.infsec.util.*;
import ch.ethz.infsec.monitor.visitor.*;

import static ch.ethz.infsec.util.Table.join1;

public class  MAnd implements Mformula, CoFlatMapFunction<PipelineEvent, PipelineEvent, PipelineEvent> {

    boolean bool;
    public Mformula formula1;
    public Mformula formula2;
    public Integer indexOfCommonKey;
    Tuple<HashMap<Long, Table>,HashMap<Long, Table>> mbuf2;
    HashSet<Long> terminatorsLHS;
    HashSet<Long> terminatorsRHS;
    HashMap<Long, Integer> terminatorCountLHS;
    HashMap<Long, Integer> terminatorCountRHS;
    Integer numberProcessors;

    public MAnd(Mformula arg1, boolean bool, Mformula arg2, Integer indexOfCommonKey) {
        this.bool = bool;
        this.formula1 = arg1;
        this.formula2 = arg2;
        this.indexOfCommonKey = indexOfCommonKey;

        this.mbuf2 = new Tuple<>(new HashMap<>(), new HashMap<>());
        terminatorsLHS = new HashSet<>();
        terminatorsRHS = new HashSet<>();
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

        if(!event.isPresent()){

            if (!terminatorCountLHS.containsKey(event.getTimepoint())) {
                terminatorCountLHS.put(event.getTimepoint(), 1);
            } else {
                terminatorCountLHS.put(event.getTimepoint(), terminatorCountLHS.get(event.getTimepoint()) + 1);
            }
            // only add terminator to LHS when received correct amount
            if (terminatorCountLHS.containsKey(event.getTimepoint())) {
                if (terminatorCountLHS.get(event.getTimepoint()).equals(this.formula1.getNumberProcessors())) {
                    terminatorsLHS.add(event.getTimepoint());
                }
            }

            // Only one terminator needs to be output
            if(terminatorsLHS.contains(event.getTimepoint()) && terminatorsRHS.contains(event.getTimepoint())){
                collector.collect(event);
                terminatorsRHS.remove(event.getTimepoint());
                terminatorsLHS.remove(event.getTimepoint());
            }

        } else {
            if (mbuf2.fst().containsKey(event.getTimepoint())) {
                mbuf2.fst().get(event.getTimepoint()).add(event.get());
            } else {
                mbuf2.fst().put(event.getTimepoint(), Table.one(event.get()));
            }

            if (bool) {
                if (mbuf2.snd.containsKey(event.getTimepoint())) {
                    for (Assignment lhs : this.mbuf2.snd.get(event.getTimepoint())) {
                        Optional<Assignment> joinResult = join1(event.get(), lhs, 0);
                        if (joinResult.isPresent()) {
                            PipelineEvent result = PipelineEvent.event(event.getTimestamp(), event.getTimepoint(), joinResult.get());
                            collector.collect(result);
                        }
                    }
                }
            }
            // if received all terminators for not event - output this event directly if no match
            if (!bool && terminatorsRHS.contains(event.getTimepoint())) {
                if (mbuf2.snd.containsKey(event.getTimepoint())) {
                    Table result = Table.join(Table.one(event.get()), bool, mbuf2.snd.get(event.getTimepoint()));
                    if (!result.isEmpty()) {
                        collector.collect(event);
                    }
                } else {
                    collector.collect(event);
                }
            }
        }
    }

    @Override
    public void flatMap2(PipelineEvent event, Collector<PipelineEvent> collector) throws Exception {

        if(!event.isPresent()){

            if (!terminatorCountRHS.containsKey(event.getTimepoint())) {
                terminatorCountRHS.put(event.getTimepoint(), 1);
            } else {
                terminatorCountRHS.put(event.getTimepoint(), terminatorCountRHS.get(event.getTimepoint()) + 1);
            }

            // only add terminator to RHS when received correct amount
            if (terminatorCountRHS.containsKey(event.getTimepoint())) {
                if (terminatorCountRHS.get(event.getTimepoint()).equals(this.formula2.getNumberProcessors())) {
                    terminatorsRHS.add(event.getTimepoint());
                }
            }
            // if received all terminators for not event - output satisfactions
            if (!bool && terminatorsRHS.contains(event.getTimepoint())) {
                if (mbuf2.fst.containsKey(event.getTimepoint())) {
                    if (mbuf2.snd.containsKey(event.getTimepoint())) {
                        Table result = Table.join(mbuf2.fst.get(event.getTimepoint()), bool, mbuf2.snd.get(event.getTimepoint()));
                        if (!result.isEmpty()) {
                            for (Assignment assignment : result) {
                                collector.collect(PipelineEvent.event(event.getTimestamp(), event.getTimepoint(), assignment));
                            }
                        }
                    } else {
                        for (Assignment assignment : mbuf2.fst.get(event.getTimepoint())) {
                            collector.collect(PipelineEvent.event(event.getTimestamp(), event.getTimepoint(), assignment));
                        }
                    }
                }
            }

            // Only one terminator needs to be output
            if(terminatorsLHS.contains(event.getTimepoint()) && terminatorsRHS.contains(event.getTimepoint())){
                collector.collect(event);
                terminatorsRHS.remove(event.getTimepoint());
                terminatorsLHS.remove(event.getTimepoint());
            }

        }else {

            if (mbuf2.snd().containsKey(event.getTimepoint())) {
                mbuf2.snd().get(event.getTimepoint()).add(event.get());
            } else {
                mbuf2.snd().put(event.getTimepoint(), Table.one(event.get()));
            }

            if (bool) {
                if (mbuf2.fst.containsKey(event.getTimepoint())) {
                    for (Assignment lhs : this.mbuf2.fst.get(event.getTimepoint())) {
                        Optional<Assignment> joinResult = join1(event.get(), lhs, 0);
                        if (joinResult.isPresent()) {
                            PipelineEvent result = PipelineEvent.event(event.getTimestamp(), event.getTimepoint(), joinResult.get());
                            collector.collect(result);
                        }
                    }
                }
            }
        }
    }
}
