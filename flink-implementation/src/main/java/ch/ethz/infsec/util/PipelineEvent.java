package ch.ethz.infsec.util;


import ch.ethz.infsec.policy.VariableID;

import java.io.Serializable;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.List;

public class PipelineEvent implements Serializable {

    private final long timestamp;
    private final long timepoint;
    private final boolean isTerminator;
    private final Assignment assignment;
    int key;


    public PipelineEvent(long timestamp, long timepoint, boolean isTerminator, Assignment assignment) {
        assert(!isTerminator || assignment==null);
        assert(isTerminator || assignment!=null);

        this.isTerminator = isTerminator;
        this.timestamp = timestamp;
        this.assignment = assignment;
        this.timepoint = timepoint;
    }
    public static PipelineEvent terminator(long timestamp, long timepoint) {
        return new PipelineEvent(timestamp,timepoint,true,null);
    }

    public static PipelineEvent event(long timestamp, long timepoint, Assignment assignment) {
        return new PipelineEvent(timestamp,timepoint,false,assignment);
    }

    public boolean isPresent(){
        return !isTerminator;
    }

    public Assignment get(){
        return this.assignment;
    }

    public long getTimestamp(){
        return this.timestamp;
    }

    public long getTimepoint(){
        return this.timepoint;
    }

    public int getKey() {return this.key;}

    public void setKey(int key) {this.key = key;}

    @Override
    public boolean equals(Object o){
        if(o == this){
            return true;
        }
        if(!(o instanceof PipelineEvent)){
            return false;
        }
        PipelineEvent pe = (PipelineEvent) o;
        if(this.isTerminator != pe.isTerminator){
            return false;
        }else{
            if(!this.isTerminator){
                return this.timestamp == pe.timestamp
                        && this.timepoint == pe.timepoint
                        && this.get().equals(pe.get());
            }else{
                return this.timestamp == pe.timestamp
                        && this.timepoint == pe.timepoint;
            }
        }


    }

    @Override
    public String toString() {
        if (isTerminator()) {
            return "@" + timestamp + ". (time point : " + timepoint + ")";
        } else {
            return "@" + timestamp + ". (time point " + timepoint + "): " + assignment.toString();
        }
    }

    private boolean isTerminator() {
        return isTerminator;
    }


}