package ch.ethz.infsec.formula.visitor;
import ch.ethz.infsec.policy.*;
import org.apache.flink.api.common.functions.FlatMapFunction;
import scala.collection.JavaConverters;
import scala.collection.Seq;
import java.util.*;
import java.util.stream.Collectors;

import ch.ethz.infsec.formula.*;
import ch.ethz.infsec.monitor.*;
import ch.ethz.infsec.util.*;

import scala.collection.JavaConversions;


public class Init0 implements FormulaVisitor<Mformula> {

    List<VariableID> freeVariablesInOrder;
    Seq<VariableID> fvio;

    public Init0(Seq<VariableID> fvio){
        ArrayList<VariableID> temp = new ArrayList<>(JavaConverters.seqAsJavaList(fvio));
        this.freeVariablesInOrder = temp;
        this.fvio = fvio;
    }

    public Mformula visit(JavaPred<VariableID> f){
        return new MPred(f.relation(), f.args(), this.freeVariablesInOrder);
    }


    public Mformula visit(JavaNot<VariableID> f) {
        List<VariableID> keys = JavaConversions.seqAsJavaList(f.arg().freeVariablesInOrder());
        // check that this is the right way to handle keys in Not case
        if(f.arg() instanceof JavaOr){
            if(((JavaOr<VariableID>) f.arg()).arg1() instanceof JavaNot){
                ArrayList<Object> freeVarsInOrder1 = new ArrayList<>(JavaConverters.seqAsJavaList(f.freeVariablesInOrder()));
                ArrayList<Object> freeVarsInOrder2 = new ArrayList<>(JavaConverters.seqAsJavaList(f.freeVariablesInOrder()));
                boolean isSubset = freeVarsInOrder1.containsAll(freeVarsInOrder2);
                if(isSubset && safe_formula(((JavaOr<VariableID>) f.arg()).arg2())){
                    return new MAnd((((JavaOr<VariableID>)f.arg()).arg1()).accept(new Init0(this.fvio)),
                            false, (((JavaOr<VariableID>) f.arg()).arg2()).accept(new Init0(this.fvio)), keys);
                }else{
                    if(((JavaOr<VariableID>)f.arg()).arg2() instanceof JavaNot){
                        return new MAnd((((JavaOr<VariableID>)f.arg()).arg1()).accept(new Init0(this.fvio)),
                                false, (((JavaOr<VariableID>) f.arg()).arg2()).accept(new Init0(this.fvio)), keys);
                    }else{
                        return null;
                    }
                }
            }else{
                return null;
            }
        }else{
            return null;
        }


    }

    public Mformula visit(JavaAnd<VariableID> f) {
        JavaGenFormula<VariableID> arg1 = f.arg1();
        JavaGenFormula<VariableID> arg2 = f.arg2();
        // get common free vars to use in keyed stream
        List<VariableID> keys = JavaConversions.seqAsJavaList(f.arg1().freeVariablesInOrder());
        List<VariableID> keys2 = JavaConversions.seqAsJavaList(f.arg2().freeVariablesInOrder());
        List<VariableID> commonKeys = keys.stream().filter(keys2::contains).collect(Collectors.toList());

        if(safe_formula(arg2)){
            return new MAnd(arg1.accept(new Init0(this.fvio)), true, arg2.accept(new Init0(this.fvio)), commonKeys);
        }else{
            ArrayList<Object> freeVarsInOrder1 = new ArrayList<>(JavaConverters.seqAsJavaList(f.freeVariablesInOrder()));
            ArrayList<Object> freeVarsInOrder2 = new ArrayList<>(JavaConverters.seqAsJavaList(f.freeVariablesInOrder()));
            boolean isSubset = freeVarsInOrder1.containsAll(freeVarsInOrder2);
            if(arg2 instanceof JavaNot && isSubset){
                return new MAnd(arg1.accept(new Init0(this.fvio)), false, arg2.accept(new Init0(this.fvio)), commonKeys);
            }else{
                return null;
            }
        }

    }

    public Mformula visit(JavaAll<VariableID> f) {
        return null;
    }

    public Mformula visit(JavaEx<VariableID> f) {
        VariableID variable = f.variable();
        List<VariableID> freeVariablesInOrderCopy = new ArrayList<>(this.freeVariablesInOrder);
        freeVariablesInOrderCopy.add(0,variable);
        Seq<VariableID> fvios = JavaConverters.asScalaBufferConverter(freeVariablesInOrderCopy).asScala().toSeq();
        JavaGenFormula<VariableID> subformula = f.arg();
        return new MExists(subformula.accept(new Init0(fvios)), variable);
    }

    public Mformula visit(JavaFalse<VariableID> f) {
        return new MRel(Table.empty());
    }

    public Mformula visit(JavaTrue<VariableID> f) {
        int n = f.freeVariablesInOrder().size();
        return new MRel(Table.one(Assignment.nones(n)));
    }

    public Mformula visit(JavaNext<VariableID> f) {
        return new MNext(f.interval(), (f.arg()).accept(new Init0(this.fvio)), true, new LinkedList<>());
    }

    public Mformula visit(JavaOr<VariableID> f) {
        return new MOr((f.arg1()).accept(new Init0(this.fvio)),
                (f.arg2()).accept(new Init0(this.fvio)));
    }

    public Mformula visit(JavaPrev<VariableID> f) {
        return new MPrev(f.interval(), (f.arg()).accept(new Init0(this.fvio)), true, new LinkedList<>());

    }

    public Mformula visit(JavaSince<VariableID> f) {

        if(safe_formula(f.arg1())){
            return new MSince(true,
                    (f.arg1()).accept(new Init0(this.fvio)),
                    f.interval(),
                    (f.arg2()).accept(new Init0(this.fvio)));
        }else{
            if((f.arg1()) instanceof JavaNot){
                return new MSince(false,
                        (f.arg1()).accept(new Init0(this.fvio)),
                        f.interval(),
                        (f.arg2()).accept(new Init0(this.fvio)));
            }else{
                return null;
            }
        }

    }

    public Mformula visit(JavaUntil<VariableID> f) {

        if(safe_formula(f.arg1())){
            return new MUntil(true,
                    (f.arg1()).accept(new Init0(this.fvio)),
                    f.interval(),
                    (f.arg2()).accept(new Init0(this.fvio)));
        }else{
            if((f.arg1()) instanceof JavaNot){
                return new MUntil(false,
                        (f.arg1()).accept(new Init0(this.fvio)),
                        f.interval(),
                        (f.arg2()).accept(new Init0(this.fvio)));
            }else{
                return null;
            }
        }

    }

    @Override
    public Mformula visit(JavaOnce<VariableID> f) {
        return new MOnce(f.interval(), (f.arg()).accept(new Init0(this.fvio)));
    }

    @Override
    public Mformula visit(JavaEventually<VariableID> f) {
        return new MEventually(f.interval(), (f.arg()).accept(new Init0(this.fvio)));
    }

    public static boolean safe_formula(JavaGenFormula<VariableID> form){
        if(form instanceof JavaPred){
            return true;
        }else if(form instanceof JavaNot && ((JavaNot<VariableID>)form).arg() instanceof JavaOr && ((JavaOr<VariableID>)((JavaNot<VariableID>)form).arg()).arg1() instanceof JavaNot){
            ArrayList<Object> freeVarsInOrder1 = new ArrayList<>(JavaConverters.seqAsJavaList(((JavaOr<VariableID>)((JavaNot<VariableID>)form).arg()).arg1().freeVariablesInOrder()));
            ArrayList<Object> freeVarsInOrder2 = new ArrayList<>(JavaConverters.seqAsJavaList(((JavaOr<VariableID>)((JavaNot<VariableID>)form).arg()).arg2().freeVariablesInOrder()));
            boolean isSubset = freeVarsInOrder1.containsAll(freeVarsInOrder2);
            boolean alternative = false;
            if(((JavaOr<VariableID>)((JavaNot<VariableID>)form).arg()).arg2() instanceof JavaNot){
                alternative = safe_formula(((JavaNot<VariableID>)((JavaOr<VariableID>)((JavaNot<VariableID>)form).arg()).arg2()).arg());
            }
            return ((safe_formula(((JavaOr<VariableID>)((JavaNot<VariableID>)form).arg()).arg1()) && safe_formula(((JavaOr<VariableID>)((JavaNot<VariableID>)form).arg()).arg2()) && isSubset) || alternative);
        }else if(form instanceof JavaOr){
            ArrayList<Object> freeVarsInOrder1 = new ArrayList<>(JavaConverters.seqAsJavaList(((JavaOr<VariableID>)form).arg1().freeVariablesInOrder()));
            ArrayList<Object> freeVarsInOrder2 = new ArrayList<>(JavaConverters.seqAsJavaList(((JavaOr<VariableID>)form).arg2().freeVariablesInOrder()));
            boolean isEqual = freeVarsInOrder1.containsAll(freeVarsInOrder2) && freeVarsInOrder2.containsAll(freeVarsInOrder1);
            return (safe_formula(((JavaOr<VariableID>)form).arg1()) && safe_formula(((JavaOr<VariableID>)form).arg2()) && isEqual);

        }else if(form instanceof JavaEx){
            return safe_formula(((JavaEx<VariableID>) form).arg());
        }else if(form instanceof JavaPrev){
            return safe_formula(((JavaPrev<VariableID>) form).arg());
        }else if(form instanceof JavaNext){
            return safe_formula(((JavaNext<VariableID>) form).arg());
        }else if(form instanceof JavaOnce){
            return true;
        }else if(form instanceof JavaEventually){
            return true;
        }else if(form instanceof JavaSince){
            ArrayList<Object> freeVarsInOrder1 = new ArrayList<>(JavaConverters.seqAsJavaList(((JavaSince<VariableID>) form).arg1().freeVariablesInOrder()));
            ArrayList<Object> freeVarsInOrder2 = new ArrayList<>(JavaConverters.seqAsJavaList(((JavaSince<VariableID>) form).arg2().freeVariablesInOrder()));
            boolean isSubset = freeVarsInOrder2.containsAll(freeVarsInOrder1);
            boolean sff = safe_formula(((JavaSince<VariableID>) form).arg1());
            boolean sfs = safe_formula(((JavaSince<VariableID>) form).arg2());
            boolean alt = false;
            if( ((JavaSince<VariableID>) form).arg1() instanceof JavaNot){
                alt = safe_formula(((JavaNot<VariableID>) ((JavaSince<VariableID>) form).arg1()).arg());
            }
            boolean sec = sff || alt && sfs;
            return isSubset && sec;
        }else if(form instanceof JavaUntil){
            ArrayList<Object> freeVarsInOrder1 = new ArrayList<>(JavaConverters.seqAsJavaList(((JavaUntil<VariableID>) form).arg1().freeVariablesInOrder()));
            ArrayList<Object> freeVarsInOrder2 = new ArrayList<>(JavaConverters.seqAsJavaList(((JavaUntil<VariableID>) form).arg2().freeVariablesInOrder()));
            boolean isSubset = freeVarsInOrder2.containsAll(freeVarsInOrder1);
            boolean sff = safe_formula(((JavaUntil<VariableID>) form).arg1());
            boolean sfs = safe_formula(((JavaUntil<VariableID>) form).arg2());
            boolean alt = false;
            if( ((JavaUntil<VariableID>) form).arg1() instanceof JavaNot){
                alt = safe_formula(((JavaNot<VariableID>) ((JavaUntil<VariableID>) form).arg1()).arg());
            }
            boolean sec = sff || alt && sfs;
            return isSubset && sec;
        }else{
            return false;
        }

    }



}