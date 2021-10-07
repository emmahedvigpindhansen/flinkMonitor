package ch.ethz.infsec.formula;

import ch.ethz.infsec.policy.GenFormula;
import ch.ethz.infsec.policy.Interval;
import ch.ethz.infsec.policy.Eventually;
import ch.ethz.infsec.policy.VariableID;

import ch.ethz.infsec.formula.visitor.FormulaVisitor;
import static ch.ethz.infsec.formula.JavaGenFormula.convert;

public class JavaEventually<T> extends Eventually<T> implements JavaGenFormula<T> {

    public JavaEventually(Interval interval, GenFormula<T> arg) {
        super(interval, arg);
    }
    public <R> R accept(FormulaVisitor<R> v) {
        return v.visit((JavaEventually<VariableID>) this);
    }

    @Override
    public JavaGenFormula<T> arg(){
        return convert(super.arg());
    }

    @Override
    public Object productElement(int n) {
        return null;
    }

    @Override
    public int productArity() {
        return 0;
    }

    @Override
    public boolean canEqual(Object that) {
        return false;
    }
}

