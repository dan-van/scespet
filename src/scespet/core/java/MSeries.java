package scespet.core.java;

import gsa.esg.mekon.core.Environment;
import gsa.esg.mekon.core.EventGraphObject;
import scala.Function1;
import scala.Tuple2;
import scala.collection.TraversableOnce;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;
import scala.runtime.BoxedUnit;
import scespet.core.Agg;
import scespet.core.AggOut;
import scespet.core.CellAdder;
import scespet.core.GroupedTerm;
import scespet.core.HasVal;
import scespet.core.HasValue;
import scespet.core.MacroTerm;
import scespet.core.PartialBuiltSlicedBucket;
import scespet.core.SliceTriggerSpec;
import scespet.core.Term;
import scespet.core.VectTerm;
import scespet.util.SliceAlign;

import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * @author danvan
 * @version $Id$
 */
public class MSeries<X> {
    private MacroTerm<X> impl;

    public MSeries(MacroTerm<X> impl) {
        this.impl = impl;
    }

    public MSeries(Term<X> impl) {
        this.impl = (MacroTerm<X>) impl;
    }

    public X getValue() {
        return impl.value();
    };

//    public <B extends gsa.esg.mekon.core.Function, OUT> PartialBuiltSlicedBucket<B, OUT> bindTo(Supplier<B> newBFunc, Function1<B, Function1<X, BoxedUnit>> adder, AggOut<B, OUT> aggOut, ClassTag<B> type_b) {
//        return impl.bindTo(newBFunc, adder, aggOut, type_b);
//    }
//
    public <Y, O> MSeries<O> reduce(Supplier<Y> newBFunc, Function<Y, CellAdder<X>> adder, AggOut<Y, O> yOut, ClassTag<Y> yType) {
        Term<O> reduce = impl.reduce(newBFunc::get, adder::apply, yOut, yType);
        return new MSeries<O>((MacroTerm<O>) reduce);
    }

    public HasVal<X> input() {
        return impl.input();
    }

    public <Y> MSeries<Y> flatMap(Function1<X, TraversableOnce<Y>> expand) {
        return impl.flatMap(expand);
    }

    public <Y extends Agg> MSeries<Object> reduce_all(Y y) {
        return impl.reduce_all(y);
    }

    public <T> MSeries<T> filterType(Class<T> clazz) {
        return new MSeries<>(impl.filterType(toClassTag(clazz)));
    }

    public <Y> MSeries<Tuple2<X, Y>> take(MSeries<Y> y) {
        return impl.take(y);
    }

    public MSeries<X> sample(EventGraphObject evt) {
        return impl.sample(evt);
    }

    public GroupedTerm<X> window(HasValue<Object> window) {
        return impl.window(window);
    }

    public boolean initialised() {
        return impl.initialised();
    }

    public <Y> VectTerm<Y, Y> valueSet(Function1<X, TraversableOnce<Y>> expand) {
        return impl.valueSet(expand);
    }

    public <S> GroupedTerm<X> group(S sliceSpec, SliceAlign triggerAlign, SliceTriggerSpec<S> ev) {
        return impl.group(sliceSpec, triggerAlign, ev);
    }

    public EventGraphObject trigger() {
        return impl.trigger();
    }

    public <K> VectTerm<K, X> by(Function1<X, K> f) {
        return impl.by(f);
    }

    public <Y extends Agg> MSeries<Object> fold_all(Y y) {
        return impl.fold_all(y);
    }

    public <Y> MSeries<Tuple2<X, Y>> join(MSeries<Y> y) {
        return impl.join(y);
    }

    public VectTerm<X, X> valueSet() {
        return impl.valueSet();
    }

    public <E extends EventGraphObject> HasVal<E> eventObjectToHasVal(E evtObj) {
        return impl.eventObjectToHasVal(evtObj);
    }

    public Environment env() {
        return impl.env();
    }

    public <Y> MSeries<Y> map(Function1<X, Y> f, boolean exposeNull) {
        return impl.map(f, exposeNull);
    }

    public EventGraphObject getTrigger() {
        return impl.getTrigger();
    }

    public <Y> VectTerm<X, Y> takef(Function1<X, HasVal<Y>> newGenerator) {
        return impl.takef(newGenerator);
    }

    public static <E> HasVal<E> termToHasVal(MSeries<E> term) {
        return MSeries.termToHasVal(term);
    }

    public X value() {
        return impl.value();
    }

    public <Y, O> MSeries<O> scan(Supplier<Y> newBFunc, Function1<Y, CellAdder<X>> adder, AggOut<Y, O> yOut, Class<Y> yType) {
        return new MSeries<>(impl.scan(newBFunc::get, adder, yOut, toClassTag(yType)));
    }

    public MSeries<X> filter(Function<X, Object> accept) {
        return impl.filter(accept);
    }

    private static <T> ClassTag<T> toClassTag(Class<T> clazz) {
        ClassTag<T> apply = (ClassTag<T>)  ClassTag$.MODULE$.apply(clazz);
        return apply;
    }
}
