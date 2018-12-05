package scespet.core.japi;

import gsa.esg.mekon.core.Environment;
import gsa.esg.mekon.core.EventGraphObject;
import scala.Function0;
import scala.Function1;
import scala.Tuple2;
import scala.collection.TraversableOnce;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;
import scespet.core.Agg;
import scespet.core.AggOut;
import scespet.core.CellAdder;
import scespet.core.GroupedTerm;
import scespet.core.HasVal;
import scespet.core.HasValue;
import scespet.core.MacroTerm;
import scespet.core.SliceTriggerSpec;
import scespet.core.Term;
import scespet.core.VectTerm;
import scespet.util.SliceAlign;

import java.util.function.Function;
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
    public <Y extends CellAdder<X>> MSeries<Y> reduce(Supplier<Y> newBFunc) {
        Y instance = newBFunc.get();
        Class<Y> aClass = (Class<Y>) instance.getClass();
        return reduce(newBFunc, y -> y, y -> y, aClass);
    }

    public <Y, O> MSeries<O> reduce(Supplier<Y> newBFunc, Function<Y, CellAdder<X>> adder, AggOut<Y, O> yOut, Class<Y> yType) {
        Function0<Y> generator = JavaSupplierSupport.asScala(newBFunc);
        Function1<Y, CellAdder<X>> cellAdder = JavaSupplierSupport.asScala(adder);
        ClassTag<Y> classTag = toClassTag(yType);
        Term<O> reduce = impl.<Y, O>reduce(generator, cellAdder, yOut, classTag);
        return new MSeries<O>((MacroTerm<O>) reduce);
    }

    public <Y, O> MSeries<O> scan(Supplier<Y> newBFunc, Function<Y, CellAdder<X>> adder, AggOut<Y, O> yOut, Class<Y> yType) {
        Function0<Y> generator = JavaSupplierSupport.asScala(newBFunc);
        Function1<Y, CellAdder<X>> cellAdder = JavaSupplierSupport.asScala(adder);
        return new MSeries<>(impl.scan(generator, cellAdder, yOut, toClassTag(yType)));
    }


    public HasVal<X> input() {
        return impl.input();
    }

    public <Y> MSeries<Y> flatMap(Function<X, TraversableOnce<Y>> expand) {
        return new MSeries<>(impl.flatMap(JavaSupplierSupport.asScala(expand)));
    }

    public <Y extends Agg> MSeries<Object> reduce_all(Y y) {
        return new MSeries<>(impl.reduce_all(y));
    }

    public <T> MSeries<T> filterType(Class<T> clazz) {
        return new MSeries<>(impl.filterType(toClassTag(clazz)));
    }

    public <Y> MSeries<Tuple2<X, Y>> take(MSeries<Y> y) {
        MacroTerm<Tuple2<X, Y>> take = (MacroTerm<Tuple2<X, Y>>) impl.take(y.impl);
        return new MSeries<>(take);
    }

    public MSeries<X> sample(EventGraphObject evt) {
        return new MSeries<>(impl.sample(evt));
    }

    public GroupedTerm<X> window(HasValue<Object> window) {
        return impl.window(window);
    }

    public boolean initialised() {
        return impl.initialised();
    }

    public <Y> VectTerm<Y, Y> valueSet(Function<X, TraversableOnce<Y>> expand) {
        return impl.valueSet(JavaSupplierSupport.asScala(expand));
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
        return new MSeries<>(impl.fold_all(y));
    }

    public <Y> MSeries<Tuple2<X, Y>> join(MSeries<Y> y) {
        return new MSeries<>(impl.join(y.impl));
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

    public <Y> MSeries<Y> map(Function<X, Y> f, boolean exposeNull) {
        return new MSeries<>(impl.map(JavaSupplierSupport.asScala(f), exposeNull));
    }

    public EventGraphObject getTrigger() {
        return impl.getTrigger();
    }

    public <Y> VectTerm<X, Y> takef(Function<X, HasVal<Y>> newGenerator) {
        return impl.takef(JavaSupplierSupport.asScala(newGenerator));
    }

    public static <E> HasVal<E> termToHasVal(MSeries<E> term) {
        return MSeries.termToHasVal(term);
    }

    public X value() {
        return impl.value();
    }

    public MSeries<X> filter(Function<X, Object> accept) {
        return new MSeries<>(impl.filter(JavaSupplierSupport.asScala(accept)));
    }

    private static <T> ClassTag<T> toClassTag(Class<T> clazz) {
        ClassTag<T> apply = (ClassTag<T>)  ClassTag$.MODULE$.apply(clazz);
        return apply;
    }
}
