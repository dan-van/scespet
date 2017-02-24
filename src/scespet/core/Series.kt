package scespet.core

import gsa.esg.mekon.core.EventGraphObject
import scespet.util.SliceAlign

import java.util.function.Function
import kotlin.reflect.KClass

/**
 * @author danvan
 * *
 * @version $Id$
 */
abstract class Series<X> {
    abstract fun value():X

//  fun fold_all<Y <: Agg<X>>(y: Y):Term<Y#OUT>
    abstract fun <Y> map(f: (X) -> Y):Y
    abstract fun filter(accept: (X) -> Boolean):Series<X>

//    abstract fun <Y : Any, O> reduce(newBFunc: () -> Y, adder:(Y) -> CellAdder<X>, yOut :AggOut<Y, O>, yType: KClass<Y>) :Series<O>

//    abstract fun <Y : Any, O>scan(newBFunc: () -> Y, adder:(Y) -> CellAdder<X>, yOut :AggOut<Y, O>, yType:KClass<Y>) :Series<O>

//    fun <S> group(sliceSpec:S, triggerAlign: SliceAlign = SliceAlign.AFTER(), ev:SliceTriggerSpec<S>) :GroupedTerm<X>
//
//    fun window(window:HasValue<Boolean>) : GroupedTerm<X>
//
//    fun by<K>(f: X -> K) :MultiTerm<K,X>
//
//    fun valueSet<Y>(expand: (X->TraversableOnce<Y>)) : VectTerm<Y,Y>
//
//    fun valueSet() : VectTerm<X,X> = valueSet(valueToSingleton<X>)
//
//    /**
//     * emit an updated tuples of (this.value, y) when either series fires
//     * yes, this is like 'zip', but because 'take' is similar I didn't want to use that term
//     */
//    fun join<Y>(y:MacroTerm<Y>):MacroTerm<(X,Y)>
//
//    /**
//     * Sample this series each time {@see y} fires, and emit tuples of (this.value, y)
//     */
//    fun take<Y>(y:MacroTerm<Y>):MacroTerm<(X,Y)>
//
//    /**
//     * Sample this series each time {@see y} fires, and emit tuples of (this.value, y)
//     */
//    fun sample(evt:EventGraphObject):MacroTerm<X>
//
//    fun filterType<T:ClassTag>:Term<T> = {
//        filter( v -> reflect.classTag<T>.unapply(v).isfunined ).map(v -> v.asInstanceOf<T>)
//    }
//
////  fun filterType<T:Integer>():Term<T> = {
////    filter( v -> reflect.classTag<T>.unapply(v).isfunined ).map(v -> v.asInstanceOf<T>)
////  }
//
//    //  private fun valueToSingleton<X,Y> = (x:X) -> Traversable(x.asInstanceOf<Y>)
//    private fun valueToSingleton<Y> = (x:X) -> Traversable(x.asInstanceOf<Y>)
//
}
