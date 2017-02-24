package scespet.core;

import kotlin.jvm.functions.Function1;
import org.jetbrains.annotations.NotNull;

/**
 * @author danvan
 * @version $Id$
 */
public class KotlinImplJ<X> extends Series<X>{
    @NotNull
    public Series<X> filter(@NotNull Function1<? super X, Boolean> accept) {
        // todo: implement
        throw new UnsupportedOperationException("Please implement me");
    }

    public X value() {
        // todo: implement
        throw new UnsupportedOperationException("Please implement me");
    }

    @NotNull
    public <Y> Y map(@NotNull Function1<? super X, ? extends Y> f) {
        // todo: implement
        throw new UnsupportedOperationException("Please implement me");
    }
}
