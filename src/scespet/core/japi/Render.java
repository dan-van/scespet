package scespet.core.japi;

import datavis.data.Plot;
import datavis.data.Plot$;
import gsa.esg.mekon.core.Environment;
import gsa.esg.mekon.datalogger.DataLogger;
import gsa.esg.mekon.datalogger.DataLoggerFactory;

import javax.annotation.Nullable;

/**
 * @author danvan
 * @version $Id$
 */
public class Render {
    private Environment env;
    private Plot.ChartState chartState = null;

    public Render(Environment env) {
        this.env = env;
    }

    public static Render get(Environment env) {
        return env.getSharedObject(Render.class, env);
    }

    public <K,X extends Number> datavis.data.Plot.Options<K, X> plot(MSeries<X> series, @Nullable String name) {
        datavis.data.Plot.Options<String, ?> options = Plot$.MODULE$.newOptions();
        options.seriesNames(JavaSupplierSupport.asScala((key) -> name));

        int seriesId = options.addSeries(name);
        datavis.data.Plot.TimeSeriesDataset dataset = options.dataset();
        env.addListener(series.getTrigger(), () -> {
            dataset.add(seriesId, env.getEventTime(), series.getValue().doubleValue());
            return false;
        });
        this.chartState = options.chartState();
        return (datavis.data.Plot.Options<K, X>) options;
    }

    public Plot.ChartState getChartState() {
        return chartState;
    }

    public <X> void log(String name, MSeries<X> series, Class<X> type) {
        DataLogger<X> dataLogger = env.getService(DataLoggerFactory.class).getDataLogger(name, type);
        env.addListener(series.getTrigger(), () -> {
            dataLogger.log(series.getValue());
            return false;
        });
    }

}
