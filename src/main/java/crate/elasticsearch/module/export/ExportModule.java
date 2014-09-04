package crate.elasticsearch.module.export;

import crate.elasticsearch.action.export.ExportAction;
import crate.elasticsearch.action.export.TransportExportAction;
import crate.elasticsearch.action.export.parser.ExportParser;
import crate.elasticsearch.export.Exporter;
import org.elasticsearch.action.GenericAction;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.multibindings.MapBinder;

public class ExportModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(TransportExportAction.class);

        bind(ExportParser.class);
        bind(Exporter.class);

        MapBinder<GenericAction, TransportAction> transportActionsBinder = MapBinder.newMapBinder(binder(), GenericAction.class, TransportAction.class);

        transportActionsBinder.addBinding(ExportAction.INSTANCE).to(TransportExportAction.class);

        MapBinder<String, GenericAction> actionsBinder = MapBinder.newMapBinder(binder(), String.class, GenericAction.class);
        actionsBinder.addBinding(ExportAction.NAME).toInstance(ExportAction.INSTANCE);
    }
}