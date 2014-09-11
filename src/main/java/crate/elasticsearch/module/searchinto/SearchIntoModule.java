package crate.elasticsearch.module.searchinto;

import java.util.List;

import crate.elasticsearch.action.searchinto.SearchIntoAction;
import crate.elasticsearch.action.searchinto.TransportSearchIntoAction;
import crate.elasticsearch.action.searchinto.parser.SearchIntoParser;
import crate.elasticsearch.searchinto.BulkWriterCollector;
import crate.elasticsearch.searchinto.WriterCollectorFactory;

import org.elasticsearch.action.GenericAction;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.common.collect.ImmutableList;
import org.elasticsearch.common.collect.Lists;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.inject.SpawnModules;
import org.elasticsearch.common.inject.assistedinject.FactoryProvider;
import org.elasticsearch.common.inject.multibindings.MapBinder;

public class SearchIntoModule extends AbstractModule {
	

    @Override
    protected void configure() {
    
        MapBinder<String, WriterCollectorFactory> collectorBinder
                = MapBinder.newMapBinder(binder(),
                String.class, WriterCollectorFactory.class);

        collectorBinder.addBinding(BulkWriterCollector.NAME).toProvider(
                FactoryProvider
                        .newFactory(WriterCollectorFactory.class,
                                BulkWriterCollector.class));


    }
}