package crate.elasticsearch.plugin.inout;

import java.util.Collection;

import bist.elasticsearch.plugin.river.management.InOutRiverModule;

import org.elasticsearch.common.collect.ImmutableList;
import org.elasticsearch.common.collect.Lists;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.AbstractPlugin;
import org.elasticsearch.rest.RestModule;
import org.elasticsearch.action.ActionModule;

import crate.elasticsearch.action.dump.DumpAction;
import crate.elasticsearch.action.dump.TransportDumpAction;
import crate.elasticsearch.action.export.ExportAction;
import crate.elasticsearch.action.export.TransportExportAction;
import crate.elasticsearch.action.import_.ImportAction;
import crate.elasticsearch.action.import_.TransportImportAction;
import crate.elasticsearch.action.reindex.ReindexAction;
import crate.elasticsearch.action.reindex.TransportReindexAction;
import crate.elasticsearch.action.restore.RestoreAction;
import crate.elasticsearch.action.restore.TransportRestoreAction;
import crate.elasticsearch.action.searchinto.SearchIntoAction;
import crate.elasticsearch.action.searchinto.TransportSearchIntoAction;
import crate.elasticsearch.module.searchinto.SearchIntoModule;
import crate.elasticsearch.rest.action.admin.dump.RestDumpAction;
import crate.elasticsearch.rest.action.admin.export.RestExportAction;
import crate.elasticsearch.rest.action.admin.import_.RestImportAction;
import crate.elasticsearch.rest.action.admin.reindex.RestReindexAction;
import crate.elasticsearch.rest.action.admin.restore.RestRestoreAction;
import crate.elasticsearch.rest.action.admin.searchinto.RestSearchIntoAction;

import org.elasticsearch.river.RiversModule;

public class InOutPlugin extends AbstractPlugin {

    private final Settings settings;

    public InOutPlugin(Settings settings) {
        this.settings = settings;
    }

    public String name() {
        return "inout";
    }

    public String description() {
        return "InOut plugin";
    }
    
    public void onModule(ActionModule actionModule){
    	if (!settings.getAsBoolean("node.client", false)) {
    		actionModule.registerAction(ExportAction.INSTANCE,  TransportExportAction.class);
    		actionModule.registerAction(ImportAction.INSTANCE, TransportImportAction.class);
    		actionModule.registerAction(SearchIntoAction.INSTANCE, TransportSearchIntoAction.class);
    		actionModule.registerAction(DumpAction.INSTANCE, TransportDumpAction.class);
    		actionModule.registerAction(RestoreAction.INSTANCE, TransportRestoreAction.class);
    		actionModule.registerAction(ReindexAction.INSTANCE, TransportReindexAction.class);
    	}
    }

    public void onModule(RestModule restModule) {
        restModule.addRestAction(RestExportAction.class);
        restModule.addRestAction(RestImportAction.class);
        restModule.addRestAction(RestSearchIntoAction.class);
        restModule.addRestAction(RestDumpAction.class);
        restModule.addRestAction(RestRestoreAction.class);
        restModule.addRestAction(RestReindexAction.class);
    }

    public void onModule(RiversModule module) {
        module.registerRiver("housecleaning", InOutRiverModule.class);
    }

    @Override
    public Collection<Module> modules(Settings settings) {
        Collection<Module> modules = Lists.newArrayList();
        if (!settings.getAsBoolean("node.client", false)) {
            modules.add(new SearchIntoModule());
        }
        return modules;
    }
}
