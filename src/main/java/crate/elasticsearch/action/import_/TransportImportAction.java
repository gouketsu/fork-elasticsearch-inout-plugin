package crate.elasticsearch.action.import_;

import crate.elasticsearch.action.import_.parser.ImportParser;
import crate.elasticsearch.import_.Importer;
import crate.elasticsearch.script.ScriptProvider;

import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

/**
 *
 */
public class TransportImportAction extends AbstractTransportImportAction {

    @Inject
    public TransportImportAction(Settings settings, ClusterName clusterName,
                                         ThreadPool threadPool, ClusterService clusterService,
                                         TransportService transportService,
                                         ActionFilters actionFilters,
                                         ScriptService scriptService, ScriptProvider scriptProvider, ImportParser importParser, Importer importer, NodeEnvironment nodeEnv) {
        super(settings, "el-crate-import", clusterName, threadPool, clusterService, transportService, actionFilters, scriptService, scriptProvider, importParser, importer, nodeEnv);
    }

    
}
