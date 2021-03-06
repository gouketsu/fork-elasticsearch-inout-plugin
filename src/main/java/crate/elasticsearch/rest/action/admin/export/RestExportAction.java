package crate.elasticsearch.rest.action.admin.export;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestStatus.BAD_REQUEST;
import static org.elasticsearch.rest.RestStatus.OK;

import java.io.IOException;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.support.RestActions;

import crate.elasticsearch.action.export.ExportAction;
import crate.elasticsearch.action.export.ExportRequest;
import crate.elasticsearch.action.export.ExportResponse;
import crate.elasticsearch.client.action.export.ExportRequestBuilder;
import crate.elasticsearch.rest.action.support.RestXContentBuilder;

/**
 *
 */
public class RestExportAction extends BaseRestHandler {

    @Inject
    public RestExportAction(Settings settings, Client client, RestController controller) {
        super(settings, client);
        registerHandlers(controller);
    }

    protected void registerHandlers(RestController controller) {
        controller.registerHandler(POST, "/_export", this);
        controller.registerHandler(POST, "/{index}/_export", this);
        controller.registerHandler(POST, "/{index}/{type}/_export", this);
    }

    protected Action<ExportRequest, ExportResponse, ExportRequestBuilder, Client> action() {
        return ExportAction.INSTANCE;
    }

    public void handleRequest(final RestRequest request, final RestChannel channel, Client client) {
        ExportRequest exportRequest = new ExportRequest(Strings.splitStringByCommaToArray(request.param("index")));

        if (request.hasParam("ignore_unavailable") ||
            request.hasParam("allow_no_indices") ||
            request.hasParam("expand_wildcards")) {
            IndicesOptions iopt = IndicesOptions.fromRequest(request, IndicesOptions.lenient());
            exportRequest.indicesOptions(iopt);
        }
        else if (request.hasParam("ignore_indices")) {
            if (request.param("ignore_indices").equalsIgnoreCase("missing")) {
                exportRequest.indicesOptions(IndicesOptions.lenient());
            }
            else {
                exportRequest.indicesOptions(IndicesOptions.strict());
            }
        }
        exportRequest.listenerThreaded(false);
        try {
            if (request.hasContent()) {
                exportRequest.source(request.content(), request.contentUnsafe());
            } else {
                String source = request.param("source");
                if (source != null) {
                    exportRequest.source(source);
                } else {
                    BytesReference querySource = RestActions.parseQuerySource(request).buildAsBytes(XContentType.JSON);
                    if (querySource != null) {
                        exportRequest.source(querySource, false);
                    }
                }
            }
            exportRequest.routing(request.param("routing"));
            exportRequest.types(Strings.splitStringByCommaToArray(request.param("type")));
            exportRequest.preference(request.param("preference", "_primary"));
        } catch (Exception e) {
            try {
                XContentBuilder builder = RestXContentBuilder.restContentBuilder(request);
                channel.sendResponse(new BytesRestResponse( BAD_REQUEST, builder.startObject().field("error", e.getMessage()).endObject()));
            } catch (IOException e1) {
                logger.error("Failed to send failure response", e1);
            }
            return;
        }

        client.execute(action(), exportRequest, new ActionListener<ExportResponse>() {

            public void onResponse(ExportResponse response) {
                try {
                    XContentBuilder builder = RestXContentBuilder.restContentBuilder(request);
                    response.toXContent(builder, request);
                    channel.sendResponse(new BytesRestResponse(OK, builder));
                } catch (Exception e) {
                    onFailure(e);
                }
            }

            public void onFailure(Throwable e) {
         
                    channel.sendResponse(new BytesRestResponse(BAD_REQUEST));
               
            }
        });

    }
}
