package crate.elasticsearch.rest.action.admin.import_;

import crate.elasticsearch.action.import_.ImportAction;
import crate.elasticsearch.action.import_.ImportRequest;
import crate.elasticsearch.action.import_.ImportResponse;
import crate.elasticsearch.rest.action.support.RestXContentBuilder;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.*;
import org.elasticsearch.rest.action.support.RestActions;

import java.io.IOException;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestStatus.BAD_REQUEST;
import static org.elasticsearch.rest.RestStatus.OK;

public class RestImportAction extends BaseRestHandler {

    @Inject
    public RestImportAction(Settings settings, Client client, RestController controller) {
        super(settings, controller, client);
        registerHandlers(controller);
    }

    protected void registerHandlers(RestController controller) {
        controller.registerHandler(POST, "/_import", this);
        controller.registerHandler(POST, "/{index}/_import", this);
        controller.registerHandler(POST, "/{index}/{type}/_import", this);
    }


    protected Action action() {
        return ImportAction.INSTANCE;
    }

    public void handleRequest(final RestRequest request, final RestChannel channel, Client client) {
        ImportRequest importRequest = new ImportRequest();
        importRequest.listenerThreaded(false);
        try {
            if (request.hasContent()) {
                importRequest.source(request.content());
            } else {
                String source = request.param("source");
                if (source != null) {
                    importRequest.source(source);
                } else {
                    BytesReference querySource = RestActions.parseQuerySource(request).buildAsBytes(XContentType.JSON);
                    if (querySource != null) {
                        importRequest.source(querySource);
                    }
                }
            }
            importRequest.index(request.param("index"));
            importRequest.type(request.param("type"));
        } catch (Exception e) {
            try {
                XContentBuilder builder = RestXContentBuilder.restContentBuilder(request);
                channel.sendResponse(new BytesRestResponse(BAD_REQUEST, builder.startObject().field("error", e.getMessage()).endObject()));
            } catch (IOException e1) {
                logger.error("Failed to send failure response", e1);
            }
            return;
        }


        client.execute(action(), importRequest, new ActionListener<ImportResponse>() {

            public void onResponse(ImportResponse response) {
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
