package crate.elasticsearch.rest.action.admin.searchinto;

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

import crate.elasticsearch.action.searchinto.SearchIntoAction;
import crate.elasticsearch.action.searchinto.SearchIntoRequest;
import crate.elasticsearch.action.searchinto.SearchIntoResponse;
import crate.elasticsearch.client.action.searchinto.SearchIntoRequestBuilder;
import crate.elasticsearch.rest.action.support.RestXContentBuilder;

/**
 *
 */
public class RestSearchIntoAction extends BaseRestHandler {

    @Inject
    public RestSearchIntoAction(Settings settings, Client client,
            RestController controller) {
        super(settings, client);
        registerHandlers(controller);
    }

    protected void registerHandlers(RestController controller) {
        controller.registerHandler(POST, "/_search_into", this);
        controller.registerHandler(POST, "/{index}/_search_into", this);
        controller.registerHandler(POST, "/{index}/{type}/_search_into", this);
    }

    protected Action<SearchIntoRequest, SearchIntoResponse, SearchIntoRequestBuilder, Client> action() {
        return SearchIntoAction.INSTANCE;
    }

    public void handleRequest(final RestRequest request,
            final RestChannel channel, final Client client) {
        SearchIntoRequest searchIntoRequest = new SearchIntoRequest(
                Strings.splitStringByCommaToArray(request.param("index")));

        if (request.hasParam("ignore_unavailable") ||
                request.hasParam("allow_no_indices") ||
                request.hasParam("expand_wildcards")) {
            IndicesOptions iopt = IndicesOptions.fromRequest(request, IndicesOptions.lenient());
            searchIntoRequest.indicesOptions(iopt);
        }
        else if (request.hasParam("ignore_indices")) {
            if (request.param("ignore_indices").equalsIgnoreCase("missing")) {
                searchIntoRequest.indicesOptions(IndicesOptions.lenient());
            }
            else {
                searchIntoRequest.indicesOptions(IndicesOptions.strict());
            }
        }
        searchIntoRequest.listenerThreaded(false);
        try {
            if (request.hasContent()) {
                searchIntoRequest.source(request.content(),
                        request.contentUnsafe());
            } else {
                String source = request.param("source");
                if (source != null) {
                    searchIntoRequest.source(source);
                } else {
                    BytesReference querySource = RestActions.parseQuerySource(request).buildAsBytes(XContentType.JSON);
                    if (querySource != null) {
                        searchIntoRequest.source(querySource, false);
                    }
                }
            }
            searchIntoRequest.routing(request.param("routing"));
            searchIntoRequest.types(Strings.splitStringByCommaToArray(request.param("type")));
            searchIntoRequest.preference(request.param("preference",
                    "_primary"));
        } catch (Exception e) {
            try {
                XContentBuilder builder = RestXContentBuilder
                        .restContentBuilder(
                                request);
                channel.sendResponse(new BytesRestResponse(
                        BAD_REQUEST, builder.startObject().field("error",
                        e.getMessage()).endObject()));
            } catch (IOException e1) {
                logger.error("Failed to send failure response", e1);
            }
            return;
        }

        client.execute(action(), searchIntoRequest,
                new ActionListener<SearchIntoResponse>() {

                    public void onResponse(SearchIntoResponse response) {
                        try {
                            XContentBuilder builder = RestXContentBuilder
                                    .restContentBuilder(
                                            request);
                            response.toXContent(builder, request);
                            channel.sendResponse(new BytesRestResponse(
                                    OK, builder));
                        } catch (Exception e) {
                            onFailure(e);
                        }
                    }

                    public void onFailure(Throwable e) {
                
                            channel.sendResponse(
                                    new BytesRestResponse(BAD_REQUEST));
                      
                    }
                });

    }
}
