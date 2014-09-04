package crate.elasticsearch.action.reindex;

import org.elasticsearch.action.ClientAction;
import org.elasticsearch.client.Client;

import crate.elasticsearch.action.searchinto.SearchIntoRequest;
import crate.elasticsearch.action.searchinto.SearchIntoResponse;
import crate.elasticsearch.client.action.searchinto.SearchIntoRequestBuilder;

public class ReindexAction extends ClientAction<SearchIntoRequest, SearchIntoResponse, SearchIntoRequestBuilder>{

    public static final ClientAction<SearchIntoRequest, SearchIntoResponse, SearchIntoRequestBuilder> INSTANCE = new ReindexAction();
    public static final String NAME = "el-crate-reindex";

    protected ReindexAction() {
        super(NAME);
    }

    @Override
    public SearchIntoRequestBuilder newRequestBuilder(Client client) {
        return new SearchIntoRequestBuilder(client);
    }

    @Override
    public SearchIntoResponse newResponse() {
        return new SearchIntoResponse();
    }

}
