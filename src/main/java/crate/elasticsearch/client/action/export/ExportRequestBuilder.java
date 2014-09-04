package crate.elasticsearch.client.action.export;

import crate.elasticsearch.action.export.ExportAction;
import crate.elasticsearch.action.export.ExportRequest;
import crate.elasticsearch.action.export.ExportResponse;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.client.Client;


public class ExportRequestBuilder extends ActionRequestBuilder<ExportRequest, ExportResponse, ExportRequestBuilder, Client> {

    public ExportRequestBuilder(Client client) {
        super(client, new ExportRequest());
    }

    @Override
    protected void doExecute(ActionListener<ExportResponse> listener) {
        ((Client)client).execute(ExportAction.INSTANCE, request, listener);
    }

    public ExportRequestBuilder setIndices(String ... indices) {
        request.indices(indices);
        return this;
    }
}