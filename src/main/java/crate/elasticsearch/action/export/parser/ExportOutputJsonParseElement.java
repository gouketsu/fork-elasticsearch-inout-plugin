package crate.elasticsearch.action.export.parser;

import crate.elasticsearch.action.export.ExportContext;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.SearchParseElement;
import org.elasticsearch.search.internal.SearchContext;

import java.util.ArrayList;
import java.util.List;

/**
 * Parser for token ``output_json``. The value of the token is a boolean
 * <p/>
 * <pre>
 * "outpout_json": true
 *
 * </pre>
 */
public class ExportOutputJsonParseElement implements SearchParseElement {

    @Override
    public void parse(XContentParser parser, SearchContext context) throws Exception {
        XContentParser.Token token = parser.currentToken();
        if (token.isValue()) {
            ((ExportContext) context).outputJson(parser.booleanValue());
        } 
    }
}
