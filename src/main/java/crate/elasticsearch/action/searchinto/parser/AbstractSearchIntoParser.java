package crate.elasticsearch.action.searchinto.parser;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.SearchParseElement;
import org.elasticsearch.search.SearchParseException;

import crate.elasticsearch.action.searchinto.SearchIntoContext;
import crate.elasticsearch.script.ScriptParseElement;
import crate.elasticsearch.script.ScriptParser;

public abstract class AbstractSearchIntoParser  implements ISearchIntoParser {



    /**
     * Main method of this class to parse given payload of _search_into action
     *
     * @param context
     * @param source
     * @throws org.elasticsearch.search.SearchParseException
     *
     */
	private final ImmutableMap<String, ScriptParseElement> scriptElementParsers;
	
	@Inject
    public AbstractSearchIntoParser(ScriptParser scriptParser) {
		this.scriptElementParsers = ImmutableMap.copyOf(scriptParser.scriptElementParsers());
	}

	public void parseSource(SearchIntoContext context,
            BytesReference source) throws SearchParseException {
		
        XContentParser parser = null;
        try {
        	if (source != null && source.length() > 0) {

		
                parser = XContentFactory.xContent(source).createParser(source);
                XContentParser.Token token;
                while ((token = parser.nextToken()) != XContentParser.Token
                        .END_OBJECT) {
                    if (token == XContentParser.Token.FIELD_NAME) {
                        String fieldName = parser.currentName();
                        parser.nextToken();
                        SearchParseElement element = getElementParsers().get(
                                fieldName);
                        if (element == null) {
                        	ScriptParseElement scriptElement = scriptElementParsers.get(fieldName);
                        	if(scriptElement==null){
                                   throw new SearchParseException(context, "No parser for element [" + fieldName + "]");
                        	} else {
                          		scriptElement.parse(parser, context);
                        	}
                        } else {	
                            element.parse(parser, context);
                        }
                    } else if (token == null) {
                        break;
                    }
                }
            }
            validate(context);
        } catch (Exception e) {
            String sSource = "_na_";
            try {
                 sSource = XContentHelper.convertToJson(source, false);
            } catch (Throwable e1) {
                // ignore
            }
            throw new SearchParseException(context,
                    "Failed to parse source [" + sSource + "]", e);
        } finally {
            if (parser != null) {
                parser.close();
            }
        }
    }

    /**
     * Get the element parser map
     * @return
     */
    protected abstract ImmutableMap<String, SearchParseElement> getElementParsers();

    /**
     * Validate the pay load of the search-into context.
     * @param context
     */
    protected void validate(SearchIntoContext context) {
        if (context.hasFieldNames() && context.fieldNames().contains("_source")) {
            String index = context.mapperService().index().getName();
            for (String type : context.mapperService().types()) {
                if (!context.mapperService().documentMapper(type).sourceMapper().enabled()) {
                    throw new SearchParseException(context,
                            "The _source field of index " + index + " and type " + type + " is not stored.");
                }
            }
        }
    }
}
