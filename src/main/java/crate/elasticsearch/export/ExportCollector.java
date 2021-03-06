package crate.elasticsearch.export;

import static org.elasticsearch.common.collect.Lists.newArrayList;

import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.Scorer;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.text.StringAndBytesText;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.fieldvisitor.*;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.FieldMappers;
import org.elasticsearch.index.mapper.internal.SourceFieldMapper;
import org.elasticsearch.search.SearchHitField;
import org.elasticsearch.search.fetch.FetchSubPhase;
import org.elasticsearch.search.fetch.source.FetchSourceContext;
import org.elasticsearch.search.internal.InternalSearchHit;
import org.elasticsearch.search.internal.InternalSearchHitField;

import crate.elasticsearch.action.export.ExportContext;


public class ExportCollector extends Collector {

    private IndexReader currentReader;
    private long numExported = 0;
    private final FieldsVisitor fieldsVisitor;
    private final ExportContext context;

    private List<String> extractFieldNames;
    boolean sourceRequested;
    private final ExportFields exportFields;
    private final OutputStream out;
    private AtomicReaderContext arc;
    private final FetchSubPhase[] fetchSubPhases;


    public ExportCollector(ExportContext context,
                           OutputStream os, FetchSubPhase[] fetchSubPhases) {
        this.out = os;
        this.context = context;
        this.fetchSubPhases = fetchSubPhases;
        this.exportFields = new ExportFields(context.fieldNames());
        sourceRequested = false;

        if (!context.hasFieldNames()) {
            if (context.hasPartialFields()) {
                // partial fields need the source, so fetch it
                fieldsVisitor = new UidAndSourceFieldsVisitor();
            } else {
                // no fields specified, default to return source if no explicit indication
                if (!context.hasScriptFields() && !context.hasFetchSourceContext()) {
                    context.fetchSourceContext(new FetchSourceContext(true));
                }
                fieldsVisitor = context.sourceRequested() ? new UidAndSourceFieldsVisitor() : new JustUidFieldsVisitor();
            }
        } else if (context.fieldNames().isEmpty()) {
            if (context.sourceRequested()) {
                fieldsVisitor = new UidAndSourceFieldsVisitor();
            } else {
                fieldsVisitor = new JustUidFieldsVisitor();
            }
        } else {
            boolean loadAllStored = false;
            Set<String> fieldNames = null;
            for (String fieldName : context.fieldNames()) {
                if (fieldName.equals("*")) {
                    loadAllStored = true;
                    continue;
                }
                if (fieldName.equals(SourceFieldMapper.NAME)) {
                    if (context.hasFetchSourceContext()) {
                        context.fetchSourceContext().fetchSource(true);
                    } else {
                        context.fetchSourceContext(new FetchSourceContext(true));
                    }
                    continue;
                }
                FieldMappers x = context.smartNameFieldMappers
                        (fieldName);
                if (x != null && x.mapper().fieldType().stored()) {
                    if (fieldNames == null) {
                        fieldNames = new HashSet<String>();
                    }
                    fieldNames.add(x.mapper().names().indexName());
                } else {
                    if (extractFieldNames == null) {
                        extractFieldNames = newArrayList();
                    }
                    extractFieldNames.add(fieldName);
                }
            }
            if (loadAllStored) {
                fieldsVisitor = new AllFieldsVisitor(); // load everything, including _source
            } else if (fieldNames != null) {
                boolean loadSource = extractFieldNames != null || context.sourceRequested();
                fieldsVisitor = new CustomFieldsVisitor(fieldNames, loadSource);
            } else if (extractFieldNames != null || context.sourceRequested()) {
                fieldsVisitor = new UidAndSourceFieldsVisitor();
            } else {
                fieldsVisitor = new JustUidFieldsVisitor();
            }
        }
    }

    @Override
    public void setNextReader(AtomicReaderContext context) throws IOException {
        this.arc = context;
        this.currentReader = context.reader();
    }

    @Override
    public boolean acceptsDocsOutOfOrder() {
        return true;
    }

    @Override
    public void setScorer(Scorer scorer) throws IOException {
    }

    public long numExported(){
        return numExported;
    }

    @Override
    public void collect(int doc) throws IOException {
        fieldsVisitor.reset();
        currentReader.document(doc, fieldsVisitor);

        Map<String, SearchHitField> searchFields = null;
        if (fieldsVisitor.fields() != null) {
            searchFields = new HashMap<String, SearchHitField>(fieldsVisitor.fields().size());
            for (Map.Entry<String, List<Object>> entry : fieldsVisitor.fields().entrySet()) {
                searchFields.put(entry.getKey(), new InternalSearchHitField(entry.getKey(), entry.getValue()));
            }
        }

        DocumentMapper documentMapper = context.mapperService()
                .documentMapper(fieldsVisitor.uid().type());
        Text typeText;
        if (documentMapper == null) {
            typeText = new StringAndBytesText(fieldsVisitor.uid().type());
        } else {
            typeText = documentMapper.typeText();
        }

        InternalSearchHit searchHit = new InternalSearchHit(doc,
                fieldsVisitor.uid().id(), typeText,
                searchFields).sourceRef(fieldsVisitor.source());


        for (FetchSubPhase fetchSubPhase : fetchSubPhases) {
            FetchSubPhase.HitContext hitContext = new FetchSubPhase.HitContext();
            if (fetchSubPhase.hitExecutionNeeded(context)) {
                hitContext.reset(searchHit, arc, doc, context.searcher().getIndexReader(), doc, fieldsVisitor);
                fetchSubPhase.hitExecute(context, hitContext);
            }
        }

	searchHit.shardTarget(context.shardTarget());
	exportFields.hit(searchHit);
	XContentBuilder builder = new XContentBuilder(XContentFactory.xContent(XContentType.JSON), out);
	if (context.outputJson() == true) {
		builder.prettyPrint();
	    if (numExported > 0) {
		   out.write(',');
		   out.write('\n');
	    }
	}
	exportFields.toXContent(builder, ToXContent.EMPTY_PARAMS);
		builder.flush();
		if (context.outputJson() == false)
		{
			out.write('\n');
		}
		out.flush();
		numExported++;
    }
    public void begin() throws IOException {
    	if (context.outputJson() == true) {
    		out.write('[');
    	}
    }
    public void terminate() throws IOException {
    	if (context.outputJson() == true) {
    		out.write(']');
    	}
    }
}
