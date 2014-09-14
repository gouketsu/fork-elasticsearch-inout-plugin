package crate.elasticsearch.rest.action.support;

import java.io.IOException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilder.FieldCaseConversion;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.RestRequest;

public class RestXContentBuilder
{
  public static XContentBuilder restContentBuilder(RestRequest request)
    throws IOException
  {
    return restContentBuilder(request, request.hasContent() ? request.content() : null);
  }

  public static XContentBuilder restContentBuilder(RestRequest request, @Nullable BytesReference autoDetectSource) throws IOException {
    XContentType contentType = XContentType.fromRestContentType(request.param("format", request.header("Content-Type")));
    if (contentType == null)
    {
      if (autoDetectSource != null) {
        contentType = XContentFactory.xContentType(autoDetectSource);
      }
    }
    if (contentType == null)
    {
      contentType = XContentType.JSON;
    }
    XContentBuilder builder = new XContentBuilder(XContentFactory.xContent(contentType), new BytesStreamOutput());
    if (request.paramAsBoolean("pretty", false)) {
      builder.prettyPrint().lfAtEnd();
    }

    builder.humanReadable(request.paramAsBoolean("human", builder.humanReadable()));

    String casing = request.param("case");
    if ((casing != null) && ("camelCase".equals(casing))) {
      builder.fieldCaseConversion(XContentBuilder.FieldCaseConversion.CAMELCASE);
    }
    else
    {
      builder.fieldCaseConversion(XContentBuilder.FieldCaseConversion.NONE);
    }
    return builder;
  }

  public static XContentBuilder emptyBuilder(RestRequest request) throws IOException {
    return restContentBuilder(request, request.hasContent() ? request.content() : null).startObject().endObject();
  }

  public static void directSource(BytesReference source, XContentBuilder rawBuilder, ToXContent.Params params)
    throws IOException
  {
    XContentHelper.writeDirect(source, rawBuilder, params);
  }

  public static void restDocumentSource(BytesReference source, XContentBuilder builder, ToXContent.Params params) throws IOException {
    XContentHelper.writeRawField("_source", source, builder, params);
  }
}
