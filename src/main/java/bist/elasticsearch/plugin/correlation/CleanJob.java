package bist.elasticsearch.plugin.correlation;

import crate.elasticsearch.action.dump.DumpAction;
import crate.elasticsearch.action.export.ExportRequest;
import crate.elasticsearch.action.export.ExportResponse;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.admin.indices.close.CloseIndexRequest;
import org.elasticsearch.action.admin.indices.close.CloseIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.ClusterAdminClient;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.hppc.cursors.ObjectObjectCursor;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.river.RiverName;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.TimeZone;

/**
 * Created with IntelliJ IDEA.
 * User: zeldal.ozdemir
 * Date: 31.01.2014
 * Time: 17:55
 * To change this template use File | Settings | File Templates.
 */
public class CleanJob implements Job {
    private ESLogger logger;
    private Client client;
    private int archiveTime = 1;  // Default 1 Day
    private String archiveDirectory = "../../../archive/${index}/"; // default
    private int closeTime = 14;  // Default 2 Weeks
    private int deleteTime = 31;  // Default 1 Month
    private RiverName riverName;

/*        public CleanJob() {
        logger.info("Hey it's instantiated....:"+riverName.getType()+"-"+riverName.getName());

    }*/

    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {

        riverName = (RiverName) context.getMergedJobDataMap().get("riverName");
        logger = (ESLogger) context.getMergedJobDataMap().get("logger");
        client = (Client) context.getMergedJobDataMap().get("client");
        logger.info("Hey it's executed....:" + riverName.getType() + "-" + riverName.getName());
        try {
            getSettings();


            AdminClient admin = client.admin();
            ClusterAdminClient cluster = admin.cluster();
            ClusterStateResponse clusterStateResponse = cluster.state(new ClusterStateRequest()).actionGet();
            ImmutableOpenMap<String, IndexMetaData> indices = clusterStateResponse.getState().getMetaData().getIndices();
            for (ObjectObjectCursor<String, IndexMetaData> indiceEntry : indices) {
                IndexMetaData indexMetaData = indiceEntry.value;
                logger.info(indexMetaData.getIndex() + " - " + indexMetaData.getState());
                inspectIndex(indexMetaData);
            }

        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }


    private void getSettings() {
        GetRequestBuilder getRequestBuilder = client.prepareGet("_river", riverName.getName(), "_meta");
        GetResponse getFields = getRequestBuilder.execute().actionGet();

        Object archiveTimeObj = getFields.getSource().get("archiveTime");
        if (archiveTimeObj != null && archiveTimeObj instanceof Integer) {
            archiveTime = (Integer) archiveTimeObj;
        }
        Object closeTimeObj = getFields.getSource().get("closeTime");
        if (closeTimeObj != null && closeTimeObj instanceof Integer) {
            closeTime = (Integer) closeTimeObj;
        }
        Object deleteTimeObj = getFields.getSource().get("deleteTime");
        if (deleteTimeObj != null && deleteTimeObj instanceof Integer) {
            deleteTime = (Integer) deleteTimeObj;
        }

        Object archiveDirectoryObj = getFields.getSource().get("archiveDirectory");
        if (archiveDirectoryObj != null && archiveDirectoryObj instanceof String) {
            archiveDirectory = (String) archiveDirectoryObj;
        }
    }

    private void inspectIndex(IndexMetaData indexMetaData) {
        Calendar indexDate = parseDate(indexMetaData.getIndex());
        if (indexDate == null) {
            logger.info(indexMetaData.getIndex() + " is not timed index");
            return;
        }


        if (indexMetaData.getState() == IndexMetaData.State.OPEN) {

            Calendar archiveDate = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
            archiveDate.add(Calendar.DAY_OF_MONTH, archiveTime * -1);

/*            if (archiveDate.get(Calendar.YEAR) == indexDate.get(Calendar.YEAR) &&
                    archiveDate.get(Calendar.MONTH) == indexDate.get(Calendar.MONTH) &&
                    archiveDate.get(Calendar.DAY_OF_MONTH) == indexDate.get(Calendar.DAY_OF_MONTH)) {  // equalDate, find better solution*/
            if (archiveDate.after(indexDate) && !isArchieved(indexMetaData)) {
                archiveIndex(indexMetaData);
//                return;
            }

            Calendar closeDate = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
            closeDate.add(Calendar.DAY_OF_MONTH, closeTime * -1);

            if (closeDate.after(indexDate))
                closeIndex(indexMetaData);


        }

        Calendar expireDate = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        expireDate.add(Calendar.DAY_OF_MONTH, deleteTime * -1);

        if (expireDate.after(indexDate)) {
            deleteIndex(indexMetaData);
            return;
        }

    }

    private boolean isArchieved(IndexMetaData indexMetaData) {
        GetResponse getResponse = client.prepareGet("_river", riverName.getName(), indexMetaData.getIndex()).get();

        if(getResponse != null && getResponse.isExists()){
            Object isArchievedObj = getResponse.getSource().get("archieved");
            if(isArchievedObj != null && isArchievedObj instanceof Boolean)
                return ((Boolean) isArchievedObj).booleanValue();
        }
        return false;
    }

    private void archiveIndex(IndexMetaData indexStat) {
        logger.info("Index is Achieving");
        ExportRequest request = new ExportRequest(indexStat.getIndex());

        request.source();
        try {
            XContentBuilder builder = XContentFactory.jsonBuilder().startObject();
//            builder.field("directory", "D:\\Projects\\works\\elasticsearch\\inout-plugin-test\\output\\${index}\\");
            builder.field("directory", archiveDirectory);
            builder.field("force_overwrite", true);
            builder.endObject();
            request.source(builder.string());
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
            return;
        }

        ExportResponse exportResponse = client.execute(DumpAction.INSTANCE, request).actionGet();


        logger.info("Index is Achieved, Total:" + exportResponse.getTotalExported());

        IndexRequestBuilder updateRequestBuilder = client.prepareIndex("_river", riverName.getName(), indexStat.getIndex());
        updateRequestBuilder.setSource("archieved", true);
        IndexResponse updateResponse = updateRequestBuilder.execute().actionGet();
        if(updateResponse == null )
            logger.warn("Index is Achieved, But Failed to update River:");

    }

    private void closeIndex(IndexMetaData indexStat) {
        ActionFuture<CloseIndexResponse> close = client.admin().indices().close(new CloseIndexRequest(indexStat.getIndex()));
        CloseIndexResponse closeIndexResponse = close.actionGet();
        if (!closeIndexResponse.isAcknowledged())
            logger.warn("Failed to Close Index:" + indexStat.getIndex());
        else
            logger.info("Index is Closed:" + indexStat.getIndex());
    }


    private void deleteIndex(IndexMetaData indexStat) {
        ActionFuture<DeleteIndexResponse> delete = client.admin().indices().delete(new DeleteIndexRequest(indexStat.getIndex()));
        DeleteIndexResponse deleteIndexResponse = delete.actionGet();
        if (!deleteIndexResponse.isAcknowledged())
            logger.warn("Failed to Delete Index:" + indexStat.getIndex());
        else
            logger.info("Index is Deleted:" + indexStat.getIndex());
    }

    private Calendar parseDate(String indexName) {
        String[] split = indexName.split("-");     // assuming last part is date
        if (split.length < 2)
            return null;
        String lastPart = split[split.length - 1];
        try {

            Calendar instance = Calendar.getInstance();
            instance.setTime(new SimpleDateFormat("yyyy.MM.dd").parse(lastPart));
            return instance;
        } catch (ParseException e) {
            return null;
        }
    }
}
