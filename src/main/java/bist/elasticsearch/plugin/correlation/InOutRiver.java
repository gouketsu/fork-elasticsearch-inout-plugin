/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package bist.elasticsearch.plugin.correlation;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.river.AbstractRiverComponent;
import org.elasticsearch.river.River;
import org.elasticsearch.river.RiverName;
import org.elasticsearch.river.RiverSettings;
import org.elasticsearch.script.ScriptService;
import org.quartz.*;
import org.quartz.Calendar;
import org.quartz.impl.StdSchedulerFactory;

import java.util.*;

import static org.quartz.CronScheduleBuilder.cronSchedule;
import static org.quartz.JobBuilder.newJob;
import static org.quartz.TriggerBuilder.newTrigger;

/**
 *
 */
public class InOutRiver extends AbstractRiverComponent implements River {

    private final Client client;



    private volatile boolean closed = false;

    private Scheduler sched;
    private String cronTrigger = "0 0 1 1/1 * ? *"; // default everynight at 1


    @SuppressWarnings({"unchecked"})
    @Inject
    public InOutRiver(RiverName riverName, RiverSettings settings, Client client, ScriptService scriptService) {
        super(riverName, settings);
        this.client = client;
        if (riverName.getName().equals("default")) {
            logger.info("This is default for all");
        }

        Object cronTriggerObj = settings.settings().get("cronTrigger");
        if(cronTriggerObj != null && cronTriggerObj instanceof String)
            cronTrigger = (String) cronTriggerObj;

        try {

            SchedulerFactory sf = new StdSchedulerFactory();

            sched = sf.getScheduler();
            sched.start();
        } catch (SchedulerException e) {
            logger.error(e.getMessage(), e);
        }


    }

    @Override
    public void start() {
        try {
            JobDetail job = newJob(CleanJob.class)
                    .withIdentity(riverName.getName(), riverName.getType())
                    .build();
            java.util.Calendar calendar = java.util.Calendar.getInstance();
            logger.info("Scheduling Crond with:"+cronTrigger);

            CronTrigger trigger = newTrigger()
                    .withIdentity(riverName.getName(), riverName.getType() + "-trigger")
//                    .withSchedule(cronSchedule(calendar.get(java.util.Calendar.SECOND)+5 +" "+calendar.get(java.util.Calendar.MINUTE)+" * * * ?"))
                    .withSchedule(cronSchedule(cronTrigger))
                    .build();
            job.getJobDataMap().put("riverName",riverName);
            job.getJobDataMap().put("logger",logger);
            job.getJobDataMap().put("client",client);

            sched.scheduleJob(job, trigger);
        } catch (SchedulerException e) {
            logger.error(e.getMessage(), e);
            return;
        }


/*        thread = EsExecutors.daemonThreadFactory(settings.globalSettings(), "management_river").newThread(new Consumer());
        logger.info("starting management river");
        thread.start();*/
    }

    @Override
    public void close() {
        try {
            if (sched == null || sched.isShutdown())
                return;
            logger.info("closing management river");
            sched.shutdown();
        } catch (SchedulerException e) {
            logger.warn(e.getMessage(), e);
        }
    }

}
