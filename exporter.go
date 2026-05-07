// -*- coding: utf-8 -*-
//
// © Copyright 2023 GSI Helmholtzzentrum für Schwerionenforschung
//
// This software is distributed under
// the terms of the GNU General Public Licence version 3 (GPL Version 3),
// copied verbatim in the file "LICENCE".

package main

import (
	"strconv"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
)

type exporter struct {
	channelRunningJobs           chan runningJobsResult
	channelUserInfo              chan userInfoMapResult
	channelGroupInfo             chan groupInfoMapResult
	scrapeActive                 bool
	scrapeMutex                  sync.Mutex
	requestTimeout               int
	scrapeOKMetric               prometheus.Gauge
	stageExecutionMetric         *prometheus.GaugeVec
	runningSlurmJobsList         *prometheus.GaugeVec
	userInfoList                 *prometheus.GaugeVec
}

func newGaugeVecMetric(namespace string, metricName string, docString string, constLabels []string) *prometheus.GaugeVec {
	return prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      metricName,
			Help:      docString,
		},
		constLabels,
	)
}

func newExporter(requestTimeout int, urlLustreMetadataOperations string, urlLustreJobReadBytes string, urlLustreJobWriteBytes string) *exporter {

	if requestTimeout <= 0 {
		log.Fatal("Request timeout must be greater then 0")
	}

	scrapeOKMetric := prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: namespaceInternals,
		Name:      "light_scrape_ok",
		Help:      "Indicates if the scrape of the exporter was successful or not.",
	})

	stageExecutionMetric := newGaugeVecMetric(
		namespaceInternals,
		"light_stage_execution_seconds",
		"Execution duration in seconds spend in a specific exporter stage.",
		[]string{"name"})

	runningSlurmJobsList := newGaugeVecMetric(
		namespace,
		"running_slurm_jobs_list",
		"Full list of jobs in the Slurm Queue.",
		[]string{"jobid", "group_name", "user_name"})

	userInfoList := newGaugeVecMetric(
		namespace,
		"user_info_list",
		"Full list of users.",
		[]string{"uid", "user_name", "group_name"})


	return &exporter{
		channelRunningJobs:           make(chan runningJobsResult),
		channelUserInfo:              make(chan userInfoMapResult),
		channelGroupInfo:             make(chan groupInfoMapResult),
		requestTimeout:               requestTimeout,
		scrapeOKMetric:               scrapeOKMetric,
		stageExecutionMetric:         stageExecutionMetric,
		runningSlurmJobsList:	      runningSlurmJobsList,
		userInfoList:		      userInfoList,
	}
}

func (e *exporter) Collect(ch chan<- prometheus.Metric) {

	scrapeOK := true

	e.scrapeMutex.Lock() // Do mutex unlock ASAP

	if e.scrapeActive {
		scrapeOK = false
		log.Debug("Collect is still active... - Skipping now")
		e.scrapeMutex.Unlock()
	} else {
		log.Debug("Collect started")

		e.scrapeActive = true
		e.scrapeMutex.Unlock()

		e.stageExecutionMetric.Reset()
		e.runningSlurmJobsList.Reset()
		e.userInfoList.Reset()

		go retrieveRunningJobs(e.channelRunningJobs)
		go createUserInfoMap(e.channelUserInfo)
		go createGroupInfoMap(e.channelGroupInfo)

		runningJobsResult := <-e.channelRunningJobs
		userInfoResult := <-e.channelUserInfo
		groupInfoResult := <-e.channelGroupInfo

		for _, job := range runningJobsResult.jobs {
			e.runningSlurmJobsList.WithLabelValues(job.jobid, job.account, job.user).Add(1)
		}

		for uid, info := range userInfoResult.users {
			groupInfo, _:= groupInfoResult.groups[info.gid]
			e.userInfoList.WithLabelValues(strconv.Itoa(uid), info.user, groupInfo.group).Add(1)
		}

		e.stageExecutionMetric.Collect(ch)
		e.runningSlurmJobsList.Collect(ch)
		e.userInfoList.Collect(ch)

		e.scrapeActive = false

		log.Debug("Collect finished")
	}

	if scrapeOK {
		e.scrapeOKMetric.Set(1)
	} else {
		e.scrapeOKMetric.Set(0)
	}

	e.scrapeOKMetric.Collect(ch)
}

func (e *exporter) Describe(ch chan<- *prometheus.Desc) {
	e.scrapeOKMetric.Describe(ch)
	e.stageExecutionMetric.Describe(ch)
	e.runningSlurmJobsList.Describe(ch)
	e.userInfoList.Describe(ch)
}

func recordScrapeError(sender string, err error, scrapeOK *bool) {
	if err != nil {
		log.Errorln(sender, ": ", err)
		if scrapeOK != nil {
			*scrapeOK = false
		}
	}
}
