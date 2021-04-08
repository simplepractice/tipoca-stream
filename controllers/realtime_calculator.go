package controllers

import (
	"fmt"
	klog "github.com/practo/klog/v2"
	tipocav1 "github.com/practo/tipoca-stream/redshiftsink/api/v1"
	kafka "github.com/practo/tipoca-stream/redshiftsink/pkg/kafka"
	"math/rand"
	"sync"
	"time"
)

type topicLast struct {
	topic string
	last  int64
}

type realtimeCalculator struct {
	rsk         *tipocav1.RedshiftSink
	watcher     kafka.Watcher
	topicGroups map[string]tipocav1.Group
	cache       *sync.Map

	batchersRealtime []string
	loadersRealtime  []string

	batchersLast []topicLast
	loadersLast  []topicLast
}

func newRealtimeCalculator(
	rsk *tipocav1.RedshiftSink,
	watcher kafka.Watcher,
	topicGroups map[string]tipocav1.Group,
	cache *sync.Map,
) *realtimeCalculator {

	return &realtimeCalculator{
		rsk:          rsk,
		watcher:      watcher,
		topicGroups:  topicGroups,
		cache:        cache,
		batchersLast: []topicLast{},
		loadersLast:  []topicLast{},
	}
}

func (r *realtimeCalculator) maxLag(topic string) (int64, int64) {
	var maxBatcherLag, maxLoaderLag int64
	if r.rsk.Spec.ReleaseCondition == nil {
		maxBatcherLag = DefaultMaxBatcherLag
		maxLoaderLag = DefautMaxLoaderLag
	} else {
		if r.rsk.Spec.ReleaseCondition.MaxBatcherLag != nil {
			maxBatcherLag = *r.rsk.Spec.ReleaseCondition.MaxBatcherLag
		}
		if r.rsk.Spec.ReleaseCondition.MaxLoaderLag != nil {
			maxLoaderLag = *r.rsk.Spec.ReleaseCondition.MaxLoaderLag
		}
		if r.rsk.Spec.TopicReleaseCondition != nil {
			d, ok := r.rsk.Spec.TopicReleaseCondition[topic]
			if ok {
				if d.MaxBatcherLag != nil {
					maxBatcherLag = *d.MaxBatcherLag
				}
				if d.MaxLoaderLag != nil {
					maxLoaderLag = *d.MaxLoaderLag
				}
			}
		}
	}

	return maxBatcherLag, maxLoaderLag
}

// fetchRealtimeCache tires to get the topicRealtimeInfo from cache
// if found in cache and cache is valid it returns true and the info
// else it returns no info and false
func (r *realtimeCalculator) fetchRealtimeCache(
	topic string,
) (
	topicRealtimeInfo, bool,
) {
	loadedInfo, ok := r.cache.Load(topic)
	if !ok {
		return topicRealtimeInfo{}, false
	}

	// 120 to 240 seconds, randomness to prevent multiple parallel calls
	validSec := rand.Intn(120) + 120
	klog.V(5).Infof(
		"rsk/%s, %s, cacheValid=%vs",
		r.rsk.Name,
		topic,
		validSec,
	)

	info := loadedInfo.(topicRealtimeInfo)
	if cacheValid(time.Second*time.Duration(validSec), info.lastUpdate) {
		klog.V(4).Infof(
			"rsk/%s (realtime cache hit) topic: %s",
			r.rsk.Name,
			topic,
		)
		return info, true
	}

	return topicRealtimeInfo{}, false
}

type offsetPosition struct {
	last    *int64
	current *int64
}

type topicRealtimeInfo struct {
	lastUpdate      *int64
	batcher         *offsetPosition
	loader          *offsetPosition
	batcherRealtime bool
	loaderRealtime  bool
}

// fetchRealtimeInfo fetches the offset info for the topic
func (r *realtimeCalculator) fetchRealtimeInfo(
	topic string,
	loaderTopic *string,
	group tipocav1.Group,
) (
	topicRealtimeInfo, error,
) {
	klog.V(2).Infof("rsk/%s (fetching realtime) topic: %s", r.rsk.Name, topic)

	now := time.Now().UnixNano()
	info := topicRealtimeInfo{
		batcher:         &offsetPosition{},
		loader:          &offsetPosition{},
		batcherRealtime: false,
		loaderRealtime:  false,
		lastUpdate:      &now,
	}

	// batcher's lag analysis: a) get last
	batcherLast, err := r.watcher.LastOffset(topic, 0)
	if err != nil {
		return info, fmt.Errorf("Error getting last offset for %s", topic)
	}
	info.batcher.last = &batcherLast
	klog.V(4).Infof("rsk/%s %s, lastOffset=%v", r.rsk.Name, topic, batcherLast)

	// batcher's lag analysis: b) get current
	batcherCurrent, err := r.watcher.CurrentOffset(
		consumerGroupID(r.rsk.Name, r.rsk.Namespace, group.ID, "-batcher"),
		topic,
		0,
	)
	if err != nil {
		return info, err
	}
	klog.V(4).Infof("rsk/%s %s, currentOffset=%v (queried)", r.rsk.Name, topic, batcherCurrent)
	if batcherCurrent == -1 {
		info.batcher.current = nil
		klog.V(4).Infof("rsk/%s %s, batcher cg 404, not realtime", r.rsk.Name, topic)
		return info, nil
	} else {
		info.batcher.current = &batcherCurrent
	}

	if loaderTopic == nil {
		return info, nil
	}

	// loader's lag analysis: a) get last
	loaderLast, err := r.watcher.LastOffset(*loaderTopic, 0)
	if err != nil {
		return info, fmt.Errorf("Error getting last offset for %s", *loaderTopic)
	}
	info.loader.last = &loaderLast
	klog.V(4).Infof("rsk/%s %s, lastOffset=%v", r.rsk.Name, *loaderTopic, loaderLast)

	// loader's lag analysis: b) get current
	loaderCurrent, err := r.watcher.CurrentOffset(
		consumerGroupID(r.rsk.Name, r.rsk.Namespace, group.ID, "-loader"),
		*loaderTopic,
		0,
	)
	if err != nil {
		return info, err
	}
	klog.V(4).Infof("rsk/%s %s, currentOffset=%v (queried)", r.rsk.Name, *loaderTopic, loaderCurrent)
	if loaderCurrent == -1 {
		// CurrentOffset can be -1 in two cases (this may be required in batcher also)
		// 1. When the Consumer Group was never created in that case we return and consider the topic not realtime
		// 2. When the Consumer Group had processed before but now is showing -1 currentOffset as it is inactive due to less throughput.
		//    On such a scenario, we consider it realtime. We find this case by saving the currentOffset for the loader topcics in RedshiftSinkStatus.TopicGroup
		if group.LoaderCurrentOffset == nil {
			klog.V(2).Infof("%s, loader cg 404, not realtime", *loaderTopic)
			return info, nil
		}
		klog.V(4).Infof("rsk/%s %s, currentOffset=%v (old), cg 404, try realtime", r.rsk.Name, *loaderTopic, *group.LoaderCurrentOffset)
		// give the topic the opportunity to release based on its last found currentOffset
		info.loader.current = group.LoaderCurrentOffset
	} else {
		group.LoaderCurrentOffset = &loaderCurrent
		// updates the new queried loader offset
		klog.V(4).Infof("rsk/%s %s, cg found", r.rsk.Name, *loaderTopic)
		updateTopicGroup(r.rsk, topic, group)
		info.loader.current = &loaderCurrent
	}

	return info, nil
}

// calculate computes the realtime topics and updates its realtime info
func (r *realtimeCalculator) calculate(reloading []string, currentRealtime []string) []string {
	if len(reloading) == 0 {
		return currentRealtime
	}

	realtimeTopics := []string{}
	current := toMap(currentRealtime)

	allTopics, err := r.watcher.Topics()
	if err != nil {
		klog.Errorf(
			"Ignoring realtime update. Error fetching all topics, err:%v",
			err,
		)
		return currentRealtime
	}
	allTopicsMap := toMap(allTopics)

	for _, topic := range reloading {
		group, ok := r.topicGroups[topic]
		if !ok {
			klog.Errorf("topicGroup 404 in status for: %s", topic)
			continue
		}

		var loaderTopic *string
		ltopic := r.rsk.Spec.KafkaLoaderTopicPrefix + group.ID + "-" + topic
		_, ok = allTopicsMap[ltopic]
		if !ok {
			klog.V(2).Infof("%s topic 404, not realtime.", ltopic)
		} else {
			loaderTopic = &ltopic
		}

		now := time.Now().UnixNano()

		info, hit := r.fetchRealtimeCache(topic)
		if !hit { // fetch again, cache miss
			info, err = r.fetchRealtimeInfo(topic, loaderTopic, group)
			if err != nil {
				klog.Errorf(
					"rsk/%s Error fetching realtime info for topic: %s, err: %v",
					r.rsk.Name,
					topic,
					err,
				)
				// if there is an error in finding lag
				// and the topic was already in realtime consider it realtime
				// consumer groups disappear due to inactivity, hence this
				_, ok := current[topic]
				if ok {
					r.cache.Store(
						topic,
						topicRealtimeInfo{
							batcherRealtime: true,
							loaderRealtime:  true,
							lastUpdate:      &now,
						},
					)
					realtimeTopics = append(realtimeTopics, topic)
					r.batchersRealtime = append(r.batchersRealtime, topic)
					r.loadersRealtime = append(r.loadersRealtime, ltopic)
					continue
				}
			}
		}

		// compute realtime
		maxBatcherLag, maxLoaderLag := r.maxLag(topic)
		if info.batcher != nil && info.batcher.last != nil {
			if info.batcher.current != nil {
				lag := *info.batcher.last - *info.batcher.current
				klog.V(2).Infof("rsk/%s: %s lag=%v", r.rsk.Name, topic, lag)
				if lag <= maxBatcherLag {
					klog.V(2).Infof("rsk/%s: %s batcher realtime", r.rsk.Name, topic)
					info.batcherRealtime = true
					r.batchersRealtime = append(r.batchersRealtime, topic)
				}
			}
			r.batchersLast = append(
				r.batchersLast,
				topicLast{
					topic: topic,
					last:  *info.batcher.last,
				},
			)
		}
		if info.loader != nil && info.loader.last != nil {
			if info.loader.current != nil {
				lag := *info.loader.last - *info.loader.current
				klog.V(2).Infof("rsk/%s: %s lag=%v", r.rsk.Name, ltopic, lag)
				if lag <= maxLoaderLag {
					klog.V(2).Infof("rsk/%s: %s loader realtime", r.rsk.Name, ltopic)
					info.loaderRealtime = true
					r.loadersRealtime = append(r.loadersRealtime, ltopic)
				}
			}
			r.loadersLast = append(
				r.loadersLast,
				topicLast{
					topic: ltopic,
					last:  *info.loader.last,
				},
			)
		}

		if info.batcherRealtime && info.loaderRealtime {
			klog.V(2).Infof("rsk/%s: %s realtime", r.rsk.Name, topic)
			realtimeTopics = append(realtimeTopics, topic)
		} else {
			if info.batcherRealtime == false && info.loaderRealtime == false {
				klog.V(2).Infof("%v: waiting to reach realtime", topic)
				klog.V(2).Infof("%v: waiting to reach realtime", ltopic)
			} else if info.batcherRealtime == false {
				klog.V(2).Infof("%v: waiting to reach realtime", topic)
			} else if info.loaderRealtime == false {
				klog.V(2).Infof("%v: waiting to reach realtime", ltopic)
			}
		}

		if !hit {
			info.lastUpdate = &now
		}
		r.cache.Store(topic, info)
	}

	return realtimeTopics
}