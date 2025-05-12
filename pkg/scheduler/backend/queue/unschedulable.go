/*
Copyright 2025 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package queue

import (
	v1 "k8s.io/api/core/v1"
	"reflect"

	"k8s.io/kubernetes/pkg/scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/metrics"
	"k8s.io/kubernetes/pkg/scheduler/util"
)

// unschedulablePods is a wrapper for UnschedulablePods data structure
type unschedulablePods interface {
	// clear removes all the entries from the unschedulable podInfoMap.
	clear()
	// addOrUpdate adds a pod to the unschedulable podInfoMap.
	addOrUpdate(pInfo *framework.QueuedPodInfo, event string)
	// update updates the pod in activeQ if oldPodInfo is already in the queue.
	// It returns new pod info if updated, nil otherwise.
	update(newPod *v1.Pod, oldPodInfo *framework.QueuedPodInfo) *framework.QueuedPodInfo
	// add adds a new pod to the activeQ.
	// The event should show which event triggered this addition and is used for the metric recording.
	// This method should be called in activeQueue.underLock().
	add(pInfo *framework.QueuedPodInfo, event string)
	// delete deletes a pod from the unschedulable podInfoMap.
	delete(pod *v1.Pod, gated bool)
	// get returns the QueuedPodInfo if a pod with the same key as the key of the given "pod"
	get(pod *v1.Pod) (pInfo *framework.QueuedPodInfo, ok bool)
	// has inform if pInfo exists in the queue.
	has(pInfo *framework.QueuedPodInfo) bool
	// list returns all pods that are in the queue.
	list() []*v1.Pod
	// len returns length of the queue.
	len() int
}

// UnschedulablePods holds pods that cannot be scheduled. This data structure
// is used to implement unschedulable.
type UnschedulablePods struct {
	// podInfoMap is a map key by a pod's full-name and the value is a pointer to the QueuedPodInfo.
	podInfoMap map[string]*framework.QueuedPodInfo
	keyFunc    func(*v1.Pod) string
	// unschedulableRecorder/gatedRecorder updates the counter when elements of an unschedulablePodsMap
	// get added or removed, and it does nothing if it's nil.
	unschedulableRecorder, gatedRecorder metrics.MetricRecorder
}

// add adds a pod to the unschedulable podInfoMap.
// The event should show which event triggered the addition and is used for the metric recording.
func (u *UnschedulablePods) add(pInfo *framework.QueuedPodInfo, event string) {
	podID := u.keyFunc(pInfo.Pod)
	if _, exists := u.podInfoMap[podID]; !exists {
		if pInfo.Gated && u.gatedRecorder != nil {
			u.gatedRecorder.Inc()
		} else if !pInfo.Gated && u.unschedulableRecorder != nil {
			u.unschedulableRecorder.Inc()
		}
		metrics.SchedulerQueueIncomingPods.WithLabelValues("unschedulable", event).Inc()
	}
	u.podInfoMap[podID] = pInfo
}

// Update updates unschedulable pod's info in podInfoMap.
func (u *UnschedulablePods) update(newPod *v1.Pod, oldPodInfo *framework.QueuedPodInfo) *framework.QueuedPodInfo {
	podID := u.keyFunc(oldPodInfo.Pod)
	if !reflect.DeepEqual(u.podInfoMap[podID].Pod, newPod) {
		u.podInfoMap[podID].Pod = newPod
		return u.podInfoMap[podID]
	}
	return nil
}

// addOrUpdate adds a pod to the unschedulable podInfoMap.
// The event should show which event triggered the addition and is used for the metric recording.
func (u *UnschedulablePods) addOrUpdate(pInfo *framework.QueuedPodInfo, event string) {
	podID := u.keyFunc(pInfo.Pod)
	if _, exists := u.podInfoMap[podID]; !exists {
		if pInfo.Gated && u.gatedRecorder != nil {
			u.gatedRecorder.Inc()
		} else if !pInfo.Gated && u.unschedulableRecorder != nil {
			u.unschedulableRecorder.Inc()
		}
		metrics.SchedulerQueueIncomingPods.WithLabelValues("unschedulable", event).Inc()
	}
	u.podInfoMap[podID] = pInfo
}

// delete deletes a pod from the unschedulable podInfoMap.
// The `gated` parameter is used to figure out which metric should be decreased.
func (u *UnschedulablePods) delete(pod *v1.Pod, gated bool) {
	podID := u.keyFunc(pod)
	if _, exists := u.podInfoMap[podID]; exists {
		if gated && u.gatedRecorder != nil {
			u.gatedRecorder.Dec()
		} else if !gated && u.unschedulableRecorder != nil {
			u.unschedulableRecorder.Dec()
		}
	}
	delete(u.podInfoMap, podID)
}

// get returns the QueuedPodInfo if a pod with the same key as the key of the given "pod"
// is found in the map. It returns nil otherwise.
func (u *UnschedulablePods) get(pod *v1.Pod) (*framework.QueuedPodInfo, bool) {
	podKey := u.keyFunc(pod)
	if pInfo, exists := u.podInfoMap[podKey]; exists {
		return pInfo, true
	}
	return nil, false
}

// clear removes all the entries from the unschedulable podInfoMap.
func (u *UnschedulablePods) clear() {
	u.podInfoMap = make(map[string]*framework.QueuedPodInfo)
	if u.unschedulableRecorder != nil {
		u.unschedulableRecorder.Clear()
	}
	if u.gatedRecorder != nil {
		u.gatedRecorder.Clear()
	}
}

// newUnschedulablePods initializes a new object of UnschedulablePods.
func newUnschedulablePods(unschedulableRecorder, gatedRecorder metrics.MetricRecorder) *UnschedulablePods {
	return &UnschedulablePods{
		podInfoMap:            make(map[string]*framework.QueuedPodInfo),
		keyFunc:               util.GetPodFullName,
		unschedulableRecorder: unschedulableRecorder,
		gatedRecorder:         gatedRecorder,
	}
}

// has informs if pInfo is unschedulable pod.
func (up *UnschedulablePods) has(pInfo *framework.QueuedPodInfo) bool {
	for _, p := range up.podInfoMap {
		if reflect.DeepEqual(pInfo, p) {
			return true
		}
	}
	return false
}

// list returns all pods that are in podInfoMap.
func (up *UnschedulablePods) list() []*v1.Pod {
	var result []*v1.Pod
	for _, pInfo := range up.podInfoMap {
		result = append(result, pInfo.Pod)
	}
	return result
}

// len returns length of podInfoMap.
func (up *UnschedulablePods) len() int {
	return len(up.podInfoMap)
}
