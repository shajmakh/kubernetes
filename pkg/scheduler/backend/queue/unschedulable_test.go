package queue

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
)

func TestUnschedulablePodsMap(t *testing.T) {
	var pods = []*v1.Pod{
		st.MakePod().Name("p0").Namespace("ns1").Annotation("annot1", "val1").NominatedNodeName("node1").Obj(),
		st.MakePod().Name("p1").Namespace("ns1").Annotation("annot", "val").Obj(),
		st.MakePod().Name("p2").Namespace("ns2").Annotation("annot2", "val2").Annotation("annot3", "val3").NominatedNodeName("node3").Obj(),
		st.MakePod().Name("p3").Namespace("ns4").Annotation("annot2", "val2").Annotation("annot3", "val3").NominatedNodeName("node1").Obj(),
	}
	var updatedPods = make([]*v1.Pod, len(pods))
	updatedPods[0] = pods[0].DeepCopy()
	updatedPods[1] = pods[1].DeepCopy()
	updatedPods[3] = pods[3].DeepCopy()

	tests := []struct {
		name                   string
		podsToAdd              []*v1.Pod
		expectedMapAfterAdd    map[string]*framework.QueuedPodInfo
		podsToUpdate           []*v1.Pod
		expectedMapAfterUpdate map[string]*framework.QueuedPodInfo
		podsToDelete           []*v1.Pod
		expectedMapAfterDelete map[string]*framework.QueuedPodInfo
	}{
		{
			name:      "create, update, delete subset of pods",
			podsToAdd: []*v1.Pod{pods[0], pods[1], pods[2], pods[3]},
			expectedMapAfterAdd: map[string]*framework.QueuedPodInfo{
				util.GetPodFullName(pods[0]): {PodInfo: mustNewTestPodInfo(t, pods[0]), UnschedulablePlugins: sets.New[string]()},
				util.GetPodFullName(pods[1]): {PodInfo: mustNewTestPodInfo(t, pods[1]), UnschedulablePlugins: sets.New[string]()},
				util.GetPodFullName(pods[2]): {PodInfo: mustNewTestPodInfo(t, pods[2]), UnschedulablePlugins: sets.New[string]()},
				util.GetPodFullName(pods[3]): {PodInfo: mustNewTestPodInfo(t, pods[3]), UnschedulablePlugins: sets.New[string]()},
			},
			podsToUpdate: []*v1.Pod{updatedPods[0]},
			expectedMapAfterUpdate: map[string]*framework.QueuedPodInfo{
				util.GetPodFullName(pods[0]): {PodInfo: mustNewTestPodInfo(t, updatedPods[0]), UnschedulablePlugins: sets.New[string]()},
				util.GetPodFullName(pods[1]): {PodInfo: mustNewTestPodInfo(t, pods[1]), UnschedulablePlugins: sets.New[string]()},
				util.GetPodFullName(pods[2]): {PodInfo: mustNewTestPodInfo(t, pods[2]), UnschedulablePlugins: sets.New[string]()},
				util.GetPodFullName(pods[3]): {PodInfo: mustNewTestPodInfo(t, pods[3]), UnschedulablePlugins: sets.New[string]()},
			},
			podsToDelete: []*v1.Pod{pods[0], pods[1]},
			expectedMapAfterDelete: map[string]*framework.QueuedPodInfo{
				util.GetPodFullName(pods[2]): {PodInfo: mustNewTestPodInfo(t, pods[2]), UnschedulablePlugins: sets.New[string]()},
				util.GetPodFullName(pods[3]): {PodInfo: mustNewTestPodInfo(t, pods[3]), UnschedulablePlugins: sets.New[string]()},
			},
		},
		{
			name:      "create, update, delete all",
			podsToAdd: []*v1.Pod{pods[0], pods[3]},
			expectedMapAfterAdd: map[string]*framework.QueuedPodInfo{
				util.GetPodFullName(pods[0]): {PodInfo: mustNewTestPodInfo(t, pods[0]), UnschedulablePlugins: sets.New[string]()},
				util.GetPodFullName(pods[3]): {PodInfo: mustNewTestPodInfo(t, pods[3]), UnschedulablePlugins: sets.New[string]()},
			},
			podsToUpdate: []*v1.Pod{updatedPods[3]},
			expectedMapAfterUpdate: map[string]*framework.QueuedPodInfo{
				util.GetPodFullName(pods[0]): {PodInfo: mustNewTestPodInfo(t, pods[0]), UnschedulablePlugins: sets.New[string]()},
				util.GetPodFullName(pods[3]): {PodInfo: mustNewTestPodInfo(t, updatedPods[3]), UnschedulablePlugins: sets.New[string]()},
			},
			podsToDelete:           []*v1.Pod{pods[0], pods[3]},
			expectedMapAfterDelete: map[string]*framework.QueuedPodInfo{},
		},
		{
			name:      "delete non-existing and existing pods",
			podsToAdd: []*v1.Pod{pods[1], pods[2]},
			expectedMapAfterAdd: map[string]*framework.QueuedPodInfo{
				util.GetPodFullName(pods[1]): {PodInfo: mustNewTestPodInfo(t, pods[1]), UnschedulablePlugins: sets.New[string]()},
				util.GetPodFullName(pods[2]): {PodInfo: mustNewTestPodInfo(t, pods[2]), UnschedulablePlugins: sets.New[string]()},
			},
			podsToUpdate: []*v1.Pod{updatedPods[1]},
			expectedMapAfterUpdate: map[string]*framework.QueuedPodInfo{
				util.GetPodFullName(pods[1]): {PodInfo: mustNewTestPodInfo(t, updatedPods[1]), UnschedulablePlugins: sets.New[string]()},
				util.GetPodFullName(pods[2]): {PodInfo: mustNewTestPodInfo(t, pods[2]), UnschedulablePlugins: sets.New[string]()},
			},
			podsToDelete: []*v1.Pod{pods[2], pods[3]},
			expectedMapAfterDelete: map[string]*framework.QueuedPodInfo{
				util.GetPodFullName(pods[1]): {PodInfo: mustNewTestPodInfo(t, updatedPods[1]), UnschedulablePlugins: sets.New[string]()},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			upm := newUnschedulablePods(nil, nil)
			for _, p := range test.podsToAdd {
				upm.addOrUpdate(newQueuedPodInfoForLookup(p), framework.EventUnscheduledPodAdd.Label())
			}
			if diff := cmp.Diff(test.expectedMapAfterAdd, upm.podInfoMap, cmpopts.IgnoreUnexported(framework.PodInfo{})); diff != "" {
				t.Errorf("Unexpected map after adding pods(-want, +got):\n%s", diff)
			}

			if len(test.podsToUpdate) > 0 {
				for _, p := range test.podsToUpdate {
					upm.addOrUpdate(newQueuedPodInfoForLookup(p), framework.EventUnscheduledPodUpdate.Label())
				}
				if diff := cmp.Diff(test.expectedMapAfterUpdate, upm.podInfoMap, cmpopts.IgnoreUnexported(framework.PodInfo{})); diff != "" {
					t.Errorf("Unexpected map after updating pods (-want, +got):\n%s", diff)
				}
			}
			for _, p := range test.podsToDelete {
				upm.delete(p, false)
			}
			if diff := cmp.Diff(test.expectedMapAfterDelete, upm.podInfoMap, cmpopts.IgnoreUnexported(framework.PodInfo{})); diff != "" {
				t.Errorf("Unexpected map after deleting pods (-want, +got):\n%s", diff)
			}
			upm.clear()
			if len(upm.podInfoMap) != 0 {
				t.Errorf("Expected the map to be empty, but has %v elements.", len(upm.podInfoMap))
			}
		})
	}
}
