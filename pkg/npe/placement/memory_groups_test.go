// Copyright 2026 NVIDIA CORPORATION
// SPDX-License-Identifier: Apache-2.0

package placement

import (
	"encoding/json"
	"fmt"
	"math"
	"testing"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	podresourcesv1 "k8s.io/kubelet/pkg/apis/podresources/v1"

	"github.com/kai-scheduler/KAI-scheduler/pkg/apis/scheduling/v1alpha2"
)

func memoryBlock(size uint64, nodes ...int64) *podresourcesv1.ContainerMemory {
	block := &podresourcesv1.ContainerMemory{MemoryType: "memory", Size: size, Topology: &podresourcesv1.TopologyInfo{}}
	for _, node := range nodes {
		block.Topology.Nodes = append(block.Topology.Nodes, &podresourcesv1.NUMANode{ID: node})
	}
	return block
}

func managedPod(names ...string) *v1.Pod {
	pod := &v1.Pod{Status: v1.PodStatus{QOSClass: v1.PodQOSGuaranteed}}
	for _, name := range names {
		pod.Spec.Containers = append(pod.Spec.Containers, v1.Container{Name: name, Resources: v1.ResourceRequirements{Requests: v1.ResourceList{v1.ResourceMemory: resource.MustParse("1Gi")}}})
		pod.Status.ContainerStatuses = append(pod.Status.ContainerStatuses, v1.ContainerStatus{Name: name, State: v1.ContainerState{Running: &v1.ContainerStateRunning{}}})
	}
	return pod
}

func TestMemoryGroupsAggregateMasksAndRetainZero(t *testing.T) {
	observed := &podresourcesv1.PodResources{Containers: []*podresourcesv1.ContainerResources{
		{Name: "first", Memory: []*podresourcesv1.ContainerMemory{memoryBlock(10, 1, 0, 0), memoryBlock(0, 2)}},
		{Name: "second", Memory: []*podresourcesv1.ContainerMemory{memoryBlock(20, 0, 1)}},
	}}
	value := MemoryGroupsValue(managedPod("first", "second"), observed, false)
	var groups []v1alpha2.NUMAMemoryGroupPlacement
	if err := json.Unmarshal([]byte(value), &groups); err != nil {
		t.Fatal(err)
	}
	if len(groups) != 2 || len(groups[0].MemoryNodes) != 2 || groups[0].MemoryNodes[0] != "node-0" {
		t.Fatalf("unexpected groups: %s", value)
	}
	amount := groups[0].Amount[v1.ResourceMemory]
	zero := groups[1].Amount[v1.ResourceMemory]
	if amount.Value() != 30 || !zero.IsZero() {
		t.Fatalf("unexpected amounts: %s", value)
	}
	if _, exists := groups[1].Amount[v1.ResourceMemory]; !exists {
		t.Fatal("zero resource omitted")
	}
}

func TestMemoryGroupsCompleteness(t *testing.T) {
	restartPolicy := v1.ContainerRestartPolicyAlways
	tests := []struct {
		name     string
		modify   func(*v1.Pod)
		observed *podresourcesv1.PodResources
		previous bool
		want     string
	}{
		{name: "prestart", modify: func(pod *v1.Pod) { pod.Status.ContainerStatuses = nil }, want: ""},
		{name: "missing running block", want: "null"},
		{name: "regressed before status catches up", modify: func(pod *v1.Pod) { pod.Status.ContainerStatuses = nil }, previous: true, want: "null"},
		{name: "partial containers", modify: func(pod *v1.Pod) {
			pod.Spec.Containers = append(pod.Spec.Containers, v1.Container{Name: "second", Resources: pod.Spec.Containers[0].Resources})
		}, observed: &podresourcesv1.PodResources{Containers: []*podresourcesv1.ContainerResources{{Name: "first", Memory: []*podresourcesv1.ContainerMemory{memoryBlock(10, 0)}}}}, want: "null"},
		{name: "hidden ordinary init", modify: func(pod *v1.Pod) {
			pod.Spec.InitContainers = []v1.Container{{Name: "init"}}
			pod.Status.InitContainerStatuses = []v1.ContainerStatus{{Name: "init", State: v1.ContainerState{Running: &v1.ContainerStateRunning{}}}}
			pod.Status.ContainerStatuses = nil
		}, want: "null"},
		{name: "ordinary init restarted after app termination", modify: func(pod *v1.Pod) {
			pod.Spec.InitContainers = []v1.Container{{Name: "init"}}
			pod.Status.InitContainerStatuses = []v1.ContainerStatus{{Name: "init", State: v1.ContainerState{Running: &v1.ContainerStateRunning{}}}}
			pod.Status.ContainerStatuses[0].State = v1.ContainerState{Terminated: &v1.ContainerStateTerminated{}}
		}, observed: &podresourcesv1.PodResources{Containers: []*podresourcesv1.ContainerResources{{Name: "first", Memory: []*podresourcesv1.ContainerMemory{memoryBlock(10, 0)}}}}, want: "null"},
		{name: "sum overflow", observed: &podresourcesv1.PodResources{Containers: []*podresourcesv1.ContainerResources{{Name: "first", Memory: []*podresourcesv1.ContainerMemory{memoryBlock(math.MaxInt64, 0), memoryBlock(1, 0)}}}}, want: "null"},
		{name: "block overflow", observed: &podresourcesv1.PodResources{Containers: []*podresourcesv1.ContainerResources{{Name: "first", Memory: []*podresourcesv1.ContainerMemory{memoryBlock(math.MaxUint64, 0)}}}}, want: "null"},
		{name: "unknown memory container", observed: &podresourcesv1.PodResources{Containers: []*podresourcesv1.ContainerResources{{Name: "first", Memory: []*podresourcesv1.ContainerMemory{memoryBlock(10, 0)}}, {Name: "unknown", Memory: []*podresourcesv1.ContainerMemory{memoryBlock(1, 0)}}}}, want: "null"},
		{name: "duplicate memory container", observed: &podresourcesv1.PodResources{Containers: []*podresourcesv1.ContainerResources{{Name: "first", Memory: []*podresourcesv1.ContainerMemory{memoryBlock(10, 0)}}, {Name: "first", Memory: []*podresourcesv1.ContainerMemory{memoryBlock(1, 0)}}}}, want: "null"},
		{name: "ordinary init completed app not started", modify: func(pod *v1.Pod) {
			pod.Spec.InitContainers = []v1.Container{{Name: "init"}}
			pod.Status.InitContainerStatuses = []v1.ContainerStatus{{Name: "init", State: v1.ContainerState{Terminated: &v1.ContainerStateTerminated{}}}}
			pod.Status.ContainerStatuses = nil
		}, want: "null"},
		{name: "missing sidecar", modify: func(pod *v1.Pod) {
			pod.Spec.InitContainers = []v1.Container{{Name: "sidecar", RestartPolicy: &restartPolicy, Resources: pod.Spec.Containers[0].Resources}}
		}, observed: &podresourcesv1.PodResources{Containers: []*podresourcesv1.ContainerResources{{Name: "first", Memory: []*podresourcesv1.ContainerMemory{memoryBlock(10, 0)}}}}, want: "null"},
		{name: "invalid missing topology", observed: &podresourcesv1.PodResources{Containers: []*podresourcesv1.ContainerResources{{Name: "first", Memory: []*podresourcesv1.ContainerMemory{memoryBlock(10)}}}}, want: "null"},
		{name: "invalid node", observed: &podresourcesv1.PodResources{Containers: []*podresourcesv1.ContainerResources{{Name: "first", Memory: []*podresourcesv1.ContainerMemory{memoryBlock(10, -1)}}}}, want: "null"},
		{name: "non guaranteed empty", modify: func(pod *v1.Pod) { pod.Status.QOSClass = v1.PodQOSBurstable }, want: "[]"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			pod := managedPod("first")
			if test.modify != nil {
				test.modify(pod)
			}
			if got := MemoryGroupsValue(pod, test.observed, test.previous); got != test.want {
				t.Fatalf("got %q, want %q", got, test.want)
			}
		})
	}
}

func TestMemoryGroupsNonGuaranteedPods(t *testing.T) {
	states := []struct {
		name     string
		modify   func(*v1.Pod)
		observed *podresourcesv1.PodResources
		previous bool
	}{
		{name: "before startup", modify: func(pod *v1.Pod) { pod.Status.ContainerStatuses = nil }},
		{name: "missing running observation"},
		{name: "previous observation", previous: true},
		{name: "reported block", observed: &podresourcesv1.PodResources{Containers: []*podresourcesv1.ContainerResources{{Name: "first", Memory: []*podresourcesv1.ContainerMemory{memoryBlock(10, 0)}}}}},
		{name: "invalid block", observed: &podresourcesv1.PodResources{Containers: []*podresourcesv1.ContainerResources{{Name: "first", Memory: []*podresourcesv1.ContainerMemory{memoryBlock(10, -1)}}}}, previous: true},
		{name: "running ordinary init", modify: func(pod *v1.Pod) {
			pod.Spec.InitContainers = []v1.Container{{Name: "init"}}
			pod.Status.InitContainerStatuses = []v1.ContainerStatus{{Name: "init", State: v1.ContainerState{Running: &v1.ContainerStateRunning{}}}}
		}, previous: true},
	}
	for _, qosClass := range []v1.PodQOSClass{v1.PodQOSBurstable, v1.PodQOSBestEffort} {
		for _, statusPresent := range []bool{true, false} {
			for _, state := range states {
				name := fmt.Sprintf("%s/status=%t/%s", qosClass, statusPresent, state.name)
				t.Run(name, func(t *testing.T) {
					pod := managedPod("first")
					if qosClass == v1.PodQOSBestEffort {
						pod.Spec.Containers[0].Resources = v1.ResourceRequirements{}
					}
					pod.Status.QOSClass = ""
					if statusPresent {
						pod.Status.QOSClass = qosClass
					}
					if state.modify != nil {
						state.modify(pod)
					}
					if got := MemoryGroupsValue(pod, state.observed, state.previous); got != "[]" {
						t.Fatalf("got %q, want []", got)
					}
				})
			}
		}
	}
}

func TestMemoryGroupsBlockValidation(t *testing.T) {
	tests := []struct {
		name  string
		block *podresourcesv1.ContainerMemory
		want  string
	}{
		{name: "nil block", want: "null"},
		{name: "nil topology", block: &podresourcesv1.ContainerMemory{MemoryType: "memory", Size: 10}, want: "null"},
		{name: "nil node", block: &podresourcesv1.ContainerMemory{MemoryType: "memory", Size: 10, Topology: &podresourcesv1.TopologyInfo{Nodes: []*podresourcesv1.NUMANode{nil}}}, want: "null"},
		{name: "unsupported resource", block: &podresourcesv1.ContainerMemory{MemoryType: "cpu", Size: 10, Topology: memoryBlock(10, 0).Topology}, want: "null"},
		{name: "default memory type", block: &podresourcesv1.ContainerMemory{Size: 10, Topology: memoryBlock(10, 0).Topology}, want: `[{"memoryNodes":["node-0"],"amount":{"memory":"10"}}]`},
		{name: "maximum amount", block: memoryBlock(math.MaxInt64, 0), want: `[{"memoryNodes":["node-0"],"amount":{"memory":"9223372036854775807"}}]`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			observed := &podresourcesv1.PodResources{Containers: []*podresourcesv1.ContainerResources{{Name: "first", Memory: []*podresourcesv1.ContainerMemory{test.block}}}}
			if got := MemoryGroupsValue(managedPod("first"), observed, false); got != test.want {
				t.Fatalf("got %q, want %q", got, test.want)
			}
		})
	}
}

func TestMemoryGroupsRequiresEveryRequestedMemoryResource(t *testing.T) {
	pod := managedPod("first")
	pod.Spec.Containers[0].Resources.Requests[v1.ResourceName("hugepages-2Mi")] = resource.MustParse("2Mi")
	observed := &podresourcesv1.PodResources{Containers: []*podresourcesv1.ContainerResources{{Name: "first", Memory: []*podresourcesv1.ContainerMemory{memoryBlock(10, 0)}}}}
	if got := MemoryGroupsValue(pod, observed, false); got != "null" {
		t.Fatalf("missing hugepages yielded %q, want null", got)
	}
	hugepages := memoryBlock(2*1024*1024, 0)
	hugepages.MemoryType = "hugepages-2Mi"
	observed.Containers[0].Memory = append(observed.Containers[0].Memory, hugepages)
	want := `[{"memoryNodes":["node-0"],"amount":{"hugepages-2Mi":"2Mi","memory":"10"}}]`
	if got := MemoryGroupsValue(pod, observed, false); got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}
