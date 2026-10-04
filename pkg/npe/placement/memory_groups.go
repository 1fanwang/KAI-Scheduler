// Copyright 2026 NVIDIA CORPORATION
// SPDX-License-Identifier: Apache-2.0

package placement

import (
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strings"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	podresourcesv1 "k8s.io/kubelet/pkg/apis/podresources/v1"

	"github.com/kai-scheduler/KAI-scheduler/pkg/apis/scheduling/v1alpha2"
	commonpod "github.com/kai-scheduler/KAI-scheduler/pkg/common/pod"
	"github.com/kai-scheduler/KAI-scheduler/pkg/common/resources"
)

const incompleteMemoryGroups = "null"

// MemoryGroupsValue builds the pod's observed memory-group annotation from podResources allocations.
// Non-Guaranteed pods return "[]". Guaranteed pods return "" before allocations start, "null" for
// incomplete or invalid observations, or a complete group list. Previously observed pods never
// revert to "", preserving the override of persisted predictions when observations regress.
func MemoryGroupsValue(pod *v1.Pod, observed *podresourcesv1.PodResources, previouslyObserved bool) string {
	if !commonpod.IsGuaranteed(pod) {
		return "[]"
	}
	expected := map[string]v1.Container{}
	for _, container := range pod.Spec.Containers {
		expected[container.Name] = container
	}
	for _, container := range pod.Spec.InitContainers {
		if restartable(container) {
			expected[container.Name] = container
		}
	}
	groups, represented, err := aggregateMemoryGroups(observed, expected)
	if err != nil {
		return incompleteMemoryGroups
	}
	complete, started := memoryObservationComplete(pod, represented, expected)
	if !complete {
		if len(groups) != 0 || started || previouslyObserved {
			return incompleteMemoryGroups
		}
		return ""
	}
	encoded, err := json.Marshal(groups)
	if err != nil {
		return incompleteMemoryGroups
	}
	return string(encoded)
}

func aggregateMemoryGroups(observed *podresourcesv1.PodResources, expected map[string]v1.Container) ([]v1alpha2.NUMAMemoryGroupPlacement, map[string]*podresourcesv1.ContainerResources, error) {
	groups := []v1alpha2.NUMAMemoryGroupPlacement{}
	byMask := map[string]int{}
	represented := map[string]*podresourcesv1.ContainerResources{}
	maxAmount := resource.NewQuantity(math.MaxInt64, resource.BinarySI)
	for _, container := range observed.GetContainers() {
		if _, exists := represented[container.GetName()]; exists {
			return nil, nil, fmt.Errorf("duplicate container %q", container.GetName())
		}
		if len(container.GetMemory()) > 0 {
			if _, exists := expected[container.GetName()]; !exists {
				return nil, nil, fmt.Errorf("unexpected memory allocation for %q", container.GetName())
			}
		}
		represented[container.GetName()] = container
		for _, block := range container.GetMemory() {
			nodes := map[int64]struct{}{}
			for _, node := range block.GetTopology().GetNodes() {
				if node == nil || node.GetID() < 0 {
					return nil, nil, fmt.Errorf("invalid memory NUMA node")
				}
				nodes[node.GetID()] = struct{}{}
			}
			if len(nodes) == 0 || block.GetSize() > math.MaxInt64 {
				return nil, nil, fmt.Errorf("invalid memory block topology or size")
			}
			ids := make([]int64, 0, len(nodes))
			for node := range nodes {
				ids = append(ids, node)
			}
			sort.Slice(ids, func(first, second int) bool { return ids[first] < ids[second] })
			zones := make([]string, len(ids))
			for index, node := range ids {
				zones[index] = zoneName(node)
			}
			key := strings.Join(zones, ",")
			index, exists := byMask[key]
			if !exists {
				index = len(groups)
				byMask[key] = index
				groups = append(groups, v1alpha2.NUMAMemoryGroupPlacement{MemoryNodes: zones, Amount: v1.ResourceList{}})
			}
			name := v1.ResourceName(memoryResourceName(block.GetMemoryType()))
			if !resources.IsMemoryResource(name) {
				return nil, nil, fmt.Errorf("unsupported memory resource %q", name)
			}
			quantity := groups[index].Amount[name]
			quantity.Add(*resource.NewQuantity(int64(block.GetSize()), resource.BinarySI))
			if quantity.Cmp(*maxAmount) > 0 {
				return nil, nil, fmt.Errorf("memory amount overflows int64")
			}
			groups[index].Amount[name] = quantity
		}
	}
	sort.Slice(groups, func(first, second int) bool {
		return strings.Join(groups[first].MemoryNodes, ",") < strings.Join(groups[second].MemoryNodes, ",")
	})
	return groups, represented, nil
}

func memoryObservationComplete(pod *v1.Pod, represented map[string]*podresourcesv1.ContainerResources, expected map[string]v1.Container) (bool, bool) {
	started := map[string]bool{}
	applicationStarted := false
	for _, status := range pod.Status.ContainerStatuses {
		started[status.Name] = status.State.Running != nil || status.State.Terminated != nil
		applicationStarted = applicationStarted || started[status.Name]
	}
	ordinaryInitStarted := false
	anyStarted := applicationStarted
	ordinaryInit := map[string]bool{}
	for _, container := range pod.Spec.InitContainers {
		ordinaryInit[container.Name] = !restartable(container)
	}
	for _, status := range pod.Status.InitContainerStatuses {
		started[status.Name] = status.State.Running != nil || status.State.Terminated != nil
		anyStarted = anyStarted || started[status.Name]
		if ordinaryInit[status.Name] && started[status.Name] {
			if status.State.Running != nil {
				return false, true
			}
			ordinaryInitStarted = true
		}
	}
	if ordinaryInitStarted && !applicationStarted {
		return false, true
	}
	for _, container := range expected {
		for name, amount := range container.Resources.Requests {
			if !resources.IsMemoryResource(name) || amount.Sign() <= 0 {
				continue
			}
			entry, exists := represented[container.Name]
			if !started[container.Name] || !exists {
				return false, anyStarted
			}
			found := false
			for _, block := range entry.GetMemory() {
				if memoryResourceName(block.GetMemoryType()) == string(name) {
					found = true
				}
			}
			if !found {
				return false, true
			}
		}
	}
	return true, anyStarted
}

func restartable(container v1.Container) bool {
	return container.RestartPolicy != nil && *container.RestartPolicy == v1.ContainerRestartPolicyAlways
}
