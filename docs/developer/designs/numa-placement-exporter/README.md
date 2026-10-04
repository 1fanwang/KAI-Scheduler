<!--
Copyright 2026 NVIDIA CORPORATION
SPDX-License-Identifier: Apache-2.0
-->

# Per-Node NUMA Placement Exporter (NPE)

Related: [Memory Manager group admission modeling](https://github.com/kai-scheduler/KAI-Scheduler/issues/2013)

## Summary

The **NUMA Placement Exporter (NPE)** is an optional per-node DaemonSet that reads the kubelet's
**podResources API**, derives the actual NUMA-node placement of each pod's exclusive resources, and
publishes that attribution on the pod. The same API exposes each container's static Memory Manager
blocks, including their complete NUMA affinity and reserved size. NPE aggregates those blocks into
pod-level singleton and cross-NUMA groups. The
[NUMA scheduler plugin](../numa-topology/README.md) already consumes the existing per-zone
annotation. Consuming the new memory-group annotation is a separate scheduler change; older
scheduler versions ignore it, so the exporter can be upgraded first.

The exporter is optional for existing zone-placement accounting, which can fall back to predicted
placement. The KAI operator deploys it automatically when the `numa` plugin is enabled.
Memory-group-aware scheduling requires complete observed or predicted ownership; the exporter
supplies observations, not scheduler admission logic.

## Motivation

The `NodeResourceTopology` (NRT) CRD the scheduler consumes is **aggregate per zone** — it
reports "zone 0 has 2 free GPUs," never "pod V holds a GPU in zone 0." But the kubelet's
Topology Manager makes the real per-pod NUMA assignment at admission, and the scheduler never
observes it. The NUMA plugin therefore *predicts* each pod's zone.

Prediction is adequate for filtering (the kubelet backstops admission), but it is a genuine
problem for **reclaim simulation**: to free an aligned slot for a pending pod, the scheduler
must know which zone evicting a victim opens up. If it mispredicts the victim's zone, it can
evict a victim that does *not* create a usable aligned slot — a wasted eviction plus a
`TopologyAffinityError` bounce. (This bites specifically when the pending pod needs multiple
per-zone-scarce resources co-located; GPU-bound pods with abundant per-zone CPU are largely
immune — see the plugin doc's *Reclaim-simulation accuracy* note.)

The information the scheduler is missing exists on the node: the kubelet podResources API
reports, per container, the exact device IDs (with their NUMA `Topology`) and CPU IDs that were
allocated. Surfacing it replaces prediction when a complete observation is available.

## Goals

- Publish, per pod, the actual NUMA-node assignment of its topology-aligned resources.
- Publish each pod's Memory Manager groups and aggregate reserved amounts.
- Preserve existing zone-placement consumption while providing memory-group observations for
  follow-up scheduler support.
- Keep the scheduler's consumption cheap (no new informer, no new CRD if avoidable).

## Non-Goals

- Replacing the NRT exporter. NRT per-zone allocatable capacity is still required; this exporter
  adds per-pod attribution and Memory Manager group ownership.
- Influencing the kubelet's placement. The exporter is read-only with respect to allocation; it
  observes and reports, it does not hint or pin.
- Modeling static Memory Manager groups for non-Guaranteed memory. CPU and device observations
  remain useful whenever their kubelet managers provide exclusive placement, regardless of pod QoS.

## Design Details

### Architecture

A DaemonSet runs on each NUMA/GPU node. Its pod-placement path:

1. Connects to the local kubelet podResources gRPC socket
   (`/var/lib/kubelet/pod-resources/kubelet.sock`, hostPath-mounted, read-only).
2. Calls `List` to get per-pod, per-container resource allocations: device IDs with
   `Topology.Nodes` (NUMA affinity), `cpu_ids`, and memory blocks with their complete NUMA affinity.
3. Joins podResources results with a synchronized, node-scoped pod informer cache used for QoS,
   resource requests, container status, and init-container lifecycle validation.
4. Maps each allocation to a NUMA node:
   - **Devices** (GPU/NIC): the NUMA node is in the podResources `Topology` field directly.
   - **CPUs**: map `cpu_ids` → NUMA node using the node's CPU topology (from `/sys` or the NRT
     zones already on the node).
   - **Memory**: the podResources memory blocks carry every node in their affinity.
5. Writes the existing per-zone placement annotation and a separate pod-level memory-group
   annotation, patching each only when its canonical value changes.

Memory Manager blocks are reported by the same podResources response. NPE retains the containing
container's identity while validating completeness, then aggregates blocks by mask and memory type
for publication. The wire format and completeness rules are specified below; node-wide admission
and ownership accounting belong to the scheduler, not the exporter.

### Published Format

The existing per-zone annotation remains unchanged:

```yaml
kai.scheduler/numa-placement-observed: |-
  [{"zone":"node-0","amount":{"nvidia.com/gpu":"2","cpu":"8"}}]
```

Memory Manager groups use a second annotation:

```yaml
kai.scheduler/numa-memory-groups-observed: |-
  [
    {
      "memoryNodes": ["node-0", "node-1"],
      "amount": {"memory": "17179869184"}
    }
  ]
```

Blocks from several containers with the same mask are summed by memory resource. The pod UID is the
implicit owner. The annotation is tri-state: a list is complete, including `[]` for no groups; JSON
`null` explicitly marks incomplete or invalid state and overrides prediction; absence means NPE has
not produced a decision, so a group-aware scheduler prediction may bridge initial startup.
`memoryNodes` contains NRT zone names; `amount` is a Kubernetes ResourceList keyed by `memory`
or a hugepage resource such as `hugepages-2Mi`. Zero-sized blocks still retain group affinity.
Pods outside Guaranteed QoS always publish `[]`, since static Memory Manager does not allocate
blocks for them.

### Lifecycle and Freshness

Exclusive CPU and device assignments are normally stable, but the represented container set and
Memory Manager blocks can change while init containers complete and application containers start.
The exporter patches either annotation whenever its canonical value changes.

There is a small initial lag between a container starting and its complete observation appearing.
During that window, an absent observation may use a complete prediction. An explicitly incomplete
observation tells a group-aware scheduler that node ownership is unknown, or transitioning during ordinary
init. An incomplete observation never partially replaces predicted memory ownership.

On every reconciliation, every expected application container and restartable init container
requesting managed memory must have started and reported its blocks. A missing container or empty
`memory` list is incomplete, even after an earlier complete observation.

If completeness is lost after an earlier successful observation, or NPE detects an ordinary-init
transition, it writes `null` instead of retaining stale groups or exposing an older prediction.
The annotation value is the literal string `"null"`; patching JSON null into metadata would delete
the key and enable prediction fallback. Completeness checks require a synchronized node-scoped pod
informer, and reconciliation considers API pods even when podResources omits them. NPE validates
pod-local blocks; follow-up scheduler support validates node-wide group overlap and aggregate capacity.

### Reconciliation and annotation drift

Polling and node-scoped pod informer events use the same reconciliation path. Each pass computes
placement from podResources and compares it with the pod's annotations in the informer cache.
Only missing or different values are patched, including annotations removed or modified externally.
There is no separate API-server drift pass or last-written placement cache.

`--drift-resync-interval` is a deprecated no-op retained for command-line compatibility.
Polling and pod events repair annotation drift.
Once the informer reflects the desired annotations, reconciliation
performs no pod patches.

### RBAC and security

- **Exporter → kubelet podResources:** read-only access to the podResources socket via a hostPath
  mount. This is the same surface NRT exporters use.
- **Exporter → API server:** permission to `get`, `list`, `watch`, and `patch` pods. NPE limits
  writes to its owned annotations and uses a `spec.nodeName` field selector in code. Standard RBAC
  does not enforce annotation-key or field-selector boundaries.
- **Scheduler:** no new permissions — it already lists/watches pods.

## Planned Scheduler Integration

The following consumption rules are implemented by the separate scheduler change, not by NPE.

- **NRT** remains the source for per-zone allocatable capacity and Topology Manager policy/scope.
- **This exporter** supplies observed per-pod CPU and device quantities and per-container Memory
  Manager blocks aggregated into pod-level group masks and reserved amounts.
- **Scheduler scope:** the NUMA plugin models `single-numa-node`, `restricted`, and `best-effort`.
  Policy `none` bypasses plugin evaluation and accounting even when the exporter reports groups;
  kubelet remains responsible for admission on those nodes.
- **Memory Manager group availability** is reconstructed as the sum of NRT `Allocatable` across the
  mask minus the amounts reported by every pod owner. No per-zone byte split is needed.
- **Capacity source:** the scheduler continues trusting NRT `Allocatable`, as it does today. Matching
  Memory Manager's internal allocatable capacity remains a deployment assumption.
- A complete observed list replaces predicted memory ownership. An absent observed annotation uses
  a complete prediction when present. Explicit `null`, malformed, or invalid observed state forces
  the node's group view unknown or transitioning as applicable.

## Limitations and Caveats

- **Initial-observation lag:** a just-started pod may be unannotated or incomplete until the
  exporter observes all expected containers. An absent annotation may use a complete prediction;
  explicit incomplete state tells a group-aware scheduler to block managed-memory allocations until
  complete ownership
  is available, with a distinct reason during an ordinary-init transition.
- **Exporter must be deployed on every relevant node**, or coverage is partial (mixed
  observed/predicted zone placement and unknown Memory Manager state on uncovered nodes). For a
  group-aware scheduler, unknown ownership blocks new managed-memory allocations on
  uncovered nodes rather than silently bypassing group checks.
- **Annotation write load:** bounded by writing only when the canonical snapshot changes. A pod may
  have several updates as init and application containers transition.
- **Ordinary init containers:** podResources omits non-restartable init containers. A group-aware scheduler
  tracks transition-owner pod UIDs and blocks further managed-memory allocations until a complete
  application-container observation clears them. Rollback and task removal reverse ownership too.
- **Observation freshness:** the group annotation has no timestamp. If NPE stops reconciling, a
  stale annotation remains usable until another component detects exporter liveness; kubelet
  admission remains the final backstop.

## Superseded long-term by DRA

Under **Dynamic Resource Allocation** ([KEP-3063][kep3063], GA-track) the scheduler itself
allocates devices and records them in `ResourceClaim` status, and DRA drivers can expose NUMA
node as a device attribute — so the scheduler knows real placement with no scraping. This exporter
is therefore a stopgap for the legacy device-plugin + Topology Manager world (and for CPU/memory
NUMA, which DRA does not yet manage — [KEP-3695][kep3695] tracks bridging podResources/DRA). As
workloads move to DRA, the need for it fades.

The topology-aware WG is building per-container NUMA-placement feedback from the same podResources
data: the [`numaplacement`][numaplacement] encoding and resource-topology-exporter PRs
([#390][rte390]/#396) derive each container's actual NUMA affinity and publish it, but as NRT CRD
node-level attributes alongside the fingerprint rather than as pod annotations. If the community
adopts this capability, it could replace this implementation.

## Future Work

- Consume the upstream `numaplacement` NRT attribute (RTE) instead of self-published pod
  annotations, once available.
- Revisit/retire as workloads migrate to DRA.

[numaplacement]: https://github.com/k8stopologyawareschedwg/numaplacement
[rte390]: https://github.com/k8stopologyawareschedwg/resource-topology-exporter/pull/390
[kep3063]: https://github.com/kubernetes/enhancements/tree/master/keps/sig-node/3063-dynamic-resource-allocation
[kep3695]: https://github.com/kubernetes/enhancements/issues/3695
