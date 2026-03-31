declare -A node_to_zone
declare -A zone_nodes
declare -A node_pods
#!/bin/bash

set -euo pipefail

# Show s3shuffler pods grouped by node and zone (uses a constant number of kubectl calls)

# One kubectl call to list pods+node, and one to list nodes+zone labels
pods_data=$(kubectl get pods -l app=s3shuffler -o jsonpath='{range .items[*]}{.metadata.name}{"\t"}{.spec.nodeName}{"\n"}{end}')
nodes_data=$(kubectl get nodes -o jsonpath='{range .items[*]}{.metadata.name}{"\t"}{.metadata.labels.zone}{"\n"}{end}')

declare -A node_zone
declare -A node_pods
declare -A zone_nodes

# Parse nodes_data to build node -> zone map
while IFS=$'\t' read -r node zone; do
    if [[ -z "$node" ]]; then
        continue
    fi
    if [[ -z "$zone" ]]; then
        zone="no-zone"
    fi
    node_zone[$node]="$zone"
done <<< "$nodes_data"

# Parse pods_data to collect pods per node
while IFS=$'\t' read -r pod node; do
    if [[ -z "$pod" || -z "$node" || "$node" == "<none>" ]]; then
        continue
    fi
    if [[ -z "${node_pods[$node]:-}" ]]; then
        node_pods[$node]="$pod"
    else
        node_pods[$node]="${node_pods[$node]}, $pod"
    fi
    # ensure node has a zone entry
    if [[ -z "${node_zone[$node]:-}" ]]; then
        node_zone[$node]="no-zone"
    fi
done <<< "$pods_data"

# Group nodes by zone, but only include nodes that have at least one pod
for node in "${!node_pods[@]}"; do
    zone=${node_zone[$node]:-no-zone}
    if [[ -z "${zone_nodes[$zone]:-}" ]]; then
        zone_nodes[$zone]="$node"
    else
        zone_nodes[$zone]="${zone_nodes[$zone]} $node"
    fi
done

# Output grouped by zone (sorted)
for zone in $(printf "%s\n" "${!zone_nodes[@]}" | sort); do
    echo "$zone:"
    for node in $(printf "%s\n" ${zone_nodes[$zone]} | sort); do
        echo "  $node: ${node_pods[$node]:-}"
    done
    echo
done
