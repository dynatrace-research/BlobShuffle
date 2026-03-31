#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<EOF
Usage: $0 N

Label the first N nodes in each AZ (node label "zone") that have label "type=sut".

For each zone, take the first N nodes (lexicographic by node name). For node index p (1..N)
we add labels "nodegroup-size-M=true" for M = (k*3) where k runs from p..N (i.e. multiples of 3
from p*3 up to N*3). Example: p=1 -> 3,6,9,...; p=2 -> 6,9,...

Examples:
  $0 4

Requires: kubectl configured to talk to the cluster and permission to label nodes.
EOF
}

if [[ ${1:-} == "-h" || ${1:-} == "--help" ]]; then
  usage
  exit 0
fi

if [[ $# -ne 1 ]]; then
  usage
  exit 2
fi

N=$1
if ! [[ "$N" =~ ^[0-9]+$ ]] || [[ "$N" -lt 1 ]]; then
  echo "Error: N must be a positive integer." >&2
  exit 2
fi

echo "Labeling first ${N} nodes per AZ (label zone) among nodes with label type=sut"

# Gather nodes with type=sut and their zones
mapfile -t lines < <(kubectl get nodes -l type=sut -o custom-columns=NAME:.metadata.name,ZONE:.metadata.labels.zone --no-headers 2>/dev/null)
if [[ ${#lines[@]} -eq 0 ]]; then
  echo "No nodes found with label type=sut" >&2
  exit 0
fi

declare -A zone_nodes

for line in "${lines[@]}"; do
  name=$(echo "$line" | awk '{print $1}')
  zone=$(echo "$line" | awk '{print $2}')
  if [[ -z "$zone" || "$zone" == "<none>" ]]; then
    echo "Skipping node $name with empty zone" >&2
    continue
  fi
  # build a space-separated list of nodes per zone
  if [[ -z "${zone_nodes[$zone]:-}" ]]; then
    zone_nodes[$zone]="$name"
  else
    zone_nodes[$zone]="${zone_nodes[$zone]} $name"
  fi
done

for zone in "${!zone_nodes[@]}"; do
  echo "Processing zone: $zone"
  # split into array and sort node names to have deterministic order
  IFS=' ' read -r -a arr <<< "${zone_nodes[$zone]}"
  IFS=$'\n' sorted=($(printf "%s\n" "${arr[@]}" | sort))
  unset IFS

  count=${#sorted[@]}
  if [[ $count -eq 0 ]]; then
    echo "  no nodes in zone $zone (skipping)"
    continue
  fi

  max_index=$(( N < count ? N : count ))
  for ((i=0;i<max_index;i++)); do
    node=${sorted[i]}
    p=$((i+1))
    echo "  node #$p: $node"
    for ((k=p;k<=N;k++)); do
      m=$((k*3))
      label_key="nodegroup-size-$m"
      echo "    adding label: $label_key=true"
      kubectl label node "$node" "$label_key=true" --overwrite || {
        echo "      failed to label $node with $label_key" >&2
      }
    done
  done
done

echo "Done."
