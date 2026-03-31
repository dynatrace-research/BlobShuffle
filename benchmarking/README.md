# BlobShuffle Benchmarking

This directory contains everything needed to benchmark BlobShuffle on Kubernetes with AWS as the target cloud environment. It covers:

- **Infrastructure**: Amazon EKS cluster definition and AWS-specific storage configuration
- **Manifests**: Kubernetes manifests for manual deployment of BlobShuffle and the load generator
- **Helm values**: Configuration for the [Theodolite](https://www.theodolite.rocks/) framework
- **Theodolite definitions**: Benchmark CRD and ready-to-use execution files for the experiments described in the BlobShuffle paper

> **Before you start**: Update all image references in the manifests to point to the container registry where you published your BlobShuffle image (search for `<YOUR_REGISTRY>`). Similarly replace `<YOUR_S3_BUCKET>` and `<YOUR_CLUSTER_NAME>` with your own values.

---

## Directory Layout

```
benchmarking/
├── README.md
├── infrastructure/
│   ├── aws/
│   │   ├── cluster.yaml                  # EKS cluster definition (eksctl)
│   │   └── kafka-storage-class.yaml      # AWS EBS storage class for Kafka
│   └── scripts/
│       ├── label-nodegroup-sizes.sh      # Labels SUT nodes for topology-aware scheduling
│       └── show-pods-by-zone.sh          # Debugging: show BlobShuffle pods grouped by zone
├── manifests/
│   ├── s3shuffler/                       # BlobShuffle system-under-test manifests
│   └── load-generator/                   # Load generator manifests
├── helm-values/
│   ├── values.yaml                       # Base Theodolite Helm values (Kafka, Prometheus)
│   ├── values-nodegroups.yaml            # Node affinity / topology scheduling
│   └── values-aws-kafka-storage.yaml     # Persistent EBS volumes for Kafka
└── theodolite/
    ├── benchmark.yaml                    # Theodolite Benchmark CRD
    └── executions/
        ├── scalability/                  # Scale from 6 to 48 BlobShuffle instances
        ├── batch-size/                   # Vary batch size from 1 MB to 128 MB
        ├── partitioner/                  # Compare partitioning strategies
        └── partitions/                   # Vary partitions-per-instance ratio
```

---

## Prerequisites

| Tool | Purpose |
|------|---------|
| [`eksctl`](https://eksctl.io/) | Create and manage the EKS cluster |
| [`kubectl`](https://kubernetes.io/docs/tasks/tools/) | Interact with the cluster |
| [`helm`](https://helm.sh/docs/intro/install/) | Install Theodolite |
| [AWS CLI](https://aws.amazon.com/cli/) | Authenticate and configure AWS resources |

You also need:
- An AWS account with permissions to create EKS clusters, EC2 instances, EBS volumes, IAM roles, and S3 buckets
- An S3 bucket for BlobShuffle to store batched objects
- An IAM policy granting `s3:GetObject` and `s3:PutObject` on that bucket
- A container registry (e.g., Amazon ECR) with your built BlobShuffle image

---

## 1 — Set Up the Kubernetes Cluster (AWS EKS)

### 1.1 Create the EKS cluster

Edit `infrastructure/aws/cluster.yaml` and fill in your values for `<YOUR_CLUSTER_NAME>`, `<AWS_ACCOUNT_ID>`, `<YOUR_S3_POLICY_NAME>`, etc. Then create the cluster:

```sh
eksctl create cluster -f infrastructure/aws/cluster.yaml
```

> **Cost warning**: The full cluster configuration (infra + 3 × 8 SUT nodes + 3 × 3 Kafka nodes) is large and expensive. Downscale `desiredCapacity` values if running smaller experiments.

### 1.2 Install the Kubernetes Metrics Server

EKS does not install the metrics server by default, which is needed for `kubectl top`:

```sh
kubectl delete -f https://github.com/kubernetes-sigs/metrics-server/releases/latest/download/components.yaml
kubectl apply -f https://github.com/kubernetes-sigs/metrics-server/releases/latest/download/components.yaml
```

### 1.3 Create the Kafka storage class

Theodolite's Kafka deployment uses persistent EBS volumes. Create the required storage class:

```sh
kubectl apply -f infrastructure/aws/kafka-storage-class.yaml
```

### 1.4 Label SUT nodes for topology-aware scheduling

BlobShuffle spreads pods across three simulated availability zones (labels `zone-1`, `zone-2`, `zone-3`). The `label-nodegroup-sizes.sh` script adds `nodegroup-size-N=true` labels that Theodolite uses to constrain pods to a specific number of nodes per zone.

Run it with the number of nodes per SUT nodegroup you want to use (up to `desiredCapacity`):

```sh
./infrastructure/scripts/label-nodegroup-sizes.sh <N>
# Example: 8 nodes per zone → N=8
./infrastructure/scripts/label-nodegroup-sizes.sh 8
```

This will label each of the first N nodes in each zone with labels like `nodegroup-size-3=true`, `nodegroup-size-6=true`, …, `nodegroup-size-N*3=true`.

---

## 2 — Manual Deployment

Use this approach to run BlobShuffle with a specific configuration without Theodolite automation.

### 2.1 Install Theodolite (for Kafka and Prometheus)

Even for manual benchmarks, Theodolite is needed to deploy Kafka and Prometheus:

```sh
helm repo add theodolite https://www.theodolite.rocks
helm repo update
helm install theodolite theodolite/theodolite \
  -f https://raw.githubusercontent.com/cau-se/theodolite/main/helm/preconfigs/extended-metrics.yaml \
  -f helm-values/values.yaml \
  -f helm-values/values-nodegroups.yaml \
  -f helm-values/values-aws-kafka-storage.yaml
```

Wait for the Kafka cluster to be ready (this can take a few minutes):

```sh
kubectl wait kafka/theodolite-kafka --for=condition=Ready --timeout=5m
```

### 2.2 Apply RBAC for BlobShuffle

BlobShuffle pods need to read node labels to determine their zone. Apply the required RBAC:

```sh
kubectl apply -f manifests/s3shuffler/s3shuffler-rbac.yaml
```

### 2.3 Deploy BlobShuffle

Before applying, review and adjust the following in `manifests/s3shuffler/s3shuffler-deployment.yaml`:
- `image`: your container registry and image tag
- `S3_BUCKET`: your S3 bucket name
- `replicas`: desired number of BlobShuffle instances
- `BATCH_DESIRED_BYTES`, `BATCH_MAX_MS`, `PARTITIONER`, cache settings as needed
- `nodeSelector.nodegroup-size-N`: optionally pin to a specific node count
- `partitions` in `input-topic.yaml` and `output-topic.yaml`: for best performance set this to `replicas × k` where k is a multiple of 3 (e.g. 9 is a good default)

```sh
kubectl apply -f manifests/s3shuffler/
```

Wait for pods to become ready:

```sh
kubectl rollout status deployment/s3shuffler
```

### 2.4 Deploy the load generator

```sh
kubectl apply -f manifests/load-generator/
```

### 2.5 Monitor

Check pod placement across zones:

```sh
./infrastructure/scripts/show-pods-by-zone.sh
```

Check consumer lag:

```sh
kubectl exec -it deploy/kcat -- kcat -b theodolite-kafka-kafka-bootstrap:9092 -L
```

Check throughput via Prometheus (port-forward to Prometheus):

```sh
kubectl port-forward svc/prometheus-operated 9090:9090
# Then open http://localhost:9090 and query:
# sum(rate(kafka_server_brokertopicmetrics_messagesin_total{topic="output"}[20s]))
```

**Documentation on Grafana dashboard access and OTel/Dynatrace integration for advanced monitoring will follow soon.**

### 2.6 Tear down

```sh
kubectl delete -f manifests/load-generator/
# Wait for load generator to stop (~30s)
kubectl delete -f manifests/s3shuffler/
```

---

## 3 — Automated Benchmarks with Theodolite

[Theodolite](https://www.theodolite.rocks/) automates the benchmarking workflow: it deploys the system under test and load generator, runs experiments across a range of configurations, collects metrics from Prometheus, and evaluates SLOs.

### 3.1 Install Theodolite

Same as step 2.1 above — if you already installed it for manual deployment, skip this step.

```sh
helm repo add theodolite https://www.theodolite.rocks
helm repo update
helm install theodolite theodolite/theodolite \
  -f https://raw.githubusercontent.com/cau-se/theodolite/main/helm/preconfigs/extended-metrics.yaml \
  -f helm-values/values.yaml \
  -f helm-values/values-nodegroups.yaml \
  -f helm-values/values-aws-kafka-storage.yaml
```

Wait for Kafka to be ready:

```sh
kubectl wait kafka/theodolite-kafka --for=condition=Ready --timeout=5m
```

### 3.2 Apply RBAC

```sh
kubectl apply -f manifests/s3shuffler/s3shuffler-rbac.yaml
```

### 3.3 Install the benchmark definition

Theodolite reads the SUT and load-generator manifests from Kubernetes ConfigMaps. Create them from the `manifests/` directories and apply the benchmark CRD:

```sh
# Re-create ConfigMaps (run this whenever manifests change)
kubectl delete configmaps --ignore-not-found=true \
  blobshuffle-resources-load-generator \
  blobshuffle-resources

kubectl create configmap blobshuffle-resources-load-generator \
  --from-file manifests/load-generator/

kubectl create configmap blobshuffle-resources \
  --from-file manifests/s3shuffler/

kubectl apply -f theodolite/benchmark.yaml
```

Verify the benchmark was accepted:

```sh
kubectl get benchmark s3shuffler
```

### 3.4 Run benchmark executions

Each file in `theodolite/executions/` is a Theodolite `Execution` resource that runs one experiment. Apply an execution to start it:

```sh
# Example: scalability experiment with 12 instances on 6-node cluster
kubectl apply -f theodolite/executions/scalability/s3shuffler-nodes-6-instances-12.yaml
```

Watch progress:

```sh
kubectl get execution -w
kubectl logs -l app=theodolite -f
```

#### Available experiment series

| Series | Directory | Description |
|--------|-----------|-------------|
| **Scalability** | `executions/scalability/` | Fixed 16 MB batch size; scale from 6 to 48 instances (3–24 nodes/zone) |
| **Batch size** | `executions/batch-size/` | Fixed instance count; sweep batch sizes from 1 MB to 128 MB |
| **Partitioner** | `executions/partitioner/` | Fixed settings; compare `KAFKA_BUILTIN`, `KAFKA_OPTIMIZED`, `KOMIHASH4_3`, `KAFKA_4_2`, `RANDOM` |
| **Partitions** | `executions/partitions/` | Fixed 24 instances; vary partitions-per-instance ratio from ×3 to ×30 |

#### Running a full series

```sh
# Run all scalability experiments sequentially
kubectl apply -f theodolite/executions/scalability/

# Run all batch-size experiments
kubectl apply -f theodolite/executions/batch-size/
```

Theodolite automatically queues them and ensures only one runs at a time.

### 3.5 Retrieve results

Theodolite stores results in a persistent volume inside the operator pod. Copy them out:

```sh
kubectl cp \
  $(kubectl get pod -l app=theodolite -o jsonpath="{.items[0].metadata.name}"):results \
  ./results \
  -c results-access
```

Results are organized by execution name and contain per-step metric data in CSV format.

**An evaluation notebook for analyzing these results will follow soon.**

---

## 5 — Cleanup

### Uninstall Theodolite

```sh
helm uninstall theodolite
```

### Delete the EKS cluster

```sh
# The --disable-nodegroup-eviction flag is required to avoid hangs on node groups
# with PodDisruptionBudgets
eksctl delete cluster -f infrastructure/aws/cluster.yaml --disable-nodegroup-eviction
```

### Clean up leftover EBS volumes

Theodolite's Kafka deployment creates EBS volumes that are sometimes not fully cleaned up. List any remaining volumes and delete them manually if needed:

```sh
# List leftover EBS volumes belonging to the cluster
aws ec2 describe-volumes \
  --filters "Name=tag:kubernetes.io/cluster/<YOUR_CLUSTER_NAME>,Values=owned" \
  --query "Volumes[*].{ID:VolumeId,Size:Size,State:State,Zone:AvailabilityZone,PVC:Tags[?Key=='kubernetes.io/created-for/pvc/name'].Value|[0]}" \
  --output table

# Delete a specific volume
aws ec2 delete-volume --volume-id <VOLUME_ID>
```
