# Avix

Avix is a batch and streaming job scheduler designed to replace Apache Hadoop YARN for use cases like batch inference, simulations, ML parameter grid search, and support for ephemeral jobs (simple or distributed) or continuous jobs (streaming).

It aims to be an ultra-simple and powerful solution for data practitioners (data scientists, data engineers, ML engineers) by abstracting the complexities of execution backends.

## Features

- **Extreme Simplicity**: A unique YAML specification for all jobs, independent of the backend.
- **Data Practitioner Focused**: 
    - Native integration with ML tools (MLflow, WandB, TensorBoard).
    - Integrated hyperparameter tuning.
    - Transparent dataset management (S3, GCS, Azure Blob).
    - Secret and environment variable management.
    - Data pipelines support.
    - Auto-scaling based on metrics.
    - Jupyter integration.
- **Intelligent Queue Management**: Priority-based queuing, fair-sharing, preemption, and cloud bursting.
- **Real-time Monitoring**: Real-time logs and metrics via gRPC/WebSocket.

## Architecture

Avix is composed of:
- `avix-cli`: A Rust-based CLI for managing jobs.
- `avix-spec`: API specifications (REST and gRPC) and data models.
- `avix-core`: The core logic for job scheduling and backend abstraction.

## Getting Started

### Installation

```bash
# Recommended: install an up-to-date Rust toolchain via rustup.
# (Some distro packages ship older Cargo versions that can fail to build recent
# transitive dependencies.)
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

# Avix pins a compatible toolchain in `rust-toolchain.toml`.
# With rustup installed, entering the repo will prompt you to install it (or you
# can install it explicitly):
rustup toolchain install 1.85.0
```

To install the Avix CLI:

```bash
cargo install --locked --path crates/avix-cli
```

### Running locally

1. Start the Avix server:
   ```bash
   avix server
   ```

2. Submit a job (in another terminal):
   - Hello world (YAML):
     ```bash
     avix job submit examples/hello-world.yaml
     ```
   - Python script (uses `--from-py`, runs the script content in the container):
     ```bash
     avix job submit examples/train.py --from-py --backend local-docker
     ```
   - Workflow (sequence of jobs):
     ```bash
     avix job submit examples/workflow-hello-train.yaml
     ```

3. List jobs:
   ```bash
   avix job list
   ```

4. Watch logs:
   ```bash
   avix job logs <job_id> --follow
   ```

5. View metrics:
   ```bash
   avix metrics <job_id>
   ```

### Running with Docker

Avix supports a local Docker backend for single-node execution.

```bash
avix job submit my-job.yaml --backend local-docker
```

## Job Specification

Jobs are defined using a unified YAML format. See `examples/` for more details.

```yaml
apiVersion: avix.vargafoundation.org/v1alpha1
kind: Job
metadata:
  name: my-ml-inference
spec:
  backend: local-docker
  execution:
    image: myregistry/inference:latest
    command: ["python", "inference.py"]
```

## Workflow Specification

A workflow is a simple ordered list of jobs.

```yaml
apiVersion: avix.vargafoundation.org/v1alpha1
kind: Workflow
metadata:
  name: hello-then-train
spec:
  onFailure: stop
  jobs:
    - apiVersion: avix.vargafoundation.org/v1alpha1
      kind: Job
      metadata:
        name: hello-step
      spec:
        backend: local-docker
        execution:
          image: alpine
          command: ["echo", "Hello from Avix workflow!"]
    - apiVersion: avix.vargafoundation.org/v1alpha1
      kind: Job
      metadata:
        name: train-step
      spec:
        backend: local-docker
        execution:
          image: python:3.11-slim
          command: ["python", "-c", "print('hello')"]
```

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
