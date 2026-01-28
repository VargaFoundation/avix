use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Job {
    pub api_version: String,
    pub kind: String,
    pub metadata: Metadata,
    pub spec: JobSpec,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Metadata {
    pub name: String,
    pub namespace: Option<String>,
    pub labels: Option<HashMap<String, String>>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct JobSpec {
    pub backend: Option<String>,
    pub queue: Option<String>,
    pub priority: Option<i32>,
    pub resources: Option<Resources>,
    pub affinity: Option<Affinity>,
    pub tolerations: Option<Vec<Toleration>>,
    #[serde(rename = "type")]
    pub job_type: Option<String>,
    pub execution: Execution,
    pub ml_tracking: Option<MlTracking>,
    pub hyperparams: Option<Hyperparams>,
    pub dependencies: Option<Vec<Dependency>>,
    pub scaling: Option<Scaling>,
    pub cost_budget: Option<f64>,
    pub restart_policy: Option<String>,
    pub ttl_seconds_after_finished: Option<i64>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Resources {
    pub cpu: Option<String>,
    pub memory: Option<String>,
    pub gpu: Option<u32>,
    pub disk: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Execution {
    pub image: String,
    pub command: Vec<String>,
    pub args: Option<Vec<String>>,
    pub env: Option<Vec<EnvVar>>,
    pub volumes: Option<Vec<Volume>>,
    pub requirements: Option<Vec<String>>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct EnvVar {
    pub name: String,
    pub value: Option<String>,
    pub value_from: Option<ValueFrom>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ValueFrom {
    pub secret_ref: SecretRef,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct SecretRef {
    pub name: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct MlTracking {
    pub wandb: Option<bool>,
    pub mlflow: Option<MlflowConfig>,
    pub tensorboard: Option<bool>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct MlflowConfig {
    pub tracking_uri: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Hyperparams {
    pub grid: Option<HashMap<String, Vec<serde_json::Value>>>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Affinity {
    pub node_labels: Option<HashMap<String, String>>,
    pub anti_affinity: Option<AntiAffinity>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct AntiAffinity {
    pub job_labels: Option<HashMap<String, String>>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Toleration {
    pub key: String,
    pub value: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Dependency {
    pub job: String,
    pub on: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Scaling {
    pub min_instances: Option<u32>,
    pub max_instances: Option<u32>,
    pub metric: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Volume {
    pub name: String,
    pub s3: Option<S3Volume>,
    pub gcs: Option<GcsVolume>,
    pub azure_blob: Option<AzureBlobVolume>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct S3Volume {
    pub bucket: String,
    pub path: Option<String>,
    pub mount_path: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct GcsVolume {
    pub bucket: String,
    pub path: Option<String>,
    pub mount_path: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct AzureBlobVolume {
    pub container: String,
    pub path: Option<String>,
    pub mount_path: String,
}

/// Queue configuration for job scheduling
#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct QueueConfig {
    pub name: String,
    pub priority: Option<i32>,
    pub max_resources: Option<Resources>,
    pub scheduler: Option<String>,
    pub preemption: Option<bool>,
    pub burst_to: Option<String>,
}

pub mod v1 {
    tonic::include_proto!("avix.v1");
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_job_deserialization_minimal() {
        let yaml = r#"
apiVersion: avix.vargafoundation.org/v1alpha1
kind: Job
metadata:
  name: minimal-job
spec:
  execution:
    image: alpine
    command: ["echo", "hello"]
"#;
        let job: Job = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(job.api_version, "avix.vargafoundation.org/v1alpha1");
        assert_eq!(job.kind, "Job");
        assert_eq!(job.metadata.name, "minimal-job");
        assert!(job.metadata.namespace.is_none());
        assert_eq!(job.spec.execution.image, "alpine");
        assert_eq!(job.spec.execution.command, vec!["echo", "hello"]);
    }

    #[test]
    fn test_job_deserialization_full() {
        let yaml = r#"
apiVersion: avix.vargafoundation.org/v1alpha1
kind: Job
metadata:
  name: full-job
  namespace: ml-team
  labels:
    team: ml
    env: prod
spec:
  backend: local-docker
  queue: ml-batch
  priority: 100
  resources:
    cpu: "4"
    memory: "16Gi"
    gpu: 2
    disk: "100Gi"
  execution:
    image: pytorch/pytorch:latest
    command: ["python", "train.py"]
    args: ["--epochs", "10"]
  mlTracking:
    wandb: true
    tensorboard: true
  hyperparams:
    grid:
      learning_rate: [0.01, 0.001]
"#;
        let job: Job = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(job.metadata.namespace, Some("ml-team".to_string()));
        assert_eq!(job.spec.backend, Some("local-docker".to_string()));
        assert_eq!(job.spec.queue, Some("ml-batch".to_string()));
        assert_eq!(job.spec.priority, Some(100));
        
        let resources = job.spec.resources.unwrap();
        assert_eq!(resources.cpu, Some("4".to_string()));
        assert_eq!(resources.memory, Some("16Gi".to_string()));
        assert_eq!(resources.gpu, Some(2));
        assert_eq!(resources.disk, Some("100Gi".to_string()));
        
        let ml_tracking = job.spec.ml_tracking.unwrap();
        assert_eq!(ml_tracking.wandb, Some(true));
        assert_eq!(ml_tracking.tensorboard, Some(true));
    }

    #[test]
    fn test_job_serialization() {
        let job = Job {
            api_version: "avix.vargafoundation.org/v1alpha1".to_string(),
            kind: "Job".to_string(),
            metadata: Metadata {
                name: "test-job".to_string(),
                namespace: Some("default".to_string()),
                labels: None,
            },
            spec: JobSpec {
                backend: Some("local-docker".to_string()),
                queue: None,
                priority: Some(50),
                resources: Some(Resources {
                    cpu: Some("2".to_string()),
                    memory: Some("4Gi".to_string()),
                    gpu: None,
                    disk: None,
                }),
                execution: Execution {
                    image: "python:3.11".to_string(),
                    command: vec!["python".to_string(), "script.py".to_string()],
                    args: Some(vec!["--verbose".to_string()]),
                    env: None,
                },
                ml_tracking: None,
                hyperparams: None,
            },
        };

        let yaml = serde_yaml::to_string(&job).unwrap();
        assert!(yaml.contains("name: test-job"));
        assert!(yaml.contains("namespace: default"));
        assert!(yaml.contains("backend: local-docker"));
        assert!(yaml.contains("cpu: \"2\""));
        assert!(yaml.contains("image: \"python:3.11\""));
    }

    #[test]
    fn test_job_with_env_vars() {
        let yaml = r#"
apiVersion: avix.vargafoundation.org/v1alpha1
kind: Job
metadata:
  name: env-job
spec:
  execution:
    image: alpine
    command: ["env"]
    env:
      - name: MY_VAR
        value: my_value
      - name: SECRET_VAR
        valueFrom:
          secretRef:
            name: my-secret
"#;
        let job: Job = serde_yaml::from_str(yaml).unwrap();
        let env = job.spec.execution.env.unwrap();
        assert_eq!(env.len(), 2);
        assert_eq!(env[0].name, "MY_VAR");
        assert_eq!(env[0].value, Some("my_value".to_string()));
        assert_eq!(env[1].name, "SECRET_VAR");
        assert!(env[1].value_from.is_some());
        assert_eq!(env[1].value_from.as_ref().unwrap().secret_ref.name, "my-secret");
    }

    #[test]
    fn test_job_with_labels() {
        let mut labels = HashMap::new();
        labels.insert("team".to_string(), "data".to_string());
        labels.insert("env".to_string(), "staging".to_string());

        let metadata = Metadata {
            name: "labeled-job".to_string(),
            namespace: None,
            labels: Some(labels),
        };

        assert_eq!(metadata.labels.as_ref().unwrap().get("team"), Some(&"data".to_string()));
        assert_eq!(metadata.labels.as_ref().unwrap().get("env"), Some(&"staging".to_string()));
    }

    #[test]
    fn test_mlflow_config() {
        let yaml = r#"
apiVersion: avix.vargafoundation.org/v1alpha1
kind: Job
metadata:
  name: mlflow-job
spec:
  execution:
    image: alpine
    command: ["echo"]
  mlTracking:
    mlflow:
      trackingUri: http://mlflow:5000
"#;
        let job: Job = serde_yaml::from_str(yaml).unwrap();
        let ml_tracking = job.spec.ml_tracking.unwrap();
        let mlflow = ml_tracking.mlflow.unwrap();
        assert_eq!(mlflow.tracking_uri, "http://mlflow:5000");
    }

    #[test]
    fn test_hyperparams_grid() {
        let yaml = r#"
apiVersion: avix.vargafoundation.org/v1alpha1
kind: Job
metadata:
  name: grid-job
spec:
  execution:
    image: alpine
    command: ["train"]
  hyperparams:
    grid:
      lr: [0.01, 0.001]
      batch: [32, 64]
"#;
        let job: Job = serde_yaml::from_str(yaml).unwrap();
        let hyperparams = job.spec.hyperparams.unwrap();
        let grid = hyperparams.grid.unwrap();
        assert!(grid.contains_key("lr"));
        assert!(grid.contains_key("batch"));
        assert_eq!(grid.get("lr").unwrap().len(), 2);
    }

    #[test]
    fn test_job_clone() {
        let job = Job {
            api_version: "v1".to_string(),
            kind: "Job".to_string(),
            metadata: Metadata {
                name: "clone-test".to_string(),
                namespace: None,
                labels: None,
            },
            spec: JobSpec {
                backend: None,
                queue: None,
                priority: None,
                resources: None,
                execution: Execution {
                    image: "alpine".to_string(),
                    command: vec!["echo".to_string()],
                    args: None,
                    env: None,
                },
                ml_tracking: None,
                hyperparams: None,
            },
        };

        let cloned = job.clone();
        assert_eq!(cloned.metadata.name, job.metadata.name);
        assert_eq!(cloned.spec.execution.image, job.spec.execution.image);
    }

    #[test]
    fn test_resources_optional_fields() {
        let resources = Resources {
            cpu: Some("1".to_string()),
            memory: None,
            gpu: None,
            disk: None,
        };
        assert!(resources.cpu.is_some());
        assert!(resources.memory.is_none());
        assert!(resources.gpu.is_none());
    }
}
