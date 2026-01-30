use async_trait::async_trait;
use avix_spec::Job;
use anyhow::Result;
use bollard::Docker;
use bollard::container::{Config, CreateContainerOptions, StartContainerOptions, ListContainersOptions, LogOutput};
use bollard::models::ContainerSummary;
use futures_util::StreamExt;
use std::collections::HashMap;

#[async_trait]
pub trait Backend {
    async fn submit(&self, job: Job) -> Result<String>;
    async fn stop(&self, id: &str) -> Result<()>;
    async fn list(&self, namespace: Option<&str>) -> Result<Vec<JobStatus>>;
    async fn logs(&self, id: &str, follow: bool) -> Result<tokio::sync::mpsc::Receiver<String>>;
    /// Waits for job completion and returns the exit code (0 = success).
    async fn wait(&self, id: &str) -> Result<i64>;
}

#[derive(Debug, Clone)]
pub struct JobStatus {
    pub id: String,
    pub name: String,
    pub status: String,
    pub created_at: i64,
}

pub struct DockerBackend {
    docker: Docker,
}

impl DockerBackend {
    pub fn new() -> Result<Self> {
        let docker = Docker::connect_with_local_defaults()?;
        Ok(Self { docker })
    }
}

#[async_trait]
impl Backend for DockerBackend {
    async fn submit(&self, job: Job) -> Result<String> {
        let mut labels = HashMap::new();
        labels.insert("managed-by".to_string(), "avix".to_string());
        labels.insert("avix-name".to_string(), job.metadata.name.clone());
        if let Some(ns) = &job.metadata.namespace {
            labels.insert("avix-namespace".to_string(), ns.clone());
        }

        let config = build_container_config(&job, labels);

        let options = Some(CreateContainerOptions {
            name: job.metadata.name.clone(),
            ..Default::default()
        });

        let container = self.docker.create_container(options, config).await?;
        self.docker.start_container(&container.id, None::<StartContainerOptions<String>>).await?;

        Ok(container.id)
    }

    async fn stop(&self, id: &str) -> Result<()> {
        self.docker.stop_container(id, None).await?;
        Ok(())
    }

    async fn list(&self, namespace: Option<&str>) -> Result<Vec<JobStatus>> {
        let mut filters = HashMap::new();
        // Combine label filters in a single vec so we don't overwrite the previous one
        let mut label_filters = vec!["managed-by=avix".to_string()];
        if let Some(ns) = namespace {
            label_filters.push(format!("avix-namespace={}", ns));
        }
        filters.insert("label".to_string(), label_filters);

        let options = Some(ListContainersOptions {
            all: true,
            filters,
            ..Default::default()
        });

        let containers = self.docker.list_containers(options).await?;
        
        Ok(containers.into_iter().map(|c: ContainerSummary| {
            JobStatus {
                id: c.id.unwrap_or_default(),
                name: c.labels.and_then(|l| l.get("avix-name").cloned()).unwrap_or_else(|| "unknown".to_string()),
                status: c.state.unwrap_or_else(|| "unknown".to_string()),
                created_at: c.created.unwrap_or(0),
            }
        }).collect())
    }

    async fn logs(&self, id: &str, follow: bool) -> Result<tokio::sync::mpsc::Receiver<String>> {
        let options = Some(bollard::container::LogsOptions {
            follow,
            stdout: true,
            stderr: true,
            tail: "all".to_string(),
            ..Default::default()
        });

        let mut stream = self.docker.logs(id, options);
        let (tx, rx) = tokio::sync::mpsc::channel(100);

        tokio::spawn(async move {
            while let Some(log) = stream.next().await {
                match log {
                    Ok(output) => {
                        let text = match output {
                            LogOutput::StdOut { message } => String::from_utf8_lossy(&message).to_string(),
                            LogOutput::StdErr { message } => String::from_utf8_lossy(&message).to_string(),
                            LogOutput::Console { message } => String::from_utf8_lossy(&message).to_string(),
                            _ => continue,
                        };
                        if tx.send(text).await.is_err() {
                            break;
                        }
                    }
                    Err(_) => break,
                }
            }
        });

        Ok(rx)
    }

    async fn wait(&self, id: &str) -> Result<i64> {
        let mut stream = self.docker.wait_container(id, None::<bollard::container::WaitContainerOptions<String>>);
        if let Some(res) = stream.next().await {
            let res = res?;
            Ok(res.status_code)
        } else {
            Ok(-1)
        }
    }
}

fn build_container_config(job: &Job, labels: HashMap<String, String>) -> Config<String> {
    let mut cmd = job.spec.execution.command.clone();
    if let Some(args) = &job.spec.execution.args {
        cmd.extend(args.clone());
    }

    let env = job
        .spec
        .execution
        .env
        .as_ref()
        .map(|vars| {
            vars.iter()
                .filter_map(|v| v.value.as_ref().map(|val| format!("{}={}", v.name, val)))
                .collect::<Vec<_>>()
        })
        .filter(|v| !v.is_empty());

    Config {
        image: Some(job.spec.execution.image.clone()),
        cmd: Some(cmd),
        env,
        labels: Some(labels),
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use super::build_container_config;
    use avix_spec::{EnvVar, Execution, Job, JobSpec, Metadata, Resources};
    use std::collections::HashMap;

    #[test]
    fn test_build_container_config_cmd_includes_args() {
        let job = Job {
            api_version: "v1".to_string(),
            kind: "Job".to_string(),
            metadata: Metadata {
                name: "t".to_string(),
                namespace: None,
                labels: None,
            },
            spec: JobSpec {
                backend: None,
                queue: None,
                priority: None,
                resources: Some(Resources {
                    cpu: None,
                    memory: None,
                    gpu: None,
                    disk: None,
                }),
                affinity: None,
                tolerations: None,
                job_type: None,
                execution: Execution {
                    image: "alpine".to_string(),
                    command: vec!["echo".to_string()],
                    args: Some(vec!["hello".to_string(), "world".to_string()]),
                    env: None,
                    volumes: None,
                    requirements: None,
                },
                ml_tracking: None,
                hyperparams: None,
                dependencies: None,
                scaling: None,
                cost_budget: None,
                restart_policy: None,
                ttl_seconds_after_finished: None,
            },
        };

        let cfg = build_container_config(&job, HashMap::new());
        assert_eq!(
            cfg.cmd.unwrap(),
            vec!["echo".to_string(), "hello".to_string(), "world".to_string()]
        );
    }

    #[test]
    fn test_build_container_config_env_maps_key_values() {
        let job = Job {
            api_version: "v1".to_string(),
            kind: "Job".to_string(),
            metadata: Metadata {
                name: "t".to_string(),
                namespace: None,
                labels: None,
            },
            spec: JobSpec {
                backend: None,
                queue: None,
                priority: None,
                resources: Some(Resources {
                    cpu: None,
                    memory: None,
                    gpu: None,
                    disk: None,
                }),
                affinity: None,
                tolerations: None,
                job_type: None,
                execution: Execution {
                    image: "alpine".to_string(),
                    command: vec!["env".to_string()],
                    args: None,
                    env: Some(vec![
                        EnvVar {
                            name: "A".to_string(),
                            value: Some("1".to_string()),
                            value_from: None,
                        },
                        EnvVar {
                            name: "B".to_string(),
                            value: Some("two".to_string()),
                            value_from: None,
                        },
                        // secret-backed env var: ignored by docker backend for now
                        EnvVar {
                            name: "SECRET".to_string(),
                            value: None,
                            value_from: Some(avix_spec::ValueFrom {
                                secret_ref: avix_spec::SecretRef {
                                    name: "s".to_string(),
                                },
                            }),
                        },
                    ]),
                    volumes: None,
                    requirements: None,
                },
                ml_tracking: None,
                hyperparams: None,
                dependencies: None,
                scaling: None,
                cost_budget: None,
                restart_policy: None,
                ttl_seconds_after_finished: None,
            },
        };

        let cfg = build_container_config(&job, HashMap::new());
        let env = cfg.env.unwrap();

        assert!(env.contains(&"A=1".to_string()));
        assert!(env.contains(&"B=two".to_string()));
        assert!(!env.iter().any(|e| e.starts_with("SECRET=")));
    }
}

pub mod server;
