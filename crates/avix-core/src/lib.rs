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

        let config = Config {
            image: Some(job.spec.execution.image.clone()),
            cmd: Some(job.spec.execution.command.clone()),
            labels: Some(labels),
            ..Default::default()
        };

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
}

pub mod server;
