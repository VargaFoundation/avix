use tonic::{transport::Server, Request, Response, Status};
use avix_spec::v1::job_service_server::{JobService, JobServiceServer};
use avix_spec::v1::*;
use crate::{Backend, DockerBackend};
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};
use tokio_stream::wrappers::ReceiverStream;
use std::collections::{HashMap, VecDeque};
use serde_yaml::Value;

pub struct AvixServer {
    backend: Arc<dyn Backend + Send + Sync>,
    queue: Arc<Mutex<VecDeque<QueuedJob>>>,
    queued_meta: Arc<Mutex<HashMap<String, QueuedMeta>>>,
    id_map: Arc<Mutex<HashMap<String, String>>>,
    workflow_meta: Arc<Mutex<HashMap<String, WorkflowMeta>>>,
}

#[derive(Clone)]
struct QueuedJob {
    queued_id: String,
    job: avix_spec::Job,
    enqueued_at: i64,
}

#[derive(Clone)]
struct QueuedMeta {
    name: String,
    status: String,
    created_at: i64,
}

#[derive(Clone)]
struct WorkflowMeta {
    name: String,
    status: String,
    created_at: i64,
    step_ids: Vec<String>,
}

impl AvixServer {
    pub fn new(backend: Arc<dyn Backend + Send + Sync>) -> Self {
        let queue = Arc::new(Mutex::new(VecDeque::new()));
        let queued_meta = Arc::new(Mutex::new(HashMap::new()));
        let id_map = Arc::new(Mutex::new(HashMap::new()));
        let workflow_meta = Arc::new(Mutex::new(HashMap::new()));
        let backend_clone = backend.clone();
        let queue_clone = queue.clone();
        let queued_meta_clone = queued_meta.clone();
        let id_map_clone = id_map.clone();

        // simple queue worker
        tokio::spawn(async move {
            loop {
                let job = {
                    let mut q = queue_clone.lock().await;
                    q.pop_front()
                };

                if let Some(queued) = job {
                    {
                        let mut meta = queued_meta_clone.lock().await;
                        if let Some(m) = meta.get_mut(&queued.queued_id) {
                            m.status = "running".to_string();
                        }
                    }

                    println!("Processing job from queue: {}", queued.job.metadata.name);
                    match backend_clone.submit(queued.job).await {
                        Ok(backend_id) => {
                            let mut map = id_map_clone.lock().await;
                            map.insert(queued.queued_id, backend_id);
                        }
                        Err(e) => {
                            {
                                let mut meta = queued_meta_clone.lock().await;
                                if let Some(m) = meta.get_mut(&queued.queued_id) {
                                    m.status = "failed".to_string();
                                }
                            }
                            eprintln!("Failed to process job from queue: {}", e);
                        }
                    }
                } else {
                    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                }
            }
        });

        Self {
            backend,
            queue,
            queued_meta,
            id_map,
            workflow_meta,
        }
    }
}

fn generate_queued_id() -> String {
    format!("q-{:x}", rand::random::<u64>())
}

fn generate_workflow_id() -> String {
    format!("wf-{:x}", rand::random::<u64>())
}

fn extract_kind(yaml: &str) -> Result<Option<String>, serde_yaml::Error> {
    let v: Value = serde_yaml::from_str(yaml)?;
    Ok(v
        .get("kind")
        .and_then(|k| k.as_str())
        .map(|s| s.to_string()))
}

#[tonic::async_trait]
impl JobService for AvixServer {
    async fn submit_job(
        &self,
        request: Request<SubmitJobRequest>,
    ) -> Result<Response<SubmitJobResponse>, Status> {
        let req = request.into_inner();
        let kind = extract_kind(&req.job_spec_yaml)
            .map_err(|e| Status::invalid_argument(format!("Invalid YAML: {}", e)))?;

        if kind.as_deref() == Some("Workflow") {
            let workflow: avix_spec::Workflow = serde_yaml::from_str(&req.job_spec_yaml)
                .map_err(|e| Status::invalid_argument(format!("Invalid Workflow YAML: {}", e)))?;

            let wf_id = generate_workflow_id();
            let now = chrono::Utc::now().timestamp();
            {
                let mut meta = self.workflow_meta.lock().await;
                meta.insert(
                    wf_id.clone(),
                    WorkflowMeta {
                        name: workflow.metadata.name.clone(),
                        status: "running".to_string(),
                        created_at: now,
                        step_ids: Vec::new(),
                    },
                );
            }

            let backend = self.backend.clone();
            let workflow_meta = self.workflow_meta.clone();

            tokio::spawn(async move {
                let mut step_ids: Vec<String> = Vec::new();
                let on_failure = workflow
                    .spec
                    .on_failure
                    .as_deref()
                    .unwrap_or("stop")
                    .to_string();

                for job in workflow.spec.jobs.into_iter() {
                    match backend.submit(job).await {
                        Ok(id) => {
                            step_ids.push(id.clone());
                            {
                                let mut meta = workflow_meta.lock().await;
                                if let Some(m) = meta.get_mut(&wf_id) {
                                    m.step_ids = step_ids.clone();
                                    m.status = "running".to_string();
                                }
                            }

                            match backend.wait(&id).await {
                                Ok(code) if code == 0 => {}
                                Ok(_code) => {
                                    if on_failure != "continue" {
                                        let mut meta = workflow_meta.lock().await;
                                        if let Some(m) = meta.get_mut(&wf_id) {
                                            m.status = "failed".to_string();
                                        }
                                        return;
                                    }
                                }
                                Err(_) => {
                                    let mut meta = workflow_meta.lock().await;
                                    if let Some(m) = meta.get_mut(&wf_id) {
                                        m.status = "failed".to_string();
                                    }
                                    return;
                                }
                            }
                        }
                        Err(_) => {
                            let mut meta = workflow_meta.lock().await;
                            if let Some(m) = meta.get_mut(&wf_id) {
                                m.status = "failed".to_string();
                            }
                            return;
                        }
                    }
                }

                let mut meta = workflow_meta.lock().await;
                if let Some(m) = meta.get_mut(&wf_id) {
                    m.status = "completed".to_string();
                }
            });

            return Ok(Response::new(SubmitJobResponse {
                job_id: wf_id,
                status: "SUBMITTED".to_string(),
            }));
        }

        let job: avix_spec::Job = serde_yaml::from_str(&req.job_spec_yaml)
            .map_err(|e| Status::invalid_argument(format!("Invalid YAML: {}", e)))?;

        // If priority is high, submit directly, otherwise queue it
        let priority = job.spec.priority.unwrap_or(0);
        if priority > 50 {
            let job_id = self.backend.submit(job).await
                .map_err(|e| Status::internal(format!("Failed to submit job: {}", e)))?;

            Ok(Response::new(SubmitJobResponse {
                job_id,
                status: "SUBMITTED".to_string(),
            }))
        } else {
            let queued_id = generate_queued_id();
            let now = chrono::Utc::now().timestamp();

            {
                let mut meta = self.queued_meta.lock().await;
                meta.insert(
                    queued_id.clone(),
                    QueuedMeta {
                        name: job.metadata.name.clone(),
                        status: "queued".to_string(),
                        created_at: now,
                    },
                );
            }

            let mut q = self.queue.lock().await;
            q.push_back(QueuedJob {
                queued_id: queued_id.clone(),
                job,
                enqueued_at: now,
            });
            Ok(Response::new(SubmitJobResponse {
                job_id: queued_id,
                status: "QUEUED".to_string(),
            }))
        }
    }

    async fn list_jobs(
        &self,
        request: Request<ListJobsRequest>,
    ) -> Result<Response<ListJobsResponse>, Status> {
        let req = request.into_inner();
        let mut jobs = self.backend.list(Some(&req.namespace).filter(|s| !s.is_empty())).await
            .map_err(|e| Status::internal(format!("Failed to list jobs: {}", e)))?;

        // Add queued jobs (in-memory)
        let queued = self.queued_meta.lock().await;
        for (id, meta) in queued.iter() {
            jobs.push(crate::JobStatus {
                id: id.clone(),
                name: meta.name.clone(),
                status: meta.status.clone(),
                created_at: meta.created_at,
            });
        }

        // Add workflows (in-memory)
        let workflows = self.workflow_meta.lock().await;
        for (id, meta) in workflows.iter() {
            jobs.push(crate::JobStatus {
                id: id.clone(),
                name: meta.name.clone(),
                status: meta.status.clone(),
                created_at: meta.created_at,
            });
        }

        let summaries = jobs.into_iter().map(|j| JobSummary {
            id: j.id,
            name: j.name,
            status: j.status,
            created_at: Some(prost_types::Timestamp {
                seconds: j.created_at,
                nanos: 0,
            }),
        }).collect();

        Ok(Response::new(ListJobsResponse { jobs: summaries }))
    }

    async fn get_job_status(
        &self,
        request: Request<GetJobStatusRequest>,
    ) -> Result<Response<GetJobStatusResponse>, Status> {
        let req = request.into_inner();

        // Workflow status is tracked in-memory.
        {
            let workflows = self.workflow_meta.lock().await;
            if let Some(m) = workflows.get(&req.job_id) {
                return Ok(Response::new(GetJobStatusResponse {
                    id: req.job_id,
                    name: m.name.clone(),
                    status: m.status.clone(),
                    created_at: Some(prost_types::Timestamp {
                        seconds: m.created_at,
                        nanos: 0,
                    }),
                }));
            }
        }

        // If it's a queued id that already mapped to a backend id, resolve it.
        let resolved_id = {
            let map = self.id_map.lock().await;
            map.get(&req.job_id).cloned()
        };

        // If still queued and not started yet, return in-memory status.
        if resolved_id.is_none() {
            let meta = self.queued_meta.lock().await;
            if let Some(m) = meta.get(&req.job_id) {
                return Ok(Response::new(GetJobStatusResponse {
                    id: req.job_id,
                    name: m.name.clone(),
                    status: m.status.clone(),
                    created_at: Some(prost_types::Timestamp {
                        seconds: m.created_at,
                        nanos: 0,
                    }),
                }));
            }
        }

        let lookup_id = resolved_id.as_deref().unwrap_or(&req.job_id);

        let jobs = self
            .backend
            .list(None)
            .await
            .map_err(|e| Status::internal(format!("Failed to list jobs: {}", e)))?;

        let job = jobs
            .into_iter()
            .find(|j| j.id == lookup_id || j.id.starts_with(lookup_id))
            .ok_or_else(|| Status::not_found(format!("Job not found: {}", req.job_id)))?;

        Ok(Response::new(GetJobStatusResponse {
            id: job.id,
            name: job.name,
            status: job.status,
            created_at: Some(prost_types::Timestamp {
                seconds: job.created_at,
                nanos: 0,
            }),
        }))
    }

    type GetJobLogsStream = ReceiverStream<Result<LogLine, Status>>;

    async fn get_job_logs(
        &self,
        request: Request<GetJobLogsRequest>,
    ) -> Result<Response<Self::GetJobLogsStream>, Status> {
        let req = request.into_inner();
        let mut rx = self.backend.logs(&req.job_id, req.follow).await
            .map_err(|e| Status::internal(format!("Failed to get logs: {}", e)))?;

        let (tx, rx_out) = mpsc::channel(100);

        tokio::spawn(async move {
            while let Some(line) = rx.recv().await {
                let log_line = LogLine {
                    content: line,
                    timestamp: Some(prost_types::Timestamp {
                        seconds: chrono::Utc::now().timestamp(),
                        nanos: 0,
                    }),
                    stream: "stdout".to_string(),
                };
                if tx.send(Ok(log_line)).await.is_err() {
                    break;
                }
            }
        });

        Ok(Response::new(ReceiverStream::new(rx_out)))
    }

    type GetJobMetricsStream = ReceiverStream<Result<MetricPoint, Status>>;

    async fn get_job_metrics(
        &self,
        request: Request<GetJobMetricsRequest>,
    ) -> Result<Response<Self::GetJobMetricsStream>, Status> {
        let _req = request.into_inner();
        let (tx, rx_out) = mpsc::channel(100);

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(2));
            loop {
                interval.tick().await;
                let now = chrono::Utc::now().timestamp();
                // Simulated CPU metric
                let cpu = MetricPoint {
                    name: "cpu".to_string(),
                    value: 45.0 + (rand::random::<f64>() * 10.0),
                    timestamp: Some(prost_types::Timestamp { seconds: now, nanos: 0 }),
                };
                if tx.send(Ok(cpu)).await.is_err() { break; }

                // Simulated Memory metric
                let mem = MetricPoint {
                    name: "memory".to_string(),
                    value: 60.0 + (rand::random::<f64>() * 15.0),
                    timestamp: Some(prost_types::Timestamp { seconds: now, nanos: 0 }),
                };
                if tx.send(Ok(mem)).await.is_err() { break; }
            }
        });

        Ok(Response::new(ReceiverStream::new(rx_out)))
    }

    async fn estimate_cost(
        &self,
        request: Request<EstimateCostRequest>,
    ) -> Result<Response<EstimateCostResponse>, Status> {
        let req = request.into_inner();
        let job: avix_spec::Job = serde_yaml::from_str(&req.job_spec_yaml)
            .map_err(|e| Status::invalid_argument(format!("Invalid YAML: {}", e)))?;

        let backend_name = job.spec.backend.as_deref().unwrap_or("auto");

        // Extract resources
        let (cpu, memory_gi, gpu) = if let Some(ref res) = job.spec.resources {
            (
                res.cpu.as_deref().unwrap_or("1").parse::<f64>().unwrap_or(1.0),
                parse_memory_gi(res.memory.as_deref().unwrap_or("1Gi")),
                res.gpu.unwrap_or(0) as f64,
            )
        } else {
            (1.0, 1.0, 0.0)
        };

        let hourly = match backend_name {
            "local-docker" => 0.0,
            "aws-lambda" => cpu * 0.0000166667 * 3600.0 + memory_gi * 0.0000002501 * 3600.0, // per hour equivalent
            "aws-batch" => cpu * 0.04 + memory_gi * 0.004 + gpu * 2.5,
            "k8s-job" | "kubernetes" => cpu * 0.03 + memory_gi * 0.003 + gpu * 2.0,
            "gcp-cloudrun" => cpu * 0.024 + memory_gi * 0.0025,
            "azure-batch" => cpu * 0.035 + memory_gi * 0.0035 + gpu * 2.3,
            _ => cpu * 0.05 + memory_gi * 0.005 + gpu * 2.5,
        };

        Ok(Response::new(EstimateCostResponse {
            estimated_cost_usd: hourly,
            currency: "USD".to_string(),
        }))
    }

    async fn stop_job(
        &self,
        request: Request<StopJobRequest>,
    ) -> Result<Response<StopJobResponse>, Status> {
        let req = request.into_inner();

        // If it's a workflow id, mark cancelled (best-effort).
        {
            let mut meta = self.workflow_meta.lock().await;
            if let Some(m) = meta.get_mut(&req.job_id) {
                m.status = "cancelled".to_string();
                return Ok(Response::new(StopJobResponse { success: true }));
            }
        }

        // If job is still queued, remove it and mark cancelled.
        {
            let mut q = self.queue.lock().await;
            if let Some(pos) = q.iter().position(|j| j.queued_id == req.job_id) {
                q.remove(pos);
                let mut meta = self.queued_meta.lock().await;
                if let Some(m) = meta.get_mut(&req.job_id) {
                    m.status = "cancelled".to_string();
                }
                return Ok(Response::new(StopJobResponse { success: true }));
            }
        }

        // If it's a queued id already mapped to backend id, stop the backend job.
        let backend_id = {
            let map = self.id_map.lock().await;
            map.get(&req.job_id).cloned().unwrap_or(req.job_id)
        };

        self.backend
            .stop(&backend_id)
            .await
            .map_err(|e| Status::internal(format!("Failed to stop job: {}", e)))?;

        Ok(Response::new(StopJobResponse { success: true }))
    }
}

fn parse_memory_gi(mem: &str) -> f64 {
    let mem = mem.trim();
    if let Some(rest) = mem.strip_suffix("Gi") {
        rest.parse().unwrap_or(1.0)
    } else if let Some(rest) = mem.strip_suffix("Mi") {
        rest.parse::<f64>().unwrap_or(1024.0) / 1024.0
    } else if let Some(rest) = mem.strip_suffix("G") {
        rest.parse().unwrap_or(1.0)
    } else {
        mem.parse().unwrap_or(1.0)
    }
}

#[cfg(test)]
mod tests {
    use super::{parse_memory_gi, AvixServer};
    use crate::{Backend, JobStatus};
    use anyhow::Result;
    use std::sync::Arc;
    use tokio::sync::mpsc;
    use tonic::Request;

    #[test]
    fn test_parse_memory_gi() {
        assert!((parse_memory_gi("1Gi") - 1.0).abs() < 1e-6);
        assert!((parse_memory_gi("512Mi") - 0.5).abs() < 1e-6);
        assert!((parse_memory_gi("2G") - 2.0).abs() < 1e-6);
        assert!((parse_memory_gi("3") - 3.0).abs() < 1e-6);
    }

    struct MockBackend {
        jobs: Vec<JobStatus>,
    }

    #[async_trait::async_trait]
    impl Backend for MockBackend {
        async fn submit(&self, _job: avix_spec::Job) -> Result<String> {
            Ok("mock-id".to_string())
        }

        async fn stop(&self, _id: &str) -> Result<()> {
            Ok(())
        }

        async fn list(&self, _namespace: Option<&str>) -> Result<Vec<JobStatus>> {
            Ok(self.jobs.clone())
        }

        async fn logs(&self, _id: &str, _follow: bool) -> Result<mpsc::Receiver<String>> {
            let (_tx, rx) = mpsc::channel(1);
            Ok(rx)
        }

        async fn wait(&self, _id: &str) -> Result<i64> {
            Ok(0)
        }
    }

    #[tokio::test]
    async fn test_get_job_status_exact_match() {
        let backend = Arc::new(MockBackend {
            jobs: vec![JobStatus {
                id: "abc123".to_string(),
                name: "job-a".to_string(),
                status: "running".to_string(),
                created_at: 123,
            }],
        });
        let server = AvixServer::new(backend);

        let resp = server
            .get_job_status(Request::new(avix_spec::v1::GetJobStatusRequest {
                job_id: "abc123".to_string(),
            }))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(resp.id, "abc123");
        assert_eq!(resp.name, "job-a");
        assert_eq!(resp.status, "running");
        assert_eq!(resp.created_at.unwrap().seconds, 123);
    }

    #[tokio::test]
    async fn test_get_job_status_prefix_match() {
        let backend = Arc::new(MockBackend {
            jobs: vec![JobStatus {
                id: "abcdef012345".to_string(),
                name: "job-b".to_string(),
                status: "completed".to_string(),
                created_at: 456,
            }],
        });
        let server = AvixServer::new(backend);

        let resp = server
            .get_job_status(Request::new(avix_spec::v1::GetJobStatusRequest {
                job_id: "abcdef".to_string(),
            }))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(resp.id, "abcdef012345");
    }

    #[tokio::test]
    async fn test_get_job_status_not_found() {
        let backend = Arc::new(MockBackend { jobs: vec![] });
        let server = AvixServer::new(backend);

        let err = server
            .get_job_status(Request::new(avix_spec::v1::GetJobStatusRequest {
                job_id: "missing".to_string(),
            }))
            .await
            .unwrap_err();

        assert_eq!(err.code(), tonic::Code::NotFound);
    }

    #[tokio::test]
    async fn test_stop_job_cancels_queued_job() {
        let backend = Arc::new(MockBackend { jobs: vec![] });
        let server = AvixServer::new(backend);

        let yaml = r#"apiVersion: avix.vargafoundation.org/v1alpha1
kind: Job
metadata:
  name: queued-job
spec:
  priority: 0
  execution:
    image: alpine
    command: [\"echo\", \"hi\"]
"#;

        let submit = server
            .submit_job(Request::new(SubmitJobRequest {
                job_spec_yaml: yaml.to_string(),
            }))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(submit.status, "QUEUED");
        assert!(submit.job_id.starts_with("q-"));

        server
            .stop_job(Request::new(StopJobRequest {
                job_id: submit.job_id.clone(),
            }))
            .await
            .unwrap();

        let status = server
            .get_job_status(Request::new(GetJobStatusRequest {
                job_id: submit.job_id,
            }))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(status.status, "cancelled");
    }
}

pub async fn start_server(addr: std::net::SocketAddr) -> anyhow::Result<()> {
    let backend = Arc::new(DockerBackend::new()?);
    let service = AvixServer::new(backend);

    println!("Avix server listening on {}", addr);

    Server::builder()
        .add_service(JobServiceServer::new(service))
        .serve(addr)
        .await?;

    Ok(())
}
