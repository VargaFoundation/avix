use tonic::{transport::Server, Request, Response, Status};
use avix_spec::v1::job_service_server::{JobService, JobServiceServer};
use avix_spec::v1::*;
use crate::{Backend, DockerBackend};
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};
use tokio_stream::wrappers::ReceiverStream;
use std::collections::VecDeque;

pub struct AvixServer {
    backend: Arc<dyn Backend + Send + Sync>,
    queue: Arc<Mutex<VecDeque<avix_spec::Job>>>,
}

impl AvixServer {
    pub fn new(backend: Arc<dyn Backend + Send + Sync>) -> Self {
        let queue = Arc::new(Mutex::new(VecDeque::new()));
        let backend_clone = backend.clone();
        let queue_clone = queue.clone();

        // simple queue worker
        tokio::spawn(async move {
            loop {
                let job = {
                    let mut q = queue_clone.lock().await;
                    q.pop_front()
                };

                if let Some(job) = job {
                    println!("Processing job from queue: {}", job.metadata.name);
                    if let Err(e) = backend_clone.submit(job).await {
                        eprintln!("Failed to process job from queue: {}", e);
                    }
                } else {
                    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                }
            }
        });

        Self { backend, queue }
    }
}

#[tonic::async_trait]
impl JobService for AvixServer {
    async fn submit_job(
        &self,
        request: Request<SubmitJobRequest>,
    ) -> Result<Response<SubmitJobResponse>, Status> {
        let req = request.into_inner();
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
            let mut q = self.queue.lock().await;
            q.push_back(job);
            Ok(Response::new(SubmitJobResponse {
                job_id: "queued".to_string(),
                status: "QUEUED".to_string(),
            }))
        }
    }

    async fn list_jobs(
        &self,
        request: Request<ListJobsRequest>,
    ) -> Result<Response<ListJobsResponse>, Status> {
        let req = request.into_inner();
        let jobs = self.backend.list(Some(&req.namespace).filter(|s| !s.is_empty())).await
            .map_err(|e| Status::internal(format!("Failed to list jobs: {}", e)))?;

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
                let metric = MetricPoint {
                    name: "cpu_usage".to_string(),
                    value: 45.0 + (rand::random::<f64>() * 10.0), // Fake metric
                    timestamp: Some(prost_types::Timestamp {
                        seconds: chrono::Utc::now().timestamp(),
                        nanos: 0,
                    }),
                };
                if tx.send(Ok(metric)).await.is_err() {
                    break;
                }
            }
        });

        Ok(Response::new(ReceiverStream::new(rx_out)))
    }

    async fn estimate_cost(
        &self,
        _request: Request<EstimateCostRequest>,
    ) -> Result<Response<EstimateCostResponse>, Status> {
        Ok(Response::new(EstimateCostResponse {
            estimated_cost_usd: 0.0,
            currency: "USD".to_string(),
        }))
    }

    async fn stop_job(
        &self,
        request: Request<StopJobRequest>,
    ) -> Result<Response<StopJobResponse>, Status> {
        let req = request.into_inner();
        self.backend.stop(&req.job_id).await
            .map_err(|e| Status::internal(format!("Failed to stop job: {}", e)))?;

        Ok(Response::new(StopJobResponse { success: true }))
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
