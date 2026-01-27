use clap::{Parser, Subcommand};
use std::path::PathBuf;
use anyhow::Result;
use colored::*;
use avix_spec::Job;
use avix_spec::v1::job_service_client::JobServiceClient;
use avix_spec::v1::*;
use std::fs;
use std::io::{self, Write};
use futures_util::StreamExt;
use tabled::{Table, Tabled, settings::{Style, Color, object::Columns}};
use indicatif::{ProgressBar, ProgressStyle};
use console::style;
use chrono::Local;

mod tui;

#[derive(Parser)]
#[command(name = "avix")]
#[command(author = "Varga Foundation")]
#[command(version = "0.1.0")]
#[command(about = "🚀 Avix: Ultra-simple batch and streaming job scheduler", long_about = None)]
#[command(styles = get_styles())]
struct Cli {
    #[command(subcommand)]
    command: Commands,

    /// Server address
    #[arg(short, long, default_value = "http://[::1]:50051", global = true)]
    server: String,
}

fn get_styles() -> clap::builder::Styles {
    clap::builder::Styles::styled()
        .header(clap::builder::styling::AnsiColor::Cyan.on_default().bold())
        .usage(clap::builder::styling::AnsiColor::Cyan.on_default().bold())
        .literal(clap::builder::styling::AnsiColor::Green.on_default())
        .placeholder(clap::builder::styling::AnsiColor::Yellow.on_default())
}

#[derive(Subcommand)]
enum Commands {
    /// 📦 Manage jobs
    Job {
        #[command(subcommand)]
        action: JobAction,
    },
    /// ⚙️  Initialize configuration
    Init,
    /// 📊 Show job metrics with live graphs
    Metrics {
        /// Job ID
        id: String,
        /// Show ASCII graph
        #[arg(short, long)]
        graph: bool,
    },
    /// 🖥️  Start the Avix server (local mode)
    Server {
        /// Address to listen on
        #[arg(short, long, default_value = "[::1]:50051")]
        addr: String,
    },
    /// 📋 Generate job templates
    Template {
        #[command(subcommand)]
        action: TemplateAction,
    },
    /// 🎛️  Interactive dashboard (TUI)
    Dashboard,
}

#[derive(Subcommand)]
enum JobAction {
    /// Submit a job
    Submit {
        /// Path to the job specification YAML
        file: PathBuf,
        /// Backend to use (e.g., local-docker, aws-lambda)
        #[arg(short, long)]
        backend: Option<String>,
        /// Follow logs after submission
        #[arg(short, long)]
        watch: bool,
        /// Dry run (validate without submitting)
        #[arg(long)]
        dry_run: bool,
    },
    /// List jobs with colorful table
    List {
        /// Namespace to filter by
        #[arg(short, long)]
        namespace: Option<String>,
        /// Output format (table, json, yaml)
        #[arg(short, long, default_value = "table")]
        output: String,
    },
    /// Show job logs with live streaming
    Logs {
        /// Job ID
        id: String,
        /// Follow log stream in real-time
        #[arg(short, long)]
        follow: bool,
        /// Show timestamps
        #[arg(short, long)]
        timestamps: bool,
    },
    /// Get job status
    Status {
        /// Job ID
        id: String,
    },
    /// Cancel a running job
    Cancel {
        /// Job ID
        id: String,
    },
}

#[derive(Subcommand)]
enum TemplateAction {
    /// Generate a template
    Generate {
        /// Template type (ml-inference, spark-etl, grid-search, simple)
        #[arg(value_name = "TYPE")]
        template_type: String,
    },
    /// List available templates
    List,
}

#[derive(Tabled)]
struct JobRow {
    #[tabled(rename = "ID")]
    id: String,
    #[tabled(rename = "NAME")]
    name: String,
    #[tabled(rename = "STATUS")]
    status: String,
    #[tabled(rename = "BACKEND")]
    backend: String,
    #[tabled(rename = "CREATED")]
    created: String,
}

fn print_banner() {
    let banner = r#"
    ╔═══════════════════════════════════════════════════════════╗
    ║     _____        ___      ___  _____  ___  ___            ║
    ║    /  _  \      |   \    /   ||_   _| \  \/  /            ║
    ║   /  /_\  \     |    \  /    |  | |    \    /             ║
    ║  /  _____  \    |  |\ \/ /|  |  | |    /    \             ║
    ║ /__/     \__\   |__| \__/ |__|  |_|   /__/\__\            ║
    ║                                                           ║
    ║  🚀 Ultra-simple batch & streaming job scheduler          ║
    ╚═══════════════════════════════════════════════════════════╝
"#;
    println!("{}", banner.cyan());
}

fn print_section(title: &str) {
    println!("\n{} {}", "▶".blue().bold(), title.white().bold());
    println!("{}", "─".repeat(50).dimmed());
}

fn print_success(msg: &str) {
    println!("{} {}", "✓".green().bold(), msg);
}

fn print_error(msg: &str) {
    eprintln!("{} {}", "✗".red().bold(), msg.red());
}

fn print_info(msg: &str) {
    println!("{} {}", "ℹ".blue(), msg);
}

fn print_warning(msg: &str) {
    println!("{} {}", "⚠".yellow(), msg.yellow());
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    match &cli.command {
        Commands::Job { action } => {
            let mut client = match JobServiceClient::connect(cli.server.clone()).await {
                Ok(c) => c,
                Err(e) => {
                    print_error(&format!("Could not connect to server at {}: {}", cli.server, e));
                    print_info("Make sure the Avix server is running: avix server");
                    return Ok(());
                }
            };

            match action {
                JobAction::Submit { file, backend, watch, dry_run } => {
                    submit_job(&mut client, file, backend.as_deref(), *watch, *dry_run).await?;
                }
                JobAction::List { namespace, output } => {
                    list_jobs(&mut client, namespace.as_deref(), output).await?;
                }
                JobAction::Logs { id, follow, timestamps } => {
                    show_logs(&mut client, id.clone(), *follow, *timestamps).await?;
                }
                JobAction::Status { id } => {
                    show_status(&mut client, id.clone()).await?;
                }
                JobAction::Cancel { id } => {
                    cancel_job(&mut client, id.clone()).await?;
                }
            }
        }
        Commands::Init => {
            init_config()?;
        }
        Commands::Metrics { id, graph } => {
            let mut client = JobServiceClient::connect(cli.server.clone()).await
                .map_err(|e| anyhow::anyhow!("Could not connect to server: {}", e))?;
            show_metrics(&mut client, id.clone(), *graph).await?;
        }
        Commands::Server { addr } => {
            print_banner();
            print_section("Starting Avix Server");
            let socket_addr = addr.parse()?;
            print_success(&format!("Server listening on {}", addr));
            avix_core::server::start_server(socket_addr).await?;
        }
        Commands::Template { action } => {
            match action {
                TemplateAction::Generate { template_type } => {
                    generate_template(template_type)?;
                }
                TemplateAction::List => {
                    list_templates();
                }
            }
        }
        Commands::Dashboard => {
            tui::run_dashboard().await?;
        }
    }

    Ok(())
}

fn init_config() -> Result<()> {
    print_banner();
    print_section("Initializing Avix Configuration");
    
    let spinner = ProgressBar::new_spinner();
    spinner.set_style(ProgressStyle::default_spinner()
        .tick_chars("⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏")
        .template("{spinner:.cyan} {msg}")?);
    spinner.set_message("Creating configuration directory...");
    
    let config_dir = dirs::config_dir().unwrap_or_else(|| PathBuf::from(".")).join("avix");
    fs::create_dir_all(&config_dir)?;
    
    spinner.set_message("Writing default configuration...");
    let config_path = config_dir.join("config.yaml");
    let default_config = r#"# Avix Configuration
server_url: http://[::1]:50051
default_backend: local-docker

# Cloud backends (optional)
# aws:
#   region: us-east-1
#   profile: default
# 
# kubernetes:
#   kubeconfig: ~/.kube/config
#   context: default
"#;
    fs::write(&config_path, default_config)?;
    
    spinner.finish_and_clear();
    print_success(&format!("Configuration created at {}", config_path.display()));
    
    println!("\n{}", "Next steps:".white().bold());
    println!("  {} Edit {} to configure backends", "1.".cyan(), config_path.display());
    println!("  {} Start the server: {}", "2.".cyan(), "avix server".green());
    println!("  {} Submit your first job: {}", "3.".cyan(), "avix job submit job.yaml".green());
    
    Ok(())
}

async fn submit_job(
    client: &mut JobServiceClient<tonic::transport::Channel>,
    file: &PathBuf,
    backend: Option<&str>,
    watch: bool,
    dry_run: bool,
) -> Result<()> {
    print_section("Submitting Job");
    
    let spinner = ProgressBar::new_spinner();
    spinner.set_style(ProgressStyle::default_spinner()
        .tick_chars("⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏")
        .template("{spinner:.cyan} {msg}")?);
    
    spinner.set_message(format!("Reading job spec from {}...", file.display()));
    let content = fs::read_to_string(file)?;
    let mut job: Job = serde_yaml::from_str(&content)?;
    
    if let Some(b) = backend {
        job.spec.backend = Some(b.to_string());
    }
    
    spinner.finish_and_clear();
    
    // Display job summary
    println!("\n{}", "Job Summary:".white().bold());
    println!("  {} {}", "Name:".dimmed(), job.metadata.name.cyan());
    println!("  {} {}", "Image:".dimmed(), job.spec.execution.image.yellow());
    println!("  {} {}", "Backend:".dimmed(), job.spec.backend.as_deref().unwrap_or("auto").green());
    if let Some(ref res) = job.spec.resources {
        if let Some(ref cpu) = res.cpu {
            println!("  {} {}", "CPU:".dimmed(), cpu);
        }
        if let Some(ref mem) = res.memory {
            println!("  {} {}", "Memory:".dimmed(), mem);
        }
        if let Some(gpu) = res.gpu {
            println!("  {} {}", "GPU:".dimmed(), gpu);
        }
    }
    
    if dry_run {
        print_warning("Dry run mode - job not submitted");
        println!("\n{}", "Generated YAML:".white().bold());
        println!("{}", serde_yaml::to_string(&job)?);
        return Ok(());
    }
    
    let yaml_to_send = serde_yaml::to_string(&job)?;
    
    let pb = ProgressBar::new_spinner();
    pb.set_style(ProgressStyle::default_spinner()
        .tick_chars("⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏")
        .template("{spinner:.green} {msg}")?);
    pb.set_message("Submitting job...");
    pb.enable_steady_tick(std::time::Duration::from_millis(80));
    
    let response = client.submit_job(SubmitJobRequest {
        job_spec_yaml: yaml_to_send,
    }).await?;

    let res = response.into_inner();
    pb.finish_and_clear();
    
    print_success(&format!("Job submitted successfully!"));
    println!("\n  {} {}", "Job ID:".dimmed(), res.job_id.cyan().bold());
    
    if watch {
        println!();
        show_logs(client, res.job_id, true, true).await?;
    }
    
    Ok(())
}

async fn list_jobs(
    client: &mut JobServiceClient<tonic::transport::Channel>,
    namespace: Option<&str>,
    output: &str,
) -> Result<()> {
    print_section("Jobs");
    
    let response = client.list_jobs(ListJobsRequest {
        namespace: namespace.unwrap_or("").to_string(),
    }).await?;

    let res = response.into_inner();
    
    if res.jobs.is_empty() {
        print_info("No jobs found");
        return Ok(());
    }
    
    match output {
        "json" => {
            println!("{}", serde_json::to_string_pretty(&res.jobs.iter().map(|j| {
                serde_json::json!({
                    "id": j.id,
                    "name": j.name,
                    "status": j.status
                })
            }).collect::<Vec<_>>())?);
        }
        "yaml" => {
            for job in &res.jobs {
                println!("---");
                println!("id: {}", job.id);
                println!("name: {}", job.name);
                println!("status: {}", job.status);
            }
        }
        _ => {
            let rows: Vec<JobRow> = res.jobs.iter().map(|job| {
                let status_colored = match job.status.as_str() {
                    "running" => format!("{}", "● RUNNING".green()),
                    "pending" => format!("{}", "◐ PENDING".yellow()),
                    "completed" => format!("{}", "✓ COMPLETED".cyan()),
                    "failed" => format!("{}", "✗ FAILED".red()),
                    "cancelled" => format!("{}", "○ CANCELLED".dimmed()),
                    _ => job.status.clone(),
                };
                JobRow {
                    id: job.id.chars().take(12).collect(),
                    name: job.name.clone(),
                    status: status_colored,
                    backend: "local-docker".to_string(),
                    created: Local::now().format("%Y-%m-%d %H:%M").to_string(),
                }
            }).collect();
            
            let table = Table::new(rows)
                .with(Style::rounded())
                .to_string();
            
            println!("{}", table);
            println!("\n{} {} job(s)", "Total:".dimmed(), res.jobs.len());
        }
    }
    
    Ok(())
}

async fn show_logs(
    client: &mut JobServiceClient<tonic::transport::Channel>,
    id: String,
    follow: bool,
    timestamps: bool,
) -> Result<()> {
    print_section(&format!("Logs for job {}", id.chars().take(12).collect::<String>()));
    
    if follow {
        print_info("Streaming logs (Ctrl+C to stop)...");
    }
    
    let response = client.get_job_logs(GetJobLogsRequest {
        job_id: id,
        follow,
    }).await?;

    let mut stream = response.into_inner();

    while let Some(line) = stream.next().await {
        let line = line?;
        let content = &line.content;
        
        // Colorize log output
        let colored_line = if content.to_lowercase().contains("error") || content.to_lowercase().contains("exception") {
            content.red().to_string()
        } else if content.to_lowercase().contains("warn") {
            content.yellow().to_string()
        } else if content.to_lowercase().contains("info") {
            content.cyan().to_string()
        } else if content.to_lowercase().contains("debug") {
            content.dimmed().to_string()
        } else {
            content.to_string()
        };
        
        if timestamps {
            print!("{} {}", Local::now().format("%H:%M:%S").to_string().dimmed(), colored_line);
        } else {
            print!("{}", colored_line);
        }
        io::stdout().flush()?;
    }

    Ok(())
}

async fn show_status(
    client: &mut JobServiceClient<tonic::transport::Channel>,
    id: String,
) -> Result<()> {
    print_section(&format!("Status for job {}", id.chars().take(12).collect::<String>()));
    
    let response = client.get_job_status(GetJobStatusRequest {
        job_id: id.clone(),
    }).await?;
    
    let status = response.into_inner();
    
    let status_icon = match status.status.as_str() {
        "running" => "●".green(),
        "pending" => "◐".yellow(),
        "completed" => "✓".cyan(),
        "failed" => "✗".red(),
        _ => "○".dimmed(),
    };
    
    println!("\n  {} {} {}", status_icon, "Status:".dimmed(), status.status.to_uppercase().bold());
    println!("  {} {}", "Job ID:".dimmed(), id);
    
    Ok(())
}

async fn cancel_job(
    client: &mut JobServiceClient<tonic::transport::Channel>,
    id: String,
) -> Result<()> {
    print_section("Cancelling Job");
    
    let pb = ProgressBar::new_spinner();
    pb.set_style(ProgressStyle::default_spinner()
        .tick_chars("⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏")
        .template("{spinner:.yellow} {msg}")?);
    pb.set_message(format!("Cancelling job {}...", id.chars().take(12).collect::<String>()));
    pb.enable_steady_tick(std::time::Duration::from_millis(80));
    
    let _response = client.cancel_job(CancelJobRequest {
        job_id: id.clone(),
    }).await?;
    
    pb.finish_and_clear();
    print_success(&format!("Job {} cancelled", id.chars().take(12).collect::<String>()));
    
    Ok(())
}

async fn show_metrics(
    client: &mut JobServiceClient<tonic::transport::Channel>,
    id: String,
    graph: bool,
) -> Result<()> {
    print_section(&format!("Metrics for job {}", id.chars().take(12).collect::<String>()));
    
    let response = client.get_job_metrics(GetJobMetricsRequest {
        job_id: id,
    }).await?;

    let mut stream = response.into_inner();
    let mut cpu_history: Vec<f64> = Vec::new();
    let mut mem_history: Vec<f64> = Vec::new();

    while let Some(point) = stream.next().await {
        let point = point?;
        
        if graph {
            // Collect data for graph
            if point.name == "cpu" {
                cpu_history.push(point.value);
            } else if point.name == "memory" {
                mem_history.push(point.value);
            }
            
            // Clear screen and redraw
            print!("\x1B[2J\x1B[1;1H");
            print_section("Live Metrics");
            
            println!("\n{}", "CPU Usage:".cyan().bold());
            print_ascii_graph(&cpu_history, 100.0, "█".green());
            
            println!("\n{}", "Memory Usage:".magenta().bold());
            print_ascii_graph(&mem_history, 100.0, "█".magenta());
            
            println!("\n{} Press Ctrl+C to stop", "ℹ".blue());
        } else {
            let bar = create_progress_bar(point.value, 100.0, 30);
            let color = if point.value > 80.0 {
                point.value.to_string().red()
            } else if point.value > 50.0 {
                point.value.to_string().yellow()
            } else {
                point.value.to_string().green()
            };
            println!("{}: {} {} %", point.name.cyan(), bar, color);
        }
    }

    Ok(())
}

fn print_ascii_graph(data: &[f64], max_val: f64, char_style: colored::ColoredString) {
    let height = 10;
    let width = data.len().min(60);
    
    if data.is_empty() {
        println!("  {}", "No data yet...".dimmed());
        return;
    }
    
    let recent_data: Vec<f64> = data.iter().rev().take(width).rev().cloned().collect();
    
    for row in (0..height).rev() {
        let threshold = (row as f64 / height as f64) * max_val;
        print!("{:>5.0}% │", threshold);
        for &val in &recent_data {
            if val >= threshold {
                print!("{}", char_style);
            } else {
                print!(" ");
            }
        }
        println!();
    }
    println!("      └{}", "─".repeat(recent_data.len()));
}

fn create_progress_bar(value: f64, max: f64, width: usize) -> String {
    let filled = ((value / max) * width as f64) as usize;
    let empty = width.saturating_sub(filled);
    format!("[{}{}]", "█".repeat(filled), "░".repeat(empty))
}

fn generate_template(template_type: &str) -> Result<()> {
    print_section(&format!("Generating {} template", template_type));
    
    let template = match template_type {
        "simple" => r#"apiVersion: avix.vargafoundation.org/v1alpha1
kind: Job
metadata:
  name: my-simple-job
  namespace: default
spec:
  backend: local-docker
  execution:
    image: python:3.11-slim
    command: ["python", "-c"]
    args: ["print('Hello from Avix!')"]
"#,
        "ml-inference" => r#"apiVersion: avix.vargafoundation.org/v1alpha1
kind: Job
metadata:
  name: ml-inference-job
  namespace: ml-team
  labels:
    team: ml
    type: inference
spec:
  backend: auto
  queue: ml-batch
  priority: 100
  resources:
    cpu: "4"
    memory: "16Gi"
    gpu: 1
  execution:
    image: pytorch/pytorch:2.0.0-cuda11.7-cudnn8-runtime
    command: ["python", "inference.py"]
    args: ["--model", "model.pt", "--input", "/data/input"]
    env:
      - name: CUDA_VISIBLE_DEVICES
        value: "0"
  mlTracking:
    wandb: true
    tensorboard: true
"#,
        "spark-etl" => r#"apiVersion: avix.vargafoundation.org/v1alpha1
kind: Job
metadata:
  name: spark-etl-job
  namespace: data-eng
spec:
  backend: auto
  queue: etl-batch
  resources:
    cpu: "8"
    memory: "32Gi"
  execution:
    image: apache/spark:3.5.0
    command: ["spark-submit"]
    args:
      - "--master"
      - "local[*]"
      - "--driver-memory"
      - "4g"
      - "/app/etl_job.py"
"#,
        "grid-search" => r#"apiVersion: avix.vargafoundation.org/v1alpha1
kind: Job
metadata:
  name: hyperparameter-search
  namespace: ml-team
spec:
  backend: auto
  queue: ml-batch
  priority: 50
  resources:
    cpu: "2"
    memory: "8Gi"
    gpu: 1
  execution:
    image: pytorch/pytorch:2.0.0-cuda11.7-cudnn8-runtime
    command: ["python", "train.py"]
  mlTracking:
    wandb: true
    mlflow:
      trackingUri: http://mlflow-server:5000
  hyperparams:
    grid:
      learning_rate: [0.01, 0.001, 0.0001]
      batch_size: [32, 64, 128]
      dropout: [0.1, 0.2, 0.3]
"#,
        _ => {
            print_error(&format!("Unknown template type: {}", template_type));
            print_info("Use 'avix template list' to see available templates");
            return Ok(());
        }
    };
    
    println!("{}", template);
    print_success(&format!("Template generated! Redirect to file: avix template generate {} > job.yaml", template_type));
    
    Ok(())
}

fn list_templates() {
    print_section("Available Templates");
    
    let templates = vec![
        ("simple", "Basic job template for quick tasks"),
        ("ml-inference", "ML inference job with GPU and tracking"),
        ("spark-etl", "Apache Spark ETL job"),
        ("grid-search", "Hyperparameter grid search with ML tracking"),
    ];
    
    for (name, desc) in templates {
        println!("  {} {}", name.green().bold(), format!("- {}", desc).dimmed());
    }
    
    println!("\n{}", "Usage:".white().bold());
    println!("  avix template generate <type> > job.yaml");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_job_spec() {
        let yaml = r#"
apiVersion: avix.vargafoundation.org/v1alpha1
kind: Job
metadata:
  name: test-job
spec:
  execution:
    image: alpine
    command: ["echo", "hello"]
"#;
        let job: Job = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(job.metadata.name, "test-job");
        assert_eq!(job.spec.execution.image, "alpine");
    }

    #[test]
    fn test_serialize_job_spec() {
        let job = Job {
            api_version: "v1".to_string(),
            kind: "Job".to_string(),
            metadata: avix_spec::Metadata {
                name: "test".to_string(),
                namespace: None,
                labels: None,
            },
            spec: avix_spec::JobSpec {
                backend: Some("docker".to_string()),
                queue: None,
                priority: Some(10),
                resources: None,
                execution: avix_spec::Execution {
                    image: "busybox".to_string(),
                    command: vec!["ls".to_string()],
                    args: None,
                    env: None,
                },
                ml_tracking: None,
                hyperparams: None,
            }
        };
        let yaml = serde_yaml::to_string(&job).unwrap();
        assert!(yaml.contains("name: test"));
        assert!(yaml.contains("image: busybox"));
    }

    #[test]
    fn test_progress_bar_50_percent() {
        let bar = create_progress_bar(50.0, 100.0, 10);
        assert_eq!(bar.len(), 12); // 10 chars + 2 brackets
        assert!(bar.starts_with('['));
        assert!(bar.ends_with(']'));
    }

    #[test]
    fn test_progress_bar_0_percent() {
        let bar = create_progress_bar(0.0, 100.0, 10);
        assert_eq!(bar, "[░░░░░░░░░░]");
    }

    #[test]
    fn test_progress_bar_100_percent() {
        let bar = create_progress_bar(100.0, 100.0, 10);
        assert_eq!(bar, "[██████████]");
    }

    #[test]
    fn test_progress_bar_custom_width() {
        let bar = create_progress_bar(50.0, 100.0, 20);
        assert_eq!(bar.len(), 22); // 20 chars + 2 brackets
    }

    #[test]
    fn test_job_with_backend_override() {
        let yaml = r#"
apiVersion: avix.vargafoundation.org/v1alpha1
kind: Job
metadata:
  name: backend-test
spec:
  backend: local-docker
  execution:
    image: alpine
    command: ["echo", "test"]
"#;
        let mut job: Job = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(job.spec.backend, Some("local-docker".to_string()));
        
        // Simulate backend override
        job.spec.backend = Some("aws-lambda".to_string());
        assert_eq!(job.spec.backend, Some("aws-lambda".to_string()));
    }

    #[test]
    fn test_job_with_resources() {
        let yaml = r#"
apiVersion: avix.vargafoundation.org/v1alpha1
kind: Job
metadata:
  name: resource-job
spec:
  resources:
    cpu: "4"
    memory: "8Gi"
    gpu: 1
  execution:
    image: pytorch/pytorch
    command: ["python", "train.py"]
"#;
        let job: Job = serde_yaml::from_str(yaml).unwrap();
        let resources = job.spec.resources.unwrap();
        assert_eq!(resources.cpu, Some("4".to_string()));
        assert_eq!(resources.memory, Some("8Gi".to_string()));
        assert_eq!(resources.gpu, Some(1));
    }

    #[test]
    fn test_job_with_priority() {
        let yaml = r#"
apiVersion: avix.vargafoundation.org/v1alpha1
kind: Job
metadata:
  name: priority-job
spec:
  priority: 100
  queue: ml-batch
  execution:
    image: alpine
    command: ["echo"]
"#;
        let job: Job = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(job.spec.priority, Some(100));
        assert_eq!(job.spec.queue, Some("ml-batch".to_string()));
    }

    #[test]
    fn test_cli_styles() {
        // Test that get_styles() returns valid styles
        let styles = get_styles();
        // If this doesn't panic, styles are valid
        let _ = styles;
    }

    #[test]
    fn test_job_row_creation() {
        let row = JobRow {
            id: "abc123".to_string(),
            name: "test-job".to_string(),
            status: "RUNNING".to_string(),
            backend: "local-docker".to_string(),
            created: "2024-01-01 12:00".to_string(),
        };
        assert_eq!(row.id, "abc123");
        assert_eq!(row.name, "test-job");
        assert_eq!(row.status, "RUNNING");
    }

    #[test]
    fn test_job_id_truncation() {
        let long_id = "abcdefghijklmnopqrstuvwxyz123456";
        let truncated: String = long_id.chars().take(12).collect();
        assert_eq!(truncated, "abcdefghijkl");
        assert_eq!(truncated.len(), 12);
    }

    #[test]
    fn test_hello_world_example() {
        let yaml = r#"
apiVersion: avix.vargafoundation.org/v1alpha1
kind: Job
metadata:
  name: hello-world
spec:
  backend: local-docker
  execution:
    image: alpine
    command: ["echo", "Hello from Avix!"]
"#;
        let job: Job = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(job.metadata.name, "hello-world");
        assert_eq!(job.spec.backend, Some("local-docker".to_string()));
        assert_eq!(job.spec.execution.image, "alpine");
        assert_eq!(job.spec.execution.command, vec!["echo", "Hello from Avix!"]);
    }

    #[test]
    fn test_ml_inference_template_structure() {
        // Verify the ml-inference template can be parsed
        let template = r#"apiVersion: avix.vargafoundation.org/v1alpha1
kind: Job
metadata:
  name: ml-inference-job
  namespace: ml-team
  labels:
    team: ml
    type: inference
spec:
  backend: auto
  queue: ml-batch
  priority: 100
  resources:
    cpu: "4"
    memory: "16Gi"
    gpu: 1
  execution:
    image: pytorch/pytorch:2.0.0-cuda11.7-cudnn8-runtime
    command: ["python", "inference.py"]
    args: ["--model", "model.pt", "--input", "/data/input"]
    env:
      - name: CUDA_VISIBLE_DEVICES
        value: "0"
  mlTracking:
    wandb: true
    tensorboard: true
"#;
        let job: Job = serde_yaml::from_str(template).unwrap();
        assert_eq!(job.metadata.name, "ml-inference-job");
        assert_eq!(job.metadata.namespace, Some("ml-team".to_string()));
        assert!(job.spec.ml_tracking.is_some());
    }

    #[test]
    fn test_grid_search_template_structure() {
        let template = r#"apiVersion: avix.vargafoundation.org/v1alpha1
kind: Job
metadata:
  name: hyperparameter-search
  namespace: ml-team
spec:
  backend: auto
  queue: ml-batch
  priority: 50
  resources:
    cpu: "2"
    memory: "8Gi"
    gpu: 1
  execution:
    image: pytorch/pytorch:2.0.0-cuda11.7-cudnn8-runtime
    command: ["python", "train.py"]
  mlTracking:
    wandb: true
    mlflow:
      trackingUri: http://mlflow-server:5000
  hyperparams:
    grid:
      learning_rate: [0.01, 0.001, 0.0001]
      batch_size: [32, 64, 128]
      dropout: [0.1, 0.2, 0.3]
"#;
        let job: Job = serde_yaml::from_str(template).unwrap();
        assert_eq!(job.metadata.name, "hyperparameter-search");
        let hyperparams = job.spec.hyperparams.unwrap();
        let grid = hyperparams.grid.unwrap();
        assert_eq!(grid.get("learning_rate").unwrap().len(), 3);
        assert_eq!(grid.get("batch_size").unwrap().len(), 3);
    }
}
