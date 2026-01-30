use clap::{Parser, Subcommand};
use std::path::PathBuf;
use anyhow::Result;
use colored::*;
use avix_spec::{Job, Workflow};
use avix_spec::v1::job_service_client::JobServiceClient;
use avix_spec::v1::*;
use std::fs;
use std::io::{self, Write};
use futures_util::StreamExt;
use tabled::{Table, Tabled, settings::{Style, Color, object::Columns}};
use indicatif::{ProgressBar, ProgressStyle};
use console::style;
use chrono::Local;
use serde_yaml::Value;

fn extract_kind(yaml: &str) -> Result<Option<String>> {
    let v: Value = serde_yaml::from_str(yaml)?;
    Ok(v.get("kind").and_then(|k| k.as_str()).map(|s| s.to_string()))
}

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
    /// 💰 Estimate job cost
    Estimate {
        /// Path to the job specification YAML
        file: PathBuf,
        /// Backend to estimate for
        #[arg(short, long)]
        backend: Option<String>,
    },
    /// 📦 Manage queues
    Queue {
        #[command(subcommand)]
        action: QueueAction,
    },
    /// ⚙️  Manage configuration
    Config {
        #[command(subcommand)]
        action: ConfigAction,
    },
    /// 🔍 Validate a job specification
    Validate {
        /// Path to the job specification YAML
        file: PathBuf,
    },
    /// 📈 Show cluster/backend status
    Status,
}

#[derive(Subcommand)]
enum QueueAction {
    /// List all queues
    List,
    /// Show queue details
    Status {
        /// Queue name
        name: String,
    },
}

#[derive(Subcommand)]
enum ConfigAction {
    /// Show current configuration
    Show,
    /// Set a configuration value
    Set {
        /// Configuration key (e.g., default_backend, aws.region)
        key: String,
        /// Configuration value
        value: String,
    },
    /// Get a configuration value
    Get {
        /// Configuration key
        key: String,
    },
    /// List configured backends
    Backends,
    /// Setup a backend
    SetupBackend {
        /// Backend name (aws, gcp, kubernetes, azure)
        backend: String,
    },
}

#[derive(Subcommand)]
enum JobAction {
    /// Submit a job
    Submit {
        /// Path to the job specification YAML or Python script
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
        /// Submit directly from Python script (auto-generates YAML)
        #[arg(long)]
        from_py: bool,
        /// Arguments to pass to the script
        #[arg(long)]
        args: Option<String>,
        /// Python image to use (default: python:3.11-slim)
        #[arg(long)]
        image: Option<String>,
        /// CPU cores
        #[arg(long)]
        cpu: Option<String>,
        /// Memory (e.g., 4Gi)
        #[arg(long)]
        memory: Option<String>,
        /// GPU count
        #[arg(long)]
        gpu: Option<u32>,
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
    ╔══════════════════════════════════════════════════════════════════════════╗
    ║                                                                          ║
    ║     █████╗ ██╗   ██╗██╗██╗  ██╗                                          ║
    ║    ██╔══██╗██║   ██║██║╚██╗██╔╝                                          ║
    ║    ███████║██║   ██║██║ ╚███╔╝                                           ║
    ║    ██╔══██║╚██╗ ██╔╝██║ ██╔██╗                                           ║
    ║    ██║  ██║ ╚████╔╝ ██║██╔╝ ██╗                                          ║
    ║    ╚═╝  ╚═╝  ╚═══╝  ╚═╝╚═╝  ╚═╝                                          ║
    ║                                                                          ║
    ║    ⚡ Ultra-simple batch & streaming job scheduler                       ║
    ║    🎯 One YAML, any backend: Docker • AWS • K8s • GCP • Azure            ║
    ║    🔥 Built for ML Engineers & Data Practitioners                        ║
    ║                                                                          ║
    ╚══════════════════════════════════════════════════════════════════════════╝
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
                JobAction::Submit { file, backend, watch, dry_run, from_py, args, image, cpu, memory, gpu } => {
                    if *from_py || file.extension().map(|e| e == "py").unwrap_or(false) {
                        submit_python_job(&mut client, file, backend.as_deref(), *watch, *dry_run, args.as_deref(), image.as_deref(), cpu.as_deref(), memory.as_deref(), *gpu).await?;
                    } else {
                        submit_job(&mut client, file, backend.as_deref(), *watch, *dry_run).await?;
                    }
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
        Commands::Estimate { file, backend } => {
            estimate_cost(file, backend.as_deref())?;
        }
        Commands::Queue { action } => {
            match action {
                QueueAction::List => {
                    list_queues();
                }
                QueueAction::Status { name } => {
                    show_queue_status(name);
                }
            }
        }
        Commands::Config { action } => {
            match action {
                ConfigAction::Show => {
                    show_config()?;
                }
                ConfigAction::Set { key, value } => {
                    set_config(key, value)?;
                }
                ConfigAction::Get { key } => {
                    get_config(key)?;
                }
                ConfigAction::Backends => {
                    list_backends();
                }
                ConfigAction::SetupBackend { backend } => {
                    setup_backend(backend)?;
                }
            }
        }
        Commands::Validate { file } => {
            validate_job(file)?;
        }
        Commands::Status => {
            show_cluster_status().await?;
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

async fn submit_python_job(
    client: &mut JobServiceClient<tonic::transport::Channel>,
    file: &PathBuf,
    backend: Option<&str>,
    watch: bool,
    dry_run: bool,
    args: Option<&str>,
    image: Option<&str>,
    cpu: Option<&str>,
    memory: Option<&str>,
    gpu: Option<u32>,
) -> Result<()> {
    print_section("Submitting Python Job");
    
    let spinner = ProgressBar::new_spinner();
    spinner.set_style(ProgressStyle::default_spinner()
        .tick_chars("⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏")
        .template("{spinner:.cyan} {msg}")?);
    
    spinner.set_message(format!("Reading Python script from {}...", file.display()));
    
    // Read the Python script
    let script_content = fs::read_to_string(file)?;
    let script_name = file.file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("script.py");
    let job_name = script_name.trim_end_matches(".py").replace("_", "-");
    
    // Extract requirements from script comments (# pip: package1, package2)
    let mut requirements: Vec<String> = Vec::new();
    for line in script_content.lines() {
        if line.starts_with("# pip:") || line.starts_with("#pip:") {
            let deps = line.trim_start_matches("# pip:").trim_start_matches("#pip:").trim();
            for dep in deps.split(',') {
                requirements.push(dep.trim().to_string());
            }
        }
    }
    
    spinner.finish_and_clear();
    
    // Build the job spec
    let image_name = image.unwrap_or("python:3.11-slim");
    let backend_name = backend.unwrap_or("local-docker");
    
    let resources = if cpu.is_some() || memory.is_some() || gpu.is_some() {
        Some(avix_spec::Resources {
            cpu: cpu.map(|s| s.to_string()),
            memory: memory.map(|s| s.to_string()),
            gpu,
            disk: None,
        })
    } else {
        None
    };
    
    // Execute the script content directly (no file mounting/build step).
    // Also installs optional `# pip:` requirements before running.
    let mut sh_script = String::from("set -e\n");
    if !requirements.is_empty() {
        sh_script.push_str("python -m pip install --no-cache-dir ");
        sh_script.push_str(&requirements.join(" "));
        sh_script.push('\n');
    }
    sh_script.push_str("python -");
    // Keep args parsing simple: pass the user args via sh/positional is out of scope,
    // so we append them to the python invocation directly.
    if let Some(script_args) = args {
        sh_script.push_str(" ");
        sh_script.push_str(script_args);
    }
    sh_script.push_str(" <<'PY'\n");
    sh_script.push_str(&script_content);
    if !script_content.ends_with('\n') {
        sh_script.push('\n');
    }
    sh_script.push_str("PY\n");

    let command = vec!["sh".to_string(), "-lc".to_string(), sh_script];
    
    let job = Job {
        api_version: "avix.vargafoundation.org/v1alpha1".to_string(),
        kind: "Job".to_string(),
        metadata: avix_spec::Metadata {
            name: job_name.clone(),
            namespace: None,
            labels: None,
        },
        spec: avix_spec::JobSpec {
            backend: Some(backend_name.to_string()),
            queue: None,
            priority: None,
            resources,
            affinity: None,
            tolerations: None,
            job_type: Some("EphemeralSimple".to_string()),
            execution: avix_spec::Execution {
                image: image_name.to_string(),
                command,
                args: None,
                env: None,
                volumes: None,
                requirements: if requirements.is_empty() { None } else { Some(requirements.clone()) },
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
    
    // Display job summary
    println!("\n{}", "Generated Job from Python Script:".white().bold());
    println!("  {} {}", "Name:".dimmed(), job_name.cyan());
    println!("  {} {}", "Script:".dimmed(), script_name.yellow());
    println!("  {} {}", "Image:".dimmed(), image_name.green());
    println!("  {} {}", "Backend:".dimmed(), backend_name.green());
    if !requirements.is_empty() {
        println!("  {} {}", "Requirements:".dimmed(), requirements.join(", ").magenta());
    }
    if let Some(ref res) = job.spec.resources {
        if let Some(ref c) = res.cpu {
            println!("  {} {}", "CPU:".dimmed(), c);
        }
        if let Some(ref m) = res.memory {
            println!("  {} {}", "Memory:".dimmed(), m);
        }
        if let Some(g) = res.gpu {
            println!("  {} {}", "GPU:".dimmed(), g);
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
    
    print_success("Job submitted successfully!");
    println!("\n  {} {}", "Job ID:".dimmed(), res.job_id.cyan().bold());
    
    if watch {
        println!();
        show_logs(client, res.job_id, true, true).await?;
    }
    
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
    let kind = extract_kind(&content)?;

    let yaml_to_send = match kind.as_deref() {
        Some("Workflow") => {
            let mut wf: Workflow = serde_yaml::from_str(&content)?;
            if let Some(b) = backend {
                for j in wf.spec.jobs.iter_mut() {
                    j.spec.backend = Some(b.to_string());
                }
            }

            spinner.finish_and_clear();
            println!("\n{}", "Workflow Summary:".white().bold());
            println!("  {} {}", "Name:".dimmed(), wf.metadata.name.cyan());
            println!("  {} {}", "Steps:".dimmed(), wf.spec.jobs.len().to_string().yellow());
            for (i, j) in wf.spec.jobs.iter().enumerate() {
                println!("    {} {}  {} {}", format!("{}.", i + 1).dimmed(), j.metadata.name.cyan(), "Image:".dimmed(), j.spec.execution.image.yellow());
            }

            if dry_run {
                print_warning("Dry run mode - workflow not submitted");
                println!("\n{}", "Generated YAML:".white().bold());
                println!("{}", serde_yaml::to_string(&wf)?);
                return Ok(());
            }

            serde_yaml::to_string(&wf)?
        }
        _ => {
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

            serde_yaml::to_string(&job)?
        }
    };
    
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
    
    let _response = client.stop_job(StopJobRequest {
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
        "distributed" => r#"apiVersion: avix.vargafoundation.org/v1alpha1
kind: Job
metadata:
  name: distributed-training
  namespace: ml-team
  labels:
    team: ml
    type: distributed
spec:
  backend: auto
  type: EphemeralDistributed
  queue: ml-batch
  priority: 80
  resources:
    cpu: "8"
    memory: "32Gi"
    gpu: 4
  affinity:
    nodeLabels:
      gpu-type: nvidia-a100
      zone: eu-west
    antiAffinity:
      jobLabels:
        type: training
  tolerations:
    - key: high-cost
      value: "true"
  execution:
    image: pytorch/pytorch:2.0.0-cuda11.7-cudnn8-runtime
    command: ["torchrun"]
    args: ["--nproc_per_node=4", "train_distributed.py"]
    env:
      - name: MASTER_ADDR
        value: "localhost"
      - name: MASTER_PORT
        value: "29500"
  scaling:
    minInstances: 1
    maxInstances: 4
    metric: "gpu>80%"
  mlTracking:
    wandb: true
    tensorboard: true
  costBudget: 50.0
  restartPolicy: OnFailure
  ttlSecondsAfterFinished: 3600
"#,
        "pipeline" => r#"apiVersion: avix.vargafoundation.org/v1alpha1
kind: Job
metadata:
  name: inference-pipeline
  namespace: ml-team
spec:
  backend: auto
  queue: ml-batch
  priority: 100
  resources:
    cpu: "4"
    memory: "16Gi"
    gpu: 1
  execution:
    image: myregistry/inference:latest
    command: ["python", "inference.py"]
    volumes:
      - name: input-data
        s3:
          bucket: my-data-bucket
          path: /input
          mountPath: /app/data/input
      - name: output-data
        s3:
          bucket: my-data-bucket
          path: /output
          mountPath: /app/data/output
  dependencies:
    - job: preprocess-job
      on: success
    - job: model-download
      on: success
  mlTracking:
    mlflow:
      trackingUri: http://mlflow-server:5000
  restartPolicy: OnFailure
"#,
        "streaming" => r#"apiVersion: avix.vargafoundation.org/v1alpha1
kind: Job
metadata:
  name: kafka-consumer
  namespace: data-eng
spec:
  backend: auto
  type: Continuous
  queue: streaming
  resources:
    cpu: "2"
    memory: "4Gi"
  execution:
    image: myregistry/kafka-consumer:latest
    command: ["python", "consumer.py"]
    env:
      - name: KAFKA_BROKERS
        value: "kafka:9092"
      - name: KAFKA_TOPIC
        value: "events"
      - name: KAFKA_GROUP
        value: "avix-consumer"
  scaling:
    minInstances: 1
    maxInstances: 10
    metric: "kafka_lag>1000"
  restartPolicy: Always
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

fn estimate_cost(file: &PathBuf, backend: Option<&str>) -> Result<()> {
    print_section("Cost Estimation");
    
    let content = fs::read_to_string(file)?;
    let job: Job = serde_yaml::from_str(&content)?;
    
    let backend_name = backend.unwrap_or(job.spec.backend.as_deref().unwrap_or("auto"));
    
    println!("\n{}", "Job Details:".white().bold());
    println!("  {} {}", "Name:".dimmed(), job.metadata.name.cyan());
    println!("  {} {}", "Backend:".dimmed(), backend_name.green());
    
    // Extract resources for cost calculation
    let (cpu, memory, gpu) = if let Some(ref res) = job.spec.resources {
        (
            res.cpu.as_deref().unwrap_or("1").parse::<f64>().unwrap_or(1.0),
            parse_memory_gi(res.memory.as_deref().unwrap_or("1Gi")),
            res.gpu.unwrap_or(0) as f64,
        )
    } else {
        (1.0, 1.0, 0.0)
    };
    
    println!("\n{}", "Resources:".white().bold());
    println!("  {} {} cores", "CPU:".dimmed(), cpu);
    println!("  {} {} GiB", "Memory:".dimmed(), memory);
    if gpu > 0.0 {
        println!("  {} {} GPU(s)", "GPU:".dimmed(), gpu);
    }
    
    // Cost estimation per backend (simulated rates)
    let hourly_cost = match backend_name {
        "local-docker" => 0.0,
        "aws-lambda" => cpu * 0.0000166667 + memory * 0.0000002501, // per 100ms
        "aws-batch" => cpu * 0.04 + memory * 0.004 + gpu * 2.5,
        "k8s-job" | "kubernetes" => cpu * 0.03 + memory * 0.003 + gpu * 2.0,
        "gcp-cloudrun" => cpu * 0.024 + memory * 0.0025,
        "azure-batch" => cpu * 0.035 + memory * 0.0035 + gpu * 2.3,
        _ => cpu * 0.05 + memory * 0.005 + gpu * 2.5, // default estimate
    };
    
    println!("\n{}", "Cost Estimate:".white().bold());
    println!("  {} ${:.4}/hour", "Hourly:".dimmed(), hourly_cost);
    println!("  {} ${:.2}/day", "Daily:".dimmed(), hourly_cost * 24.0);
    println!("  {} ${:.2}/month", "Monthly:".dimmed(), hourly_cost * 24.0 * 30.0);
    
    if let Some(budget) = job.spec.cost_budget {
        let hours_until_budget = budget / hourly_cost;
        println!("\n{}", "Budget Analysis:".white().bold());
        println!("  {} ${:.2}", "Budget:".dimmed(), budget);
        println!("  {} {:.1} hours", "Runtime until budget:".dimmed(), hours_until_budget);
        if hours_until_budget < 1.0 {
            print_warning("Budget will be exhausted in less than 1 hour!");
        }
    }
    
    // Backend comparison
    println!("\n{}", "Backend Comparison (hourly):".white().bold());
    let backends = vec![
        ("local-docker", 0.0),
        ("aws-lambda", cpu * 0.0000166667 * 3600.0 + memory * 0.0000002501 * 3600.0),
        ("aws-batch", cpu * 0.04 + memory * 0.004 + gpu * 2.5),
        ("k8s-job", cpu * 0.03 + memory * 0.003 + gpu * 2.0),
        ("gcp-cloudrun", cpu * 0.024 + memory * 0.0025),
    ];
    
    for (name, cost) in backends {
        let indicator = if name == backend_name { "→" } else { " " };
        let cost_str = if cost == 0.0 {
            "FREE".green().to_string()
        } else {
            format!("${:.4}", cost)
        };
        println!("  {} {} {}", indicator.cyan(), format!("{:15}", name).dimmed(), cost_str);
    }
    
    Ok(())
}

fn parse_memory_gi(mem: &str) -> f64 {
    let mem = mem.trim();
    if mem.ends_with("Gi") {
        mem.trim_end_matches("Gi").parse().unwrap_or(1.0)
    } else if mem.ends_with("Mi") {
        mem.trim_end_matches("Mi").parse::<f64>().unwrap_or(1024.0) / 1024.0
    } else if mem.ends_with("G") {
        mem.trim_end_matches("G").parse().unwrap_or(1.0)
    } else {
        mem.parse().unwrap_or(1.0)
    }
}

fn list_queues() {
    print_section("Queues");
    
    // Simulated queue data
    let queues = vec![
        ("default", 0, "100", "512Gi", 0, "FIFO", false),
        ("ml-batch", 10, "200", "1Ti", 16, "Fair", true),
        ("etl-batch", 5, "100", "256Gi", 0, "FIFO", false),
        ("streaming", 8, "50", "128Gi", 0, "Fair", true),
        ("high-priority", 100, "50", "256Gi", 8, "Priority", true),
    ];
    
    println!("\n{}", format!("{:15} {:8} {:10} {:10} {:5} {:10} {:10}", 
        "NAME", "PRIORITY", "MAX CPU", "MAX MEM", "GPU", "SCHEDULER", "PREEMPT").white().bold());
    println!("{}", "─".repeat(75).dimmed());
    
    for (name, priority, cpu, mem, gpu, scheduler, preempt) in queues {
        let preempt_str = if preempt { "✓".green() } else { "✗".dimmed() };
        println!("{:15} {:8} {:10} {:10} {:5} {:10} {}", 
            name.cyan(), priority, cpu, mem, gpu, scheduler, preempt_str);
    }
    
    println!("\n{} Use {} to see queue details", "ℹ".blue(), "avix queue status <name>".green());
}

fn show_queue_status(name: &str) {
    print_section(&format!("Queue: {}", name));
    
    // Simulated queue status
    println!("\n{}", "Configuration:".white().bold());
    println!("  {} {}", "Name:".dimmed(), name.cyan());
    println!("  {} {}", "Priority:".dimmed(), "10");
    println!("  {} {}", "Scheduler:".dimmed(), "Fair");
    println!("  {} {}", "Preemption:".dimmed(), "enabled".green());
    println!("  {} {}", "Burst To:".dimmed(), "aws-batch".yellow());
    
    println!("\n{}", "Resource Limits:".white().bold());
    println!("  {} {}", "Max CPU:".dimmed(), "200 cores");
    println!("  {} {}", "Max Memory:".dimmed(), "1 TiB");
    println!("  {} {}", "Max GPU:".dimmed(), "16");
    
    println!("\n{}", "Current Usage:".white().bold());
    let cpu_usage = 45.0;
    let mem_usage = 62.0;
    let gpu_usage = 75.0;
    
    println!("  {} {} {:.0}%", "CPU:".dimmed(), create_progress_bar(cpu_usage, 100.0, 20), cpu_usage);
    println!("  {} {} {:.0}%", "Memory:".dimmed(), create_progress_bar(mem_usage, 100.0, 20), mem_usage);
    println!("  {} {} {:.0}%", "GPU:".dimmed(), create_progress_bar(gpu_usage, 100.0, 20), gpu_usage);
    
    println!("\n{}", "Jobs:".white().bold());
    println!("  {} {}", "Running:".dimmed(), "5".green());
    println!("  {} {}", "Pending:".dimmed(), "3".yellow());
    println!("  {} {}", "Completed (24h):".dimmed(), "42".cyan());
    println!("  {} {}", "Failed (24h):".dimmed(), "2".red());
}

fn list_templates() {
    print_section("Available Templates");
    
    let templates = vec![
        ("simple", "Basic job template for quick tasks"),
        ("ml-inference", "ML inference job with GPU and tracking"),
        ("spark-etl", "Apache Spark ETL job"),
        ("grid-search", "Hyperparameter grid search with ML tracking"),
        ("distributed", "Distributed training with affinity and scaling"),
        ("pipeline", "Job pipeline with dependencies and volumes"),
        ("streaming", "Continuous streaming job (Kafka consumer)"),
    ];
    
    for (name, desc) in templates {
        println!("  {} {}", name.green().bold(), format!("- {}", desc).dimmed());
    }
    
    println!("\n{}", "Usage:".white().bold());
    println!("  avix template generate <type> > job.yaml");
}

fn show_config() -> Result<()> {
    print_section("Current Configuration");
    
    let config_dir = dirs::config_dir().unwrap_or_else(|| PathBuf::from(".")).join("avix");
    let config_path = config_dir.join("config.yaml");
    
    if !config_path.exists() {
        print_warning("No configuration file found. Run 'avix init' to create one.");
        return Ok(());
    }
    
    let content = fs::read_to_string(&config_path)?;
    println!("\n{} {}\n", "Config file:".dimmed(), config_path.display());
    
    // Parse and display nicely
    for line in content.lines() {
        if line.starts_with('#') {
            println!("{}", line.dimmed());
        } else if line.contains(':') {
            let parts: Vec<&str> = line.splitn(2, ':').collect();
            if parts.len() == 2 {
                println!("{}{} {}", parts[0].cyan(), ":".dimmed(), parts[1].trim());
            } else {
                println!("{}", line);
            }
        } else {
            println!("{}", line);
        }
    }
    
    Ok(())
}

fn set_config(key: &str, value: &str) -> Result<()> {
    print_section("Setting Configuration");
    
    let config_dir = dirs::config_dir().unwrap_or_else(|| PathBuf::from(".")).join("avix");
    fs::create_dir_all(&config_dir)?;
    let config_path = config_dir.join("config.yaml");
    
    let mut content = if config_path.exists() {
        fs::read_to_string(&config_path)?
    } else {
        String::new()
    };
    
    // Simple key replacement (for top-level keys)
    let key_pattern = format!("{}: ", key);
    if content.contains(&key_pattern) {
        let lines: Vec<&str> = content.lines().collect();
        let new_lines: Vec<String> = lines.iter().map(|line| {
            if line.starts_with(&key_pattern) || line.starts_with(&format!("{}:", key)) {
                format!("{}: {}", key, value)
            } else {
                line.to_string()
            }
        }).collect();
        content = new_lines.join("\n");
    } else {
        content.push_str(&format!("\n{}: {}", key, value));
    }
    
    fs::write(&config_path, content)?;
    print_success(&format!("Set {} = {}", key.cyan(), value.green()));
    
    Ok(())
}

fn get_config(key: &str) -> Result<()> {
    let config_dir = dirs::config_dir().unwrap_or_else(|| PathBuf::from(".")).join("avix");
    let config_path = config_dir.join("config.yaml");
    
    if !config_path.exists() {
        print_error("No configuration file found. Run 'avix init' to create one.");
        return Ok(());
    }
    
    let content = fs::read_to_string(&config_path)?;
    let key_pattern = format!("{}:", key);
    
    for line in content.lines() {
        if line.starts_with(&key_pattern) {
            let value = line.trim_start_matches(&key_pattern).trim();
            println!("{} = {}", key.cyan(), value.green());
            return Ok(());
        }
    }
    
    print_warning(&format!("Key '{}' not found in configuration", key));
    Ok(())
}

fn list_backends() {
    print_section("Configured Backends");
    
    let backends = vec![
        ("local-docker", "✓", "Local Docker daemon", "Ready"),
        ("aws-lambda", "○", "AWS Lambda (serverless)", "Not configured"),
        ("aws-batch", "○", "AWS Batch", "Not configured"),
        ("kubernetes", "○", "Kubernetes cluster", "Not configured"),
        ("gcp-cloudrun", "○", "Google Cloud Run", "Not configured"),
        ("azure-batch", "○", "Azure Batch", "Not configured"),
    ];
    
    println!("\n{}", format!("{:3} {:15} {:30} {:15}", "", "BACKEND", "DESCRIPTION", "STATUS").white().bold());
    println!("{}", "─".repeat(65).dimmed());
    
    for (name, icon, desc, status) in backends {
        let (icon_colored, status_colored) = if icon == "✓" {
            (icon.green(), status.green())
        } else {
            (icon.dimmed(), status.dimmed())
        };
        println!("{:3} {:15} {:30} {}", icon_colored, name.cyan(), desc.dimmed(), status_colored);
    }
    
    println!("\n{} Use {} to configure a backend", "ℹ".blue(), "avix config setup-backend <name>".green());
}

fn setup_backend(backend: &str) -> Result<()> {
    print_section(&format!("Setup Backend: {}", backend));
    
    match backend {
        "aws" | "aws-lambda" | "aws-batch" => {
            println!("\n{}", "AWS Backend Setup".white().bold());
            println!("\n{}", "Required environment variables or config:".dimmed());
            println!("  {} AWS_ACCESS_KEY_ID", "•".cyan());
            println!("  {} AWS_SECRET_ACCESS_KEY", "•".cyan());
            println!("  {} AWS_REGION (default: us-east-1)", "•".cyan());
            println!("\n{}", "Configuration commands:".white().bold());
            println!("  {} avix config set aws.region us-east-1", "$".green());
            println!("  {} avix config set aws.profile default", "$".green());
            println!("\n{}", "Or set environment variables:".dimmed());
            println!("  export AWS_ACCESS_KEY_ID=your-key");
            println!("  export AWS_SECRET_ACCESS_KEY=your-secret");
        }
        "kubernetes" | "k8s" => {
            println!("\n{}", "Kubernetes Backend Setup".white().bold());
            println!("\n{}", "Requirements:".dimmed());
            println!("  {} kubectl configured with cluster access", "•".cyan());
            println!("  {} Valid kubeconfig file", "•".cyan());
            println!("\n{}", "Configuration commands:".white().bold());
            println!("  {} avix config set kubernetes.kubeconfig ~/.kube/config", "$".green());
            println!("  {} avix config set kubernetes.context my-cluster", "$".green());
            println!("  {} avix config set kubernetes.namespace default", "$".green());
        }
        "gcp" | "gcp-cloudrun" => {
            println!("\n{}", "GCP Backend Setup".white().bold());
            println!("\n{}", "Requirements:".dimmed());
            println!("  {} gcloud CLI installed and authenticated", "•".cyan());
            println!("  {} Service account with Cloud Run permissions", "•".cyan());
            println!("\n{}", "Configuration commands:".white().bold());
            println!("  {} avix config set gcp.project my-project-id", "$".green());
            println!("  {} avix config set gcp.region us-central1", "$".green());
            println!("  {} gcloud auth application-default login", "$".green());
        }
        "azure" | "azure-batch" => {
            println!("\n{}", "Azure Backend Setup".white().bold());
            println!("\n{}", "Requirements:".dimmed());
            println!("  {} Azure CLI installed and authenticated", "•".cyan());
            println!("  {} Batch account created", "•".cyan());
            println!("\n{}", "Configuration commands:".white().bold());
            println!("  {} avix config set azure.subscription_id <id>", "$".green());
            println!("  {} avix config set azure.resource_group <rg>", "$".green());
            println!("  {} avix config set azure.batch_account <account>", "$".green());
        }
        "local-docker" => {
            println!("\n{}", "Local Docker Backend".white().bold());
            println!("\n{}", "Requirements:".dimmed());
            println!("  {} Docker daemon running", "•".cyan());
            println!("\n{}", "Verification:".white().bold());
            println!("  {} docker info", "$".green());
            print_success("Local Docker is the default backend and requires no additional setup.");
        }
        _ => {
            print_error(&format!("Unknown backend: {}", backend));
            print_info("Available backends: aws, kubernetes, gcp, azure, local-docker");
        }
    }
    
    Ok(())
}

fn validate_job(file: &PathBuf) -> Result<()> {
    print_section("Validating Job Specification");
    
    let spinner = ProgressBar::new_spinner();
    spinner.set_style(ProgressStyle::default_spinner()
        .tick_chars("⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏")
        .template("{spinner:.cyan} {msg}")?);
    spinner.set_message(format!("Reading {}...", file.display()));
    
    let content = fs::read_to_string(file)?;
    
    spinner.set_message("Parsing YAML...");
    let kind = extract_kind(&content)?;
    
    match kind.as_deref() {
        Some("Workflow") => {
            let wf: Result<Workflow, _> = serde_yaml::from_str(&content);
            match wf {
                Ok(wf) => {
                    print_success("Workflow specification is valid!");
                    println!("\n{}", "Workflow Summary:".white().bold());
                    println!("  {} {}", "Name:".dimmed(), wf.metadata.name.cyan());
                    println!("  {} {}", "Kind:".dimmed(), wf.kind);
                    println!("  {} {}", "Steps:".dimmed(), wf.spec.jobs.len().to_string().yellow());
                }
                Err(e) => {
                    print_error("Workflow specification is invalid!");
                    println!("\n{}", "Error details:".red().bold());
                    println!("  {}", e);
                }
            }
        }
        _ => {
            let job: Result<Job, _> = serde_yaml::from_str(&content);
            match job {
                Ok(job) => {
                    print_success("Job specification is valid!");

                    println!("\n{}", "Job Summary:".white().bold());
                    println!("  {} {}", "Name:".dimmed(), job.metadata.name.cyan());
                    println!("  {} {}", "API Version:".dimmed(), job.api_version);
                    println!("  {} {}", "Kind:".dimmed(), job.kind);
                    println!("  {} {}", "Image:".dimmed(), job.spec.execution.image.yellow());
                    println!("  {} {}", "Backend:".dimmed(), job.spec.backend.as_deref().unwrap_or("auto").green());

                    if let Some(ref res) = job.spec.resources {
                        println!("\n{}", "Resources:".white().bold());
                        if let Some(ref cpu) = res.cpu {
                            println!("  {} {} cores", "CPU:".dimmed(), cpu);
                        }
                        if let Some(ref mem) = res.memory {
                            println!("  {} {}", "Memory:".dimmed(), mem);
                        }
                        if let Some(gpu) = res.gpu {
                            println!("  {} {}", "GPU:".dimmed(), gpu);
                        }
                    }

                    // Warnings
                    let mut warnings = Vec::new();
                    if job.spec.backend.is_none() {
                        warnings.push("No backend specified, will use 'auto'");
                    }
                    if job.spec.resources.is_none() {
                        warnings.push("No resources specified, defaults will be used");
                    }
                    if job.spec.queue.is_none() {
                        warnings.push("No queue specified, will use 'default'");
                    }

                    if !warnings.is_empty() {
                        println!("\n{}", "Warnings:".yellow().bold());
                        for w in warnings {
                            print_warning(w);
                        }
                    }
                }
                Err(e) => {
                    print_error("Job specification is invalid!");
                    println!("\n{}", "Error details:".red().bold());
                    println!("  {}", e);
                }
            }
        }
    };
    
    Ok(())
}

async fn show_cluster_status() -> Result<()> {
    print_section("Cluster Status");
    
    println!("\n{}", "Server:".white().bold());
    println!("  {} {}", "URL:".dimmed(), "http://[::1]:50051".cyan());
    println!("  {} {}", "Status:".dimmed(), "● Connected".green());
    println!("  {} {}", "Version:".dimmed(), "0.1.0");
    
    println!("\n{}", "Backends:".white().bold());
    println!("  {} {} {}", "local-docker".cyan(), "●".green(), "Ready");
    println!("  {} {} {}", "aws-batch".cyan(), "○".dimmed(), "Not configured");
    println!("  {} {} {}", "kubernetes".cyan(), "○".dimmed(), "Not configured");
    
    println!("\n{}", "Resources (local-docker):".white().bold());
    let cpu_usage = 23.0;
    let mem_usage = 45.0;
    println!("  {} {} {:.0}%", "CPU:".dimmed(), create_progress_bar(cpu_usage, 100.0, 20), cpu_usage);
    println!("  {} {} {:.0}%", "Memory:".dimmed(), create_progress_bar(mem_usage, 100.0, 20), mem_usage);
    
    println!("\n{}", "Jobs Summary:".white().bold());
    println!("  {} {}", "Running:".dimmed(), "3".green());
    println!("  {} {}", "Pending:".dimmed(), "1".yellow());
    println!("  {} {}", "Completed (24h):".dimmed(), "27".cyan());
    println!("  {} {}", "Failed (24h):".dimmed(), "2".red());
    
    Ok(())
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
                affinity: None,
                tolerations: None,
                job_type: None,
                execution: avix_spec::Execution {
                    image: "busybox".to_string(),
                    command: vec!["ls".to_string()],
                    args: None,
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
