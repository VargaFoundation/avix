use anyhow::Result;
use crossterm::{
    event::{self, DisableMouseCapture, EnableMouseCapture, Event, KeyCode, KeyEventKind},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use ratatui::{
    backend::CrosstermBackend,
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    symbols,
    text::{Line, Span},
    widgets::{
        Axis, Block, Borders, Cell, Chart, Dataset, Gauge, List, ListItem, Paragraph, Row,
        Sparkline, Table, Tabs, Wrap,
    },
    Frame, Terminal,
};
use std::io;
use std::time::{Duration, Instant};

#[derive(Clone)]
struct JobInfo {
    id: String,
    name: String,
    status: String,
    backend: String,
    cpu: f64,
    memory: f64,
}

struct App {
    selected_tab: usize,
    jobs: Vec<JobInfo>,
    selected_job: usize,
    logs: Vec<String>,
    cpu_history: Vec<u64>,
    mem_history: Vec<u64>,
    should_quit: bool,
    tick_count: u64,
}

impl App {
    fn new() -> Self {
        // Demo data
        let jobs = vec![
            JobInfo {
                id: "abc123def456".to_string(),
                name: "ml-inference-v2".to_string(),
                status: "running".to_string(),
                backend: "local-docker".to_string(),
                cpu: 45.0,
                memory: 62.0,
            },
            JobInfo {
                id: "xyz789ghi012".to_string(),
                name: "spark-etl-daily".to_string(),
                status: "pending".to_string(),
                backend: "local-docker".to_string(),
                cpu: 0.0,
                memory: 0.0,
            },
            JobInfo {
                id: "mno345pqr678".to_string(),
                name: "grid-search-lr".to_string(),
                status: "completed".to_string(),
                backend: "local-docker".to_string(),
                cpu: 0.0,
                memory: 0.0,
            },
            JobInfo {
                id: "stu901vwx234".to_string(),
                name: "data-preprocess".to_string(),
                status: "failed".to_string(),
                backend: "local-docker".to_string(),
                cpu: 0.0,
                memory: 0.0,
            },
        ];

        let logs = vec![
            "[INFO] 2026-01-27 13:45:01 - Starting job ml-inference-v2".to_string(),
            "[INFO] 2026-01-27 13:45:02 - Loading model from /models/v2.pt".to_string(),
            "[INFO] 2026-01-27 13:45:05 - Model loaded successfully (2.3GB)".to_string(),
            "[INFO] 2026-01-27 13:45:06 - Processing batch 1/100".to_string(),
            "[INFO] 2026-01-27 13:45:08 - Processing batch 2/100".to_string(),
            "[WARN] 2026-01-27 13:45:09 - GPU memory usage high: 85%".to_string(),
            "[INFO] 2026-01-27 13:45:10 - Processing batch 3/100".to_string(),
            "[INFO] 2026-01-27 13:45:12 - Processing batch 4/100".to_string(),
        ];

        Self {
            selected_tab: 0,
            jobs,
            selected_job: 0,
            logs,
            cpu_history: vec![20, 35, 45, 50, 42, 38, 55, 60, 45, 40, 35, 45, 50, 55, 48],
            mem_history: vec![50, 52, 55, 58, 60, 62, 65, 63, 62, 64, 66, 68, 65, 62, 62],
            should_quit: false,
            tick_count: 0,
        }
    }

    fn on_tick(&mut self) {
        self.tick_count += 1;
        
        // Simulate live metrics
        if !self.jobs.is_empty() && self.jobs[0].status == "running" {
            let new_cpu = (45.0 + (self.tick_count as f64 * 0.5).sin() * 20.0) as u64;
            let new_mem = (62.0 + (self.tick_count as f64 * 0.3).cos() * 10.0) as u64;
            
            self.cpu_history.push(new_cpu);
            self.mem_history.push(new_mem);
            
            if self.cpu_history.len() > 60 {
                self.cpu_history.remove(0);
            }
            if self.mem_history.len() > 60 {
                self.mem_history.remove(0);
            }
            
            self.jobs[0].cpu = new_cpu as f64;
            self.jobs[0].memory = new_mem as f64;
        }
        
        // Simulate new log entries
        if self.tick_count % 3 == 0 {
            let batch = 4 + (self.tick_count / 3) as usize;
            if batch <= 100 {
                self.logs.push(format!(
                    "[INFO] 2026-01-27 13:45:{:02} - Processing batch {}/100",
                    12 + self.tick_count,
                    batch
                ));
                if self.logs.len() > 50 {
                    self.logs.remove(0);
                }
            }
        }
    }

    fn next_tab(&mut self) {
        self.selected_tab = (self.selected_tab + 1) % 3;
    }

    fn prev_tab(&mut self) {
        if self.selected_tab > 0 {
            self.selected_tab -= 1;
        } else {
            self.selected_tab = 2;
        }
    }

    fn next_job(&mut self) {
        if !self.jobs.is_empty() {
            self.selected_job = (self.selected_job + 1) % self.jobs.len();
        }
    }

    fn prev_job(&mut self) {
        if !self.jobs.is_empty() {
            if self.selected_job > 0 {
                self.selected_job -= 1;
            } else {
                self.selected_job = self.jobs.len() - 1;
            }
        }
    }
}

pub async fn run_dashboard() -> Result<()> {
    // Setup terminal
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen, EnableMouseCapture)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    let mut app = App::new();
    let tick_rate = Duration::from_millis(250);
    let mut last_tick = Instant::now();

    loop {
        terminal.draw(|f| ui(f, &app))?;

        let timeout = tick_rate
            .checked_sub(last_tick.elapsed())
            .unwrap_or_else(|| Duration::from_secs(0));

        if crossterm::event::poll(timeout)? {
            if let Event::Key(key) = event::read()? {
                if key.kind == KeyEventKind::Press {
                    match key.code {
                        KeyCode::Char('q') => app.should_quit = true,
                        KeyCode::Tab => app.next_tab(),
                        KeyCode::BackTab => app.prev_tab(),
                        KeyCode::Down | KeyCode::Char('j') => app.next_job(),
                        KeyCode::Up | KeyCode::Char('k') => app.prev_job(),
                        KeyCode::Char('1') => app.selected_tab = 0,
                        KeyCode::Char('2') => app.selected_tab = 1,
                        KeyCode::Char('3') => app.selected_tab = 2,
                        _ => {}
                    }
                }
            }
        }

        if last_tick.elapsed() >= tick_rate {
            app.on_tick();
            last_tick = Instant::now();
        }

        if app.should_quit {
            break;
        }
    }

    // Restore terminal
    disable_raw_mode()?;
    execute!(
        terminal.backend_mut(),
        LeaveAlternateScreen,
        DisableMouseCapture
    )?;
    terminal.show_cursor()?;

    Ok(())
}

fn ui(f: &mut Frame, app: &App) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),
            Constraint::Length(3),
            Constraint::Min(0),
            Constraint::Length(3),
        ])
        .split(f.area());

    // Header
    let header = Paragraph::new(Line::from(vec![
        Span::styled("  🚀 ", Style::default()),
        Span::styled(
            "AVIX DASHBOARD",
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        ),
        Span::styled(
            "  │  Ultra-simple batch & streaming job scheduler",
            Style::default().fg(Color::DarkGray),
        ),
    ]))
    .block(
        Block::default()
            .borders(Borders::ALL)
            .border_style(Style::default().fg(Color::Cyan)),
    );
    f.render_widget(header, chunks[0]);

    // Tabs
    let tab_titles = vec!["📦 Jobs", "📊 Metrics", "📜 Logs"];
    let tabs = Tabs::new(tab_titles)
        .block(Block::default().borders(Borders::ALL).title(" Navigation "))
        .select(app.selected_tab)
        .style(Style::default().fg(Color::White))
        .highlight_style(
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        );
    f.render_widget(tabs, chunks[1]);

    // Main content based on selected tab
    match app.selected_tab {
        0 => render_jobs_tab(f, app, chunks[2]),
        1 => render_metrics_tab(f, app, chunks[2]),
        2 => render_logs_tab(f, app, chunks[2]),
        _ => {}
    }

    // Footer
    let footer = Paragraph::new(Line::from(vec![
        Span::styled(" q", Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD)),
        Span::raw(" quit  "),
        Span::styled("Tab", Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD)),
        Span::raw(" switch tab  "),
        Span::styled("↑↓", Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD)),
        Span::raw(" navigate  "),
        Span::styled("1-3", Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD)),
        Span::raw(" jump to tab"),
    ]))
    .block(
        Block::default()
            .borders(Borders::ALL)
            .border_style(Style::default().fg(Color::DarkGray)),
    );
    f.render_widget(footer, chunks[3]);
}

fn render_jobs_tab(f: &mut Frame, app: &App, area: Rect) {
    let chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(60), Constraint::Percentage(40)])
        .split(area);

    // Jobs table
    let header_cells = ["ID", "NAME", "STATUS", "BACKEND"]
        .iter()
        .map(|h| Cell::from(*h).style(Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD)));
    let header = Row::new(header_cells).height(1);

    let rows = app.jobs.iter().enumerate().map(|(i, job)| {
        let status_style = match job.status.as_str() {
            "running" => Style::default().fg(Color::Green),
            "pending" => Style::default().fg(Color::Yellow),
            "completed" => Style::default().fg(Color::Cyan),
            "failed" => Style::default().fg(Color::Red),
            _ => Style::default(),
        };

        let status_icon = match job.status.as_str() {
            "running" => "● ",
            "pending" => "◐ ",
            "completed" => "✓ ",
            "failed" => "✗ ",
            _ => "○ ",
        };

        let cells = vec![
            Cell::from(job.id.chars().take(12).collect::<String>()),
            Cell::from(job.name.clone()),
            Cell::from(format!("{}{}", status_icon, job.status.to_uppercase())).style(status_style),
            Cell::from(job.backend.clone()),
        ];

        let row = Row::new(cells);
        if i == app.selected_job {
            row.style(Style::default().bg(Color::DarkGray))
        } else {
            row
        }
    });

    let table = Table::new(
        rows,
        [
            Constraint::Length(14),
            Constraint::Min(20),
            Constraint::Length(14),
            Constraint::Length(14),
        ],
    )
    .header(header)
    .block(
        Block::default()
            .borders(Borders::ALL)
            .title(" 📦 Jobs ")
            .border_style(Style::default().fg(Color::Cyan)),
    );
    f.render_widget(table, chunks[0]);

    // Job details
    if let Some(job) = app.jobs.get(app.selected_job) {
        let details_chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Length(8), Constraint::Min(0)])
            .split(chunks[1]);

        let details = vec![
            Line::from(vec![
                Span::styled("ID: ", Style::default().fg(Color::DarkGray)),
                Span::styled(&job.id, Style::default().fg(Color::White)),
            ]),
            Line::from(vec![
                Span::styled("Name: ", Style::default().fg(Color::DarkGray)),
                Span::styled(&job.name, Style::default().fg(Color::Cyan)),
            ]),
            Line::from(vec![
                Span::styled("Status: ", Style::default().fg(Color::DarkGray)),
                Span::styled(
                    &job.status.to_uppercase(),
                    match job.status.as_str() {
                        "running" => Style::default().fg(Color::Green),
                        "pending" => Style::default().fg(Color::Yellow),
                        "completed" => Style::default().fg(Color::Cyan),
                        "failed" => Style::default().fg(Color::Red),
                        _ => Style::default(),
                    },
                ),
            ]),
            Line::from(vec![
                Span::styled("Backend: ", Style::default().fg(Color::DarkGray)),
                Span::styled(&job.backend, Style::default().fg(Color::White)),
            ]),
        ];

        let details_widget = Paragraph::new(details).block(
            Block::default()
                .borders(Borders::ALL)
                .title(" Job Details ")
                .border_style(Style::default().fg(Color::Magenta)),
        );
        f.render_widget(details_widget, details_chunks[0]);

        // Quick metrics for selected job
        if job.status == "running" {
            let gauges_chunks = Layout::default()
                .direction(Direction::Vertical)
                .constraints([Constraint::Length(3), Constraint::Length(3), Constraint::Min(0)])
                .split(details_chunks[1]);

            let cpu_gauge = Gauge::default()
                .block(Block::default().title(" CPU ").borders(Borders::ALL))
                .gauge_style(
                    Style::default()
                        .fg(if job.cpu > 80.0 {
                            Color::Red
                        } else if job.cpu > 50.0 {
                            Color::Yellow
                        } else {
                            Color::Green
                        })
                        .bg(Color::Black),
                )
                .percent(job.cpu as u16)
                .label(format!("{:.1}%", job.cpu));
            f.render_widget(cpu_gauge, gauges_chunks[0]);

            let mem_gauge = Gauge::default()
                .block(Block::default().title(" Memory ").borders(Borders::ALL))
                .gauge_style(
                    Style::default()
                        .fg(if job.memory > 80.0 {
                            Color::Red
                        } else if job.memory > 50.0 {
                            Color::Yellow
                        } else {
                            Color::Green
                        })
                        .bg(Color::Black),
                )
                .percent(job.memory as u16)
                .label(format!("{:.1}%", job.memory));
            f.render_widget(mem_gauge, gauges_chunks[1]);
        }
    }
}

fn render_metrics_tab(f: &mut Frame, app: &App, area: Rect) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
        .split(area);

    // CPU Sparkline
    let cpu_sparkline = Sparkline::default()
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title(format!(" 🔥 CPU Usage (current: {}%) ", app.cpu_history.last().unwrap_or(&0)))
                .border_style(Style::default().fg(Color::Green)),
        )
        .data(&app.cpu_history)
        .max(100)
        .style(Style::default().fg(Color::Green));
    f.render_widget(cpu_sparkline, chunks[0]);

    // Memory Sparkline
    let mem_sparkline = Sparkline::default()
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title(format!(" 💾 Memory Usage (current: {}%) ", app.mem_history.last().unwrap_or(&0)))
                .border_style(Style::default().fg(Color::Magenta)),
        )
        .data(&app.mem_history)
        .max(100)
        .style(Style::default().fg(Color::Magenta));
    f.render_widget(mem_sparkline, chunks[1]);
}

fn render_logs_tab(f: &mut Frame, app: &App, area: Rect) {
    let log_items: Vec<ListItem> = app
        .logs
        .iter()
        .map(|log| {
            let style = if log.contains("[ERROR]") {
                Style::default().fg(Color::Red)
            } else if log.contains("[WARN]") {
                Style::default().fg(Color::Yellow)
            } else if log.contains("[INFO]") {
                Style::default().fg(Color::Cyan)
            } else if log.contains("[DEBUG]") {
                Style::default().fg(Color::DarkGray)
            } else {
                Style::default()
            };
            ListItem::new(Line::from(Span::styled(log.clone(), style)))
        })
        .collect();

    let logs_list = List::new(log_items)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title(" 📜 Live Logs (auto-refresh) ")
                .border_style(Style::default().fg(Color::Yellow)),
        )
        .style(Style::default().fg(Color::White));
    f.render_widget(logs_list, area);
}
