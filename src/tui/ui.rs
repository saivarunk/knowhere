use ratatui::{
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Cell, Clear, Paragraph, Row, Table, Wrap},
    Frame,
};

use super::app::{App, Focus, Mode};

pub fn draw(frame: &mut Frame, app: &App) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(5),  // Query editor
            Constraint::Min(10),    // Results
            Constraint::Length(1),  // Status bar
        ])
        .split(frame.area());

    draw_query_editor(frame, app, chunks[0]);
    draw_results(frame, app, chunks[1]);
    draw_status_bar(frame, app, chunks[2]);

    // Draw command line if in command mode
    if app.mode == Mode::Command {
        draw_command_line(frame, app);
    }
}

fn draw_query_editor(frame: &mut Frame, app: &App, area: Rect) {
    let is_focused = app.focus == Focus::Query;
    let border_color = if is_focused {
        Color::Cyan
    } else {
        Color::DarkGray
    };

    let block = Block::default()
        .title(" SQL Query (i: insert, Enter: execute) ")
        .borders(Borders::ALL)
        .border_style(Style::default().fg(border_color));

    let inner = block.inner(area);
    frame.render_widget(block, area);

    // Syntax highlighting for SQL
    let highlighted = highlight_sql(&app.query);
    let paragraph = Paragraph::new(highlighted)
        .wrap(Wrap { trim: false });

    frame.render_widget(paragraph, inner);

    // Show cursor in insert mode
    if app.mode == Mode::Insert && is_focused {
        let cursor_x = inner.x + (app.cursor_pos as u16 % inner.width);
        let cursor_y = inner.y + (app.cursor_pos as u16 / inner.width);
        frame.set_cursor_position((cursor_x, cursor_y));
    }
}

fn highlight_sql(query: &str) -> Line<'static> {
    let keywords = [
        "SELECT", "FROM", "WHERE", "AND", "OR", "NOT", "JOIN", "INNER", "LEFT", "RIGHT",
        "OUTER", "ON", "GROUP", "BY", "HAVING", "ORDER", "ASC", "DESC", "LIMIT", "OFFSET",
        "AS", "DISTINCT", "COUNT", "SUM", "AVG", "MIN", "MAX", "NULL", "IS", "IN", "LIKE",
        "BETWEEN", "CASE", "WHEN", "THEN", "ELSE", "END", "TRUE", "FALSE", "CROSS",
    ];

    let mut spans = Vec::new();
    let mut current = String::new();
    let mut in_string = false;
    let mut string_char = ' ';

    for c in query.chars() {
        if in_string {
            current.push(c);
            if c == string_char {
                spans.push(Span::styled(
                    current.clone(),
                    Style::default().fg(Color::Green),
                ));
                current.clear();
                in_string = false;
            }
        } else if c == '\'' || c == '"' {
            if !current.is_empty() {
                spans.push(colorize_word(&current, &keywords));
                current.clear();
            }
            current.push(c);
            in_string = true;
            string_char = c;
        } else if c.is_alphanumeric() || c == '_' {
            current.push(c);
        } else {
            if !current.is_empty() {
                spans.push(colorize_word(&current, &keywords));
                current.clear();
            }
            // Operators
            let style = match c {
                '(' | ')' | ',' => Style::default().fg(Color::Yellow),
                '=' | '<' | '>' | '!' => Style::default().fg(Color::Magenta),
                '+' | '-' | '*' | '/' | '%' => Style::default().fg(Color::Magenta),
                _ => Style::default(),
            };
            spans.push(Span::styled(c.to_string(), style));
        }
    }

    if !current.is_empty() {
        if in_string {
            spans.push(Span::styled(current, Style::default().fg(Color::Green)));
        } else {
            spans.push(colorize_word(&current, &keywords));
        }
    }

    Line::from(spans)
}

fn colorize_word(word: &str, keywords: &[&str]) -> Span<'static> {
    let upper = word.to_uppercase();
    if keywords.contains(&upper.as_str()) {
        Span::styled(
            word.to_string(),
            Style::default()
                .fg(Color::Blue)
                .add_modifier(Modifier::BOLD),
        )
    } else if word.chars().all(|c| c.is_ascii_digit() || c == '.') {
        Span::styled(word.to_string(), Style::default().fg(Color::Cyan))
    } else {
        Span::styled(word.to_string(), Style::default())
    }
}

fn draw_results(frame: &mut Frame, app: &App, area: Rect) {
    let is_focused = app.focus == Focus::Results;
    let border_color = if is_focused {
        Color::Cyan
    } else {
        Color::DarkGray
    };

    let title = if let Some(ref table) = app.result {
        format!(" Results ({} rows) ", table.row_count())
    } else if let Some(ref error) = app.error {
        format!(" Error: {} ", error)
    } else {
        " Results ".to_string()
    };

    let block = Block::default()
        .title(title)
        .borders(Borders::ALL)
        .border_style(Style::default().fg(border_color));

    let inner = block.inner(area);
    frame.render_widget(block, area);

    if let Some(ref error) = app.error {
        let error_text = Paragraph::new(error.as_str())
            .style(Style::default().fg(Color::Red))
            .wrap(Wrap { trim: true });
        frame.render_widget(error_text, inner);
        return;
    }

    if let Some(ref table) = app.result {
        if table.row_count() == 0 {
            let empty = Paragraph::new("No results");
            frame.render_widget(empty, inner);
            return;
        }

        // Build header
        let header_cells: Vec<Cell> = table
            .schema
            .columns
            .iter()
            .enumerate()
            .skip(app.result_horizontal_scroll)
            .map(|(i, col)| {
                let width = app.column_widths.get(i).copied().unwrap_or(10);
                Cell::from(truncate_string(&col.name, width))
                    .style(Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD))
            })
            .collect();

        let header = Row::new(header_cells).height(1);

        // Build rows
        let visible_height = inner.height.saturating_sub(2) as usize;
        let rows: Vec<Row> = table
            .rows
            .iter()
            .skip(app.result_scroll)
            .take(visible_height)
            .map(|row| {
                let cells: Vec<Cell> = row
                    .values
                    .iter()
                    .enumerate()
                    .skip(app.result_horizontal_scroll)
                    .map(|(i, val)| {
                        let width = app.column_widths.get(i).copied().unwrap_or(10);
                        let s = val.to_string();
                        Cell::from(truncate_string(&s, width))
                    })
                    .collect();
                Row::new(cells)
            })
            .collect();

        // Calculate column widths for display
        let widths: Vec<Constraint> = app
            .column_widths
            .iter()
            .skip(app.result_horizontal_scroll)
            .map(|&w| Constraint::Length(w as u16 + 2))
            .collect();

        let table_widget = Table::new(rows, &widths)
            .header(header)
            .highlight_style(Style::default().add_modifier(Modifier::REVERSED));

        frame.render_widget(table_widget, inner);
    } else {
        let help = Paragraph::new("Enter a SQL query and press Enter to execute")
            .style(Style::default().fg(Color::DarkGray));
        frame.render_widget(help, inner);
    }
}

fn truncate_string(s: &str, max_len: usize) -> String {
    if s.len() <= max_len {
        s.to_string()
    } else if max_len > 3 {
        format!("{}...", &s[..max_len - 3])
    } else {
        s[..max_len].to_string()
    }
}

fn draw_status_bar(frame: &mut Frame, app: &App, area: Rect) {
    let mode_str = match app.mode {
        Mode::Normal => "NORMAL",
        Mode::Insert => "INSERT",
        Mode::Command => "COMMAND",
    };

    let mode_color = match app.mode {
        Mode::Normal => Color::Blue,
        Mode::Insert => Color::Green,
        Mode::Command => Color::Yellow,
    };

    let focus_str = match app.focus {
        Focus::Query => "Query",
        Focus::Results => "Results",
    };

    let help = match app.mode {
        Mode::Normal => "i:insert  j/k:scroll  Tab:focus  ::command  q:quit",
        Mode::Insert => "Esc:normal  Enter:execute  Ctrl+C:cancel",
        Mode::Command => "Enter:execute  Esc:cancel",
    };

    let status = Line::from(vec![
        Span::styled(
            format!(" {} ", mode_str),
            Style::default().fg(Color::Black).bg(mode_color),
        ),
        Span::raw(" "),
        Span::styled(
            format!("[{}]", focus_str),
            Style::default().fg(Color::DarkGray),
        ),
        Span::raw(" "),
        Span::styled(help, Style::default().fg(Color::DarkGray)),
    ]);

    let paragraph = Paragraph::new(status);
    frame.render_widget(paragraph, area);
}

fn draw_command_line(frame: &mut Frame, app: &App) {
    let area = frame.area();
    let popup_area = Rect {
        x: 0,
        y: area.height - 1,
        width: area.width,
        height: 1,
    };

    frame.render_widget(Clear, popup_area);

    let command_line = Paragraph::new(format!(":{}", app.command_buffer))
        .style(Style::default().fg(Color::White));

    frame.render_widget(command_line, popup_area);

    // Position cursor
    frame.set_cursor_position((1 + app.command_buffer.len() as u16, popup_area.y));
}
