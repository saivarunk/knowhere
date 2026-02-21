use crate::datafusion::DataFusionContext;
use crate::storage::table::Table;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Mode {
    Normal,
    Insert,
    Command,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Focus {
    Query,
    Results,
}

pub struct App {
    pub query: String,
    pub cursor_pos: usize,
    pub result: Option<Table>,
    pub error: Option<String>,
    pub mode: Mode,
    pub focus: Focus,
    pub should_quit: bool,
    pub ctx: DataFusionContext,
    pub command_buffer: String,
    pub result_scroll: usize,
    pub result_horizontal_scroll: usize,
    pub history: Vec<String>,
    pub history_index: Option<usize>,
    pub column_widths: Vec<usize>,
}

impl App {
    pub fn new(ctx: DataFusionContext) -> Self {
        Self {
            query: String::new(),
            cursor_pos: 0,
            result: None,
            error: None,
            mode: Mode::Normal,
            focus: Focus::Query,
            should_quit: false,
            ctx,
            command_buffer: String::new(),
            result_scroll: 0,
            result_horizontal_scroll: 0,
            history: Vec::new(),
            history_index: None,
            column_widths: Vec::new(),
        }
    }

    pub fn execute_query(&mut self) {
        if self.query.trim().is_empty() {
            return;
        }

        // Add to history
        if self.history.last() != Some(&self.query) {
            self.history.push(self.query.clone());
        }
        self.history_index = None;

        match self.ctx.execute_sql(&self.query) {
            Ok(table) => {
                self.calculate_column_widths(&table);
                self.result = Some(table);
                self.error = None;
                self.result_scroll = 0;
                self.result_horizontal_scroll = 0;
            }
            Err(e) => {
                self.error = Some(e.to_string());
                self.result = None;
            }
        }
    }

    fn calculate_column_widths(&mut self, table: &Table) {
        self.column_widths = table
            .schema
            .columns
            .iter()
            .enumerate()
            .map(|(i, col)| {
                let header_width = col.name.len();
                let max_value_width = table
                    .rows
                    .iter()
                    .map(|row| row.values.get(i).map(|v| v.to_string().len()).unwrap_or(0))
                    .max()
                    .unwrap_or(0);
                header_width.max(max_value_width).max(4) // minimum width of 4
            })
            .collect();
    }

    pub fn insert_char(&mut self, c: char) {
        self.query.insert(self.cursor_pos, c);
        self.cursor_pos += 1;
    }

    pub fn delete_char(&mut self) {
        if self.cursor_pos > 0 {
            self.cursor_pos -= 1;
            self.query.remove(self.cursor_pos);
        }
    }

    pub fn delete_char_forward(&mut self) {
        if self.cursor_pos < self.query.len() {
            self.query.remove(self.cursor_pos);
        }
    }

    pub fn move_cursor_left(&mut self) {
        if self.cursor_pos > 0 {
            self.cursor_pos -= 1;
        }
    }

    pub fn move_cursor_right(&mut self) {
        if self.cursor_pos < self.query.len() {
            self.cursor_pos += 1;
        }
    }

    pub fn move_cursor_start(&mut self) {
        self.cursor_pos = 0;
    }

    pub fn move_cursor_end(&mut self) {
        self.cursor_pos = self.query.len();
    }

    pub fn move_cursor_word_forward(&mut self) {
        let chars: Vec<char> = self.query.chars().collect();
        let mut pos = self.cursor_pos;

        // Skip current word
        while pos < chars.len() && !chars[pos].is_whitespace() {
            pos += 1;
        }
        // Skip whitespace
        while pos < chars.len() && chars[pos].is_whitespace() {
            pos += 1;
        }

        self.cursor_pos = pos;
    }

    pub fn move_cursor_word_backward(&mut self) {
        let chars: Vec<char> = self.query.chars().collect();
        let mut pos = self.cursor_pos;

        if pos > 0 {
            pos -= 1;
        }

        // Skip whitespace
        while pos > 0 && chars[pos].is_whitespace() {
            pos -= 1;
        }
        // Skip current word
        while pos > 0 && !chars[pos - 1].is_whitespace() {
            pos -= 1;
        }

        self.cursor_pos = pos;
    }

    pub fn move_cursor_up(&mut self) {
        // Find the start of the current line
        let before_cursor = &self.query[..self.cursor_pos];
        let current_line_start = before_cursor.rfind('\n').map(|i| i + 1).unwrap_or(0);

        // If we're on the first line, do nothing
        if current_line_start == 0 {
            return;
        }

        // Column position within current line
        let col = self.cursor_pos - current_line_start;

        // Find the start of the previous line
        let prev_line_end = current_line_start - 1; // position of '\n'
        let prev_line_start = self.query[..prev_line_end]
            .rfind('\n')
            .map(|i| i + 1)
            .unwrap_or(0);
        let prev_line_len = prev_line_end - prev_line_start;

        // Move to the same column on the previous line, or end of line if shorter
        self.cursor_pos = prev_line_start + col.min(prev_line_len);
    }

    pub fn move_cursor_down(&mut self) {
        // Find the start of the current line
        let before_cursor = &self.query[..self.cursor_pos];
        let current_line_start = before_cursor.rfind('\n').map(|i| i + 1).unwrap_or(0);

        // Column position within current line
        let col = self.cursor_pos - current_line_start;

        // Find the end of the current line (position of '\n' or end of string)
        let current_line_end = self.query[self.cursor_pos..]
            .find('\n')
            .map(|i| self.cursor_pos + i)
            .unwrap_or(self.query.len());

        // If we're on the last line, do nothing
        if current_line_end == self.query.len() {
            return;
        }

        // Next line starts after the '\n'
        let next_line_start = current_line_end + 1;
        let next_line_end = self.query[next_line_start..]
            .find('\n')
            .map(|i| next_line_start + i)
            .unwrap_or(self.query.len());
        let next_line_len = next_line_end - next_line_start;

        // Move to the same column on the next line, or end of line if shorter
        self.cursor_pos = next_line_start + col.min(next_line_len);
    }

    pub fn delete_word_backward(&mut self) {
        let start = self.cursor_pos;
        self.move_cursor_word_backward();
        let end = self.cursor_pos;
        self.query.drain(end..start);
    }

    pub fn delete_to_end(&mut self) {
        self.query.truncate(self.cursor_pos);
    }

    pub fn delete_to_start(&mut self) {
        self.query = self.query[self.cursor_pos..].to_string();
        self.cursor_pos = 0;
    }

    pub fn clear_query(&mut self) {
        self.query.clear();
        self.cursor_pos = 0;
    }

    pub fn history_up(&mut self) {
        if self.history.is_empty() {
            return;
        }

        let new_index = match self.history_index {
            None => self.history.len() - 1,
            Some(0) => 0,
            Some(i) => i - 1,
        };

        self.history_index = Some(new_index);
        self.query = self.history[new_index].clone();
        self.cursor_pos = self.query.len();
    }

    pub fn history_down(&mut self) {
        if self.history.is_empty() {
            return;
        }

        match self.history_index {
            None => {}
            Some(i) if i >= self.history.len() - 1 => {
                self.history_index = None;
                self.query.clear();
                self.cursor_pos = 0;
            }
            Some(i) => {
                self.history_index = Some(i + 1);
                self.query = self.history[i + 1].clone();
                self.cursor_pos = self.query.len();
            }
        }
    }

    pub fn scroll_results_up(&mut self) {
        if self.result_scroll > 0 {
            self.result_scroll -= 1;
        }
    }

    pub fn scroll_results_down(&mut self) {
        if let Some(ref table) = self.result {
            if self.result_scroll < table.row_count().saturating_sub(1) {
                self.result_scroll += 1;
            }
        }
    }

    pub fn scroll_results_left(&mut self) {
        if self.result_horizontal_scroll > 0 {
            self.result_horizontal_scroll -= 1;
        }
    }

    pub fn scroll_results_right(&mut self) {
        self.result_horizontal_scroll += 1;
    }

    pub fn page_up(&mut self) {
        self.result_scroll = self.result_scroll.saturating_sub(10);
    }

    pub fn page_down(&mut self) {
        if let Some(ref table) = self.result {
            self.result_scroll = (self.result_scroll + 10).min(table.row_count().saturating_sub(1));
        }
    }

    pub fn scroll_to_top(&mut self) {
        self.result_scroll = 0;
    }

    pub fn scroll_to_bottom(&mut self) {
        if let Some(ref table) = self.result {
            self.result_scroll = table.row_count().saturating_sub(1);
        }
    }

    pub fn enter_insert_mode(&mut self) {
        self.mode = Mode::Insert;
        self.focus = Focus::Query;
    }

    pub fn enter_normal_mode(&mut self) {
        self.mode = Mode::Normal;
    }

    pub fn enter_command_mode(&mut self) {
        self.mode = Mode::Command;
        self.command_buffer.clear();
    }

    pub fn execute_command(&mut self) {
        let cmd = self.command_buffer.trim();
        match cmd {
            "q" | "quit" => self.should_quit = true,
            "e" | "exec" | "execute" => self.execute_query(),
            "w" | "write" => {
                // Could add export functionality here
            }
            "clear" => {
                self.clear_query();
                self.result = None;
                self.error = None;
            }
            _ => {}
        }
        self.command_buffer.clear();
        self.mode = Mode::Normal;
    }

    pub fn toggle_focus(&mut self) {
        self.focus = match self.focus {
            Focus::Query => Focus::Results,
            Focus::Results => Focus::Query,
        };
    }
}
