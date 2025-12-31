use crossterm::event::{self, Event, KeyCode, KeyEvent, KeyModifiers};
use std::time::Duration;

use super::app::{App, Focus, Mode};

pub fn handle_events(app: &mut App) -> std::io::Result<bool> {
    if event::poll(Duration::from_millis(100))? {
        if let Event::Key(key) = event::read()? {
            handle_key_event(app, key);
        }
    }
    Ok(app.should_quit)
}

fn handle_key_event(app: &mut App, key: KeyEvent) {
    // Handle Ctrl+C globally
    if key.modifiers.contains(KeyModifiers::CONTROL) && key.code == KeyCode::Char('c') {
        if app.mode == Mode::Insert {
            app.enter_normal_mode();
        } else {
            app.should_quit = true;
        }
        return;
    }

    match app.mode {
        Mode::Normal => handle_normal_mode(app, key),
        Mode::Insert => handle_insert_mode(app, key),
        Mode::Command => handle_command_mode(app, key),
    }
}

fn handle_normal_mode(app: &mut App, key: KeyEvent) {
    match key.code {
        // Mode switching
        KeyCode::Char('i') => app.enter_insert_mode(),
        KeyCode::Char('I') => {
            app.move_cursor_start();
            app.enter_insert_mode();
        }
        KeyCode::Char('a') => {
            app.move_cursor_right();
            app.enter_insert_mode();
        }
        KeyCode::Char('A') => {
            app.move_cursor_end();
            app.enter_insert_mode();
        }
        KeyCode::Char(':') => app.enter_command_mode(),

        // Quit
        KeyCode::Char('q') => app.should_quit = true,

        // Focus switching
        KeyCode::Tab => app.toggle_focus(),

        // Navigation in query
        KeyCode::Char('h') | KeyCode::Left => {
            if app.focus == Focus::Query {
                app.move_cursor_left();
            } else {
                app.scroll_results_left();
            }
        }
        KeyCode::Char('l') | KeyCode::Right => {
            if app.focus == Focus::Query {
                app.move_cursor_right();
            } else {
                app.scroll_results_right();
            }
        }
        KeyCode::Char('j') | KeyCode::Down => {
            if app.focus == Focus::Results {
                app.scroll_results_down();
            }
        }
        KeyCode::Char('k') | KeyCode::Up => {
            if app.focus == Focus::Results {
                app.scroll_results_up();
            }
        }
        KeyCode::Char('0') => {
            if app.focus == Focus::Query {
                app.move_cursor_start();
            }
        }
        KeyCode::Char('$') => {
            if app.focus == Focus::Query {
                app.move_cursor_end();
            }
        }
        KeyCode::Char('w') => {
            if app.focus == Focus::Query {
                app.move_cursor_word_forward();
            }
        }
        KeyCode::Char('b') => {
            if app.focus == Focus::Query {
                app.move_cursor_word_backward();
            }
        }
        KeyCode::Char('g') => {
            if app.focus == Focus::Results {
                app.scroll_to_top();
            }
        }
        KeyCode::Char('G') => {
            if app.focus == Focus::Results {
                app.scroll_to_bottom();
            }
        }

        // Page navigation
        KeyCode::Char('d') if key.modifiers.contains(KeyModifiers::CONTROL) => {
            app.page_down();
        }
        KeyCode::Char('u') if key.modifiers.contains(KeyModifiers::CONTROL) => {
            app.page_up();
        }

        // Delete operations
        KeyCode::Char('x') => {
            if app.focus == Focus::Query {
                app.delete_char_forward();
            }
        }
        KeyCode::Char('D') => {
            if app.focus == Focus::Query {
                app.delete_to_end();
            }
        }

        // Execute query
        KeyCode::Enter => app.execute_query(),

        // Clear
        KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => {
            app.clear_query();
        }

        _ => {}
    }
}

fn handle_insert_mode(app: &mut App, key: KeyEvent) {
    match key.code {
        KeyCode::Esc => app.enter_normal_mode(),
        KeyCode::Enter => {
            app.execute_query();
            app.enter_normal_mode();
        }
        KeyCode::Backspace => app.delete_char(),
        KeyCode::Delete => app.delete_char_forward(),
        KeyCode::Left => app.move_cursor_left(),
        KeyCode::Right => app.move_cursor_right(),
        KeyCode::Home => app.move_cursor_start(),
        KeyCode::End => app.move_cursor_end(),
        KeyCode::Up => app.history_up(),
        KeyCode::Down => app.history_down(),

        // Ctrl shortcuts
        KeyCode::Char('w') if key.modifiers.contains(KeyModifiers::CONTROL) => {
            app.delete_word_backward();
        }
        KeyCode::Char('u') if key.modifiers.contains(KeyModifiers::CONTROL) => {
            app.delete_to_start();
        }
        KeyCode::Char('k') if key.modifiers.contains(KeyModifiers::CONTROL) => {
            app.delete_to_end();
        }
        KeyCode::Char('a') if key.modifiers.contains(KeyModifiers::CONTROL) => {
            app.move_cursor_start();
        }
        KeyCode::Char('e') if key.modifiers.contains(KeyModifiers::CONTROL) => {
            app.move_cursor_end();
        }

        KeyCode::Char(c) => app.insert_char(c),

        _ => {}
    }
}

fn handle_command_mode(app: &mut App, key: KeyEvent) {
    match key.code {
        KeyCode::Esc => {
            app.command_buffer.clear();
            app.enter_normal_mode();
        }
        KeyCode::Enter => app.execute_command(),
        KeyCode::Backspace => {
            app.command_buffer.pop();
            if app.command_buffer.is_empty() {
                app.enter_normal_mode();
            }
        }
        KeyCode::Char(c) => app.command_buffer.push(c),
        _ => {}
    }
}
