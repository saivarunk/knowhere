# GUI Features

## SQL Intellisense

The editor provides intelligent auto-complete:

### Table Suggestions
After `FROM` or `JOIN`, type to see available tables:

```sql
SELECT * FROM us|  -- Shows: users, user_logs, etc.
```

### Column Suggestions with Aliases
Supports table aliases:

```sql
SELECT o.| FROM orders o  -- Shows: id, user_id, amount, etc.
```

<!-- TODO: Add intellisense screenshot -->
![Intellisense](../assets/screenshots/gui-intellisense.png)

---

## Query Persistence

### Save Queries
Save your SQL queries as `.sql` files:

- **Default location**: `~/knowhere/queries/`
- **Custom path**: Use the OS file picker

### Recent Queries
Access recently opened/saved queries from the toolbar dropdown.

<!-- TODO: Add recent queries screenshot -->
![Recent Queries](../assets/screenshots/gui-recent.png)

---

## Resizable Panes

Drag the divider between the editor and results pane to resize:

- Minimum 15% for each pane
- Maximum 85% for each pane
- Visual indicator when dragging

---

## Theme Support

Toggle between light and dark themes via the status bar.

| Theme | Description |
|-------|-------------|
| Dark  | Zed-inspired dark theme with syntax highlighting |
| Light | Clean light theme for bright environments |

---

## Keyboard Shortcuts

| Shortcut | Action |
|----------|--------|
| `⌘ + Enter` | Execute query |
| `⌘ + S` | Save query |
| `⌘ + O` | Open query |
