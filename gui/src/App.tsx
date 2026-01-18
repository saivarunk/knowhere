import { useState, useCallback, useRef, useEffect } from 'react';
import { FolderOpen, Play, RefreshCw, GripHorizontal, Save, FileText, Clock, ChevronDown } from 'lucide-react';
import { open, save } from '@tauri-apps/plugin-dialog';

import { Sidebar } from './components/Sidebar';
import { SqlEditor } from './components/Editor';
import { ResultsTable } from './components/Results';
import { StatusBar } from './components/StatusBar';
import { useTheme } from './hooks/useTheme';
import { loadPath, executeSql, getQueriesDirectory, saveQuery, loadQuery, getRecentQueries } from './lib/api';
import type { QueryResult, RecentQuery } from './lib/types';

function App() {
  const { theme, toggleTheme } = useTheme();
  const [tables, setTables] = useState<string[]>([]);
  const [selectedTable, setSelectedTable] = useState<string | null>(null);
  const [query, setQuery] = useState('SELECT * FROM ');
  const [result, setResult] = useState<QueryResult | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [isExecuting, setIsExecuting] = useState(false);
  const [executionTime, setExecutionTime] = useState<number | null>(null);
  const [status, setStatus] = useState<'idle' | 'running' | 'success' | 'error'>('idle');
  const [loadingPath, setLoadingPath] = useState(false);

  // Resizable split pane
  const [editorHeight, setEditorHeight] = useState(40);
  const [isDragging, setIsDragging] = useState(false);
  const containerRef = useRef<HTMLDivElement>(null);

  // Recent queries
  const [recentQueries, setRecentQueries] = useState<RecentQuery[]>([]);
  const [showRecentDropdown, setShowRecentDropdown] = useState(false);
  const [currentQueryPath, setCurrentQueryPath] = useState<string | null>(null);

  // Load recent queries on mount
  useEffect(() => {
    loadRecentQueries();
  }, []);

  const loadRecentQueries = async () => {
    try {
      const queries = await getRecentQueries();
      setRecentQueries(queries);
    } catch (err) {
      console.error('Failed to load recent queries:', err);
    }
  };

  const handleOpenFolder = useCallback(async () => {
    try {
      const selected = await open({
        directory: true,
        multiple: false,
        title: 'Select a folder',
      });

      if (selected) {
        setLoadingPath(true);
        setError(null);
        const tableList = await loadPath(selected);
        setTables(tableList);
        setResult(null);
        setStatus('idle');
        setLoadingPath(false);
      }
    } catch (err) {
      console.error('Failed to open folder:', err);
      setError(String(err));
      setStatus('error');
      setLoadingPath(false);
    }
  }, []);

  const handleOpenFile = useCallback(async () => {
    try {
      const selected = await open({
        directory: false,
        multiple: false,
        title: 'Select a file',
        filters: [
          { name: 'Data Files', extensions: ['csv', 'parquet', 'pq', 'db', 'sqlite', 'sqlite3'] },
          { name: 'All Files', extensions: ['*'] },
        ],
      });

      if (selected) {
        setLoadingPath(true);
        setError(null);
        const tableList = await loadPath(selected);
        setTables(tableList);
        setResult(null);
        setStatus('idle');
        setLoadingPath(false);
      }
    } catch (err) {
      console.error('Failed to open file:', err);
      setError(String(err));
      setStatus('error');
      setLoadingPath(false);
    }
  }, []);

  const handleOpenQuery = useCallback(async () => {
    try {
      const selected = await open({
        directory: false,
        multiple: false,
        title: 'Open SQL Query',
        filters: [
          { name: 'SQL Files', extensions: ['sql'] },
          { name: 'All Files', extensions: ['*'] },
        ],
      });

      if (selected) {
        const sql = await loadQuery(selected);
        setQuery(sql);
        setCurrentQueryPath(selected);
        await loadRecentQueries();
      }
    } catch (err) {
      console.error('Failed to open query:', err);
      setError(String(err));
    }
  }, []);

  const handleSaveQuery = useCallback(async () => {
    try {
      const defaultDir = await getQueriesDirectory();

      const selected = await save({
        title: 'Save SQL Query',
        defaultPath: currentQueryPath || `${defaultDir}/query.sql`,
        filters: [
          { name: 'SQL Files', extensions: ['sql'] },
        ],
      });

      if (selected) {
        const name = selected.split('/').pop()?.replace('.sql', '') || 'Untitled';
        await saveQuery(selected, query, name);
        setCurrentQueryPath(selected);
        await loadRecentQueries();
      }
    } catch (err) {
      console.error('Failed to save query:', err);
      setError(String(err));
    }
  }, [query, currentQueryPath]);

  const handleLoadRecentQuery = useCallback(async (recentQuery: RecentQuery) => {
    try {
      const sql = await loadQuery(recentQuery.path);
      setQuery(sql);
      setCurrentQueryPath(recentQuery.path);
      setShowRecentDropdown(false);
      await loadRecentQueries();
    } catch (err) {
      console.error('Failed to load recent query:', err);
      setError(String(err));
    }
  }, []);

  const handleExecute = useCallback(async () => {
    if (!query.trim() || isExecuting) return;

    setIsExecuting(true);
    setStatus('running');
    setError(null);
    const startTime = performance.now();

    try {
      const queryResult = await executeSql(query);
      const endTime = performance.now();
      setResult(queryResult);
      setExecutionTime(endTime - startTime);
      setStatus('success');
    } catch (err) {
      const endTime = performance.now();
      setError(String(err));
      setResult(null);
      setExecutionTime(endTime - startTime);
      setStatus('error');
    } finally {
      setIsExecuting(false);
    }
  }, [query, isExecuting]);

  const handleTableSelect = useCallback((tableName: string) => {
    setSelectedTable(tableName);
    setQuery(`SELECT * FROM "${tableName}" LIMIT 100`);
  }, []);

  // Handle split pane dragging
  const handleDragStart = useCallback(() => {
    setIsDragging(true);
  }, []);

  const handleDragMove = useCallback((e: React.MouseEvent) => {
    if (!isDragging || !containerRef.current) return;

    const container = containerRef.current;
    const rect = container.getBoundingClientRect();
    const y = e.clientY - rect.top;
    const percentage = (y / rect.height) * 100;

    setEditorHeight(Math.min(85, Math.max(15, percentage)));
  }, [isDragging]);

  const handleDragEnd = useCallback(() => {
    setIsDragging(false);
  }, []);

  const isLoaded = tables.length > 0;

  const formatTimestamp = (ts: number) => {
    const date = new Date(ts * 1000);
    return date.toLocaleDateString() + ' ' + date.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
  };

  return (
    <div className="h-screen flex flex-col bg-[color:var(--bg-primary)]">
      {/* Toolbar */}
      <div className="h-11 flex items-center gap-2 px-3 border-b bg-[color:var(--bg-secondary)]">
        {/* Data Files */}
        <button
          onClick={handleOpenFile}
          disabled={loadingPath}
          className="btn btn-secondary text-xs gap-1.5 disabled:opacity-50"
        >
          <FolderOpen size={14} />
          Open File
        </button>
        <button
          onClick={handleOpenFolder}
          disabled={loadingPath}
          className="btn btn-secondary text-xs gap-1.5 disabled:opacity-50"
        >
          <FolderOpen size={14} />
          Open Folder
        </button>

        <div className="w-px h-5 bg-[color:var(--border)] mx-1" />

        {/* Query Files */}
        <button
          onClick={handleOpenQuery}
          className="btn btn-secondary text-xs gap-1.5"
        >
          <FileText size={14} />
          Open Query
        </button>
        <button
          onClick={handleSaveQuery}
          className="btn btn-secondary text-xs gap-1.5"
        >
          <Save size={14} />
          Save Query
        </button>

        {/* Recent Queries Dropdown */}
        <div className="relative">
          <button
            onClick={() => setShowRecentDropdown(!showRecentDropdown)}
            className="btn btn-secondary text-xs gap-1"
          >
            <Clock size={14} />
            Recent
            <ChevronDown size={12} />
          </button>

          {showRecentDropdown && (
            <>
              <div
                className="fixed inset-0 z-10"
                onClick={() => setShowRecentDropdown(false)}
              />
              <div
                className="absolute top-full left-0 mt-1 w-72 max-h-80 overflow-y-auto rounded-lg shadow-lg z-20"
                style={{
                  backgroundColor: 'var(--bg-secondary)',
                  border: '1px solid var(--border)'
                }}
              >
                {recentQueries.length === 0 ? (
                  <div className="px-3 py-4 text-sm text-center" style={{ color: 'var(--text-muted)' }}>
                    No recent queries
                  </div>
                ) : (
                  recentQueries.map((rq, idx) => (
                    <button
                      key={idx}
                      onClick={() => handleLoadRecentQuery(rq)}
                      className="w-full text-left px-3 py-2 hover:bg-[color:var(--bg-tertiary)] transition-colors"
                    >
                      <div className="flex items-center gap-2">
                        <FileText size={14} style={{ color: 'var(--text-muted)' }} />
                        <span className="font-medium text-sm truncate" style={{ color: 'var(--text-primary)' }}>
                          {rq.name}
                        </span>
                      </div>
                      <div className="ml-6 text-xs truncate" style={{ color: 'var(--text-muted)' }}>
                        {rq.sql}
                      </div>
                      <div className="ml-6 text-2xs" style={{ color: 'var(--text-muted)' }}>
                        {formatTimestamp(rq.timestamp)}
                      </div>
                    </button>
                  ))
                )}
              </div>
            </>
          )}
        </div>

        {loadingPath && (
          <div className="flex items-center gap-1.5 text-xs text-[color:var(--text-muted)]">
            <RefreshCw size={12} className="animate-spin" />
            Loading...
          </div>
        )}

        <div className="flex-1" />

        <button
          onClick={handleExecute}
          disabled={!isLoaded || isExecuting}
          className="btn btn-primary text-xs gap-1.5 disabled:opacity-50 disabled:cursor-not-allowed"
        >
          <Play size={14} />
          Run Query
          <kbd className="ml-1 text-2xs opacity-70">⌘↵</kbd>
        </button>
      </div>

      {/* Main Content */}
      <div className="flex-1 flex min-h-0">
        {/* Sidebar */}
        <div className="w-56 flex-shrink-0">
          <Sidebar
            tables={tables}
            onTableSelect={handleTableSelect}
            selectedTable={selectedTable}
          />
        </div>

        {/* Editor + Results with resizable split */}
        <div
          ref={containerRef}
          className="flex-1 flex flex-col min-w-0 select-none"
          onMouseMove={isDragging ? handleDragMove : undefined}
          onMouseUp={isDragging ? handleDragEnd : undefined}
          onMouseLeave={isDragging ? handleDragEnd : undefined}
        >
          {/* SQL Editor */}
          <div
            className="min-h-[100px] border-b overflow-hidden"
            style={{ height: `${editorHeight}%` }}
          >
            <SqlEditor
              value={query}
              onChange={setQuery}
              onExecute={handleExecute}
              theme={theme}
              tables={tables}
            />
          </div>

          {/* Resize Handle */}
          <div
            className="h-2 flex items-center justify-center cursor-row-resize group"
            style={{
              backgroundColor: isDragging ? 'var(--accent)' : 'var(--bg-secondary)',
              transition: isDragging ? 'none' : 'background-color 0.15s'
            }}
            onMouseDown={handleDragStart}
          >
            <GripHorizontal
              size={14}
              className="opacity-40 group-hover:opacity-70 transition-opacity"
              style={{ color: 'var(--text-muted)' }}
            />
          </div>

          {/* Results */}
          <div
            className="min-h-[100px] bg-[color:var(--bg-primary)]"
            style={{ height: `${100 - editorHeight}%` }}
          >
            <ResultsTable
              result={result}
              error={error}
              isLoading={isExecuting}
            />
          </div>
        </div>
      </div>

      {/* Status Bar */}
      <StatusBar
        theme={theme}
        onToggleTheme={toggleTheme}
        rowCount={result?.row_count ?? null}
        executionTime={executionTime}
        status={status}
      />
    </div>
  );
}

export default App;
