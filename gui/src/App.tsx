import { useState, useCallback, useRef } from 'react';
import { FolderOpen, Play, RefreshCw, GripHorizontal } from 'lucide-react';
import { open } from '@tauri-apps/plugin-dialog';

import { Sidebar } from './components/Sidebar';
import { SqlEditor } from './components/Editor';
import { ResultsTable } from './components/Results';
import { StatusBar } from './components/StatusBar';
import { useTheme } from './hooks/useTheme';
import { loadPath, executeSql } from './lib/api';
import type { QueryResult } from './lib/types';

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
  const [editorHeight, setEditorHeight] = useState(40); // percentage
  const [isDragging, setIsDragging] = useState(false);
  const containerRef = useRef<HTMLDivElement>(null);

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

    // Clamp between 15% and 85%
    setEditorHeight(Math.min(85, Math.max(15, percentage)));
  }, [isDragging]);

  const handleDragEnd = useCallback(() => {
    setIsDragging(false);
  }, []);

  const isLoaded = tables.length > 0;

  return (
    <div className="h-screen flex flex-col bg-[color:var(--bg-primary)]">
      {/* Toolbar */}
      <div className="h-11 flex items-center gap-2 px-3 border-b bg-[color:var(--bg-secondary)]">
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
