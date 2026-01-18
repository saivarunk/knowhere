import { useState, useCallback } from 'react';
import { FolderOpen, Play, RefreshCw } from 'lucide-react';
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

        {/* Editor + Results */}
        <div className="flex-1 flex flex-col min-w-0">
          {/* SQL Editor */}
          <div className="h-[40%] min-h-[150px] border-b">
            <SqlEditor
              value={query}
              onChange={setQuery}
              onExecute={handleExecute}
              theme={theme}
            />
          </div>

          {/* Results */}
          <div className="flex-1 min-h-0 bg-[color:var(--bg-primary)]">
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
