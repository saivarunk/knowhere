import { Sun, Moon, Clock, CheckCircle2, XCircle } from 'lucide-react';
import type { Theme } from '../../lib/types';

interface StatusBarProps {
    theme: Theme;
    onToggleTheme: () => void;
    rowCount: number | null;
    executionTime: number | null;
    status: 'idle' | 'running' | 'success' | 'error';
}

export function StatusBar({ theme, onToggleTheme, rowCount, executionTime, status }: StatusBarProps) {
    return (
        <div className="h-7 flex items-center justify-between px-3 text-xs border-t bg-[color:var(--bg-secondary)]">
            {/* Left side */}
            <div className="flex items-center gap-4">
                {status === 'running' && (
                    <div className="flex items-center gap-1.5 text-primary-500">
                        <div className="w-3 h-3 animate-spin rounded-full border-2 border-primary-500 border-t-transparent" />
                        <span>Executing...</span>
                    </div>
                )}

                {status === 'success' && rowCount !== null && (
                    <div className="flex items-center gap-1.5 text-green-500">
                        <CheckCircle2 size={12} />
                        <span>{rowCount} row{rowCount !== 1 ? 's' : ''}</span>
                    </div>
                )}

                {status === 'error' && (
                    <div className="flex items-center gap-1.5 text-red-500">
                        <XCircle size={12} />
                        <span>Query failed</span>
                    </div>
                )}

                {executionTime !== null && status !== 'running' && (
                    <div className="flex items-center gap-1 text-[color:var(--text-muted)]">
                        <Clock size={11} />
                        <span>{executionTime.toFixed(0)}ms</span>
                    </div>
                )}
            </div>

            {/* Right side */}
            <div className="flex items-center gap-2">
                <button
                    onClick={onToggleTheme}
                    className="p-1 rounded hover:bg-surface-100 dark:hover:bg-surface-800 transition-colors"
                    title={`Switch to ${theme === 'dark' ? 'light' : 'dark'} mode`}
                >
                    {theme === 'dark' ? (
                        <Sun size={14} className="text-[color:var(--text-muted)]" />
                    ) : (
                        <Moon size={14} className="text-[color:var(--text-muted)]" />
                    )}
                </button>
            </div>
        </div>
    );
}
