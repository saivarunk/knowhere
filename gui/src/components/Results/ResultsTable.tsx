import { useRef, useMemo } from 'react';
import { useVirtualizer } from '@tanstack/react-virtual';
import clsx from 'clsx';
import type { QueryResult } from '../../lib/types';

interface ResultsTableProps {
    result: QueryResult | null;
    error: string | null;
    isLoading: boolean;
}

export function ResultsTable({ result, error, isLoading }: ResultsTableProps) {
    const parentRef = useRef<HTMLDivElement>(null);

    const rowVirtualizer = useVirtualizer({
        count: result?.rows.length || 0,
        getScrollElement: () => parentRef.current,
        estimateSize: () => 32,
        overscan: 10,
    });

    // Calculate column widths based on content
    const columnWidths = useMemo(() => {
        if (!result) return [];

        return result.columns.map((col, colIndex) => {
            const headerWidth = col.name.length * 8 + 24;
            const maxContentWidth = result.rows.slice(0, 100).reduce((max, row) => {
                const cellValue = row[colIndex];
                const cellStr = cellValue === null ? 'NULL' : String(cellValue);
                return Math.max(max, cellStr.length * 7 + 16);
            }, 0);

            return Math.min(Math.max(headerWidth, maxContentWidth, 80), 400);
        });
    }, [result]);

    if (isLoading) {
        return (
            <div className="h-full flex items-center justify-center text-[color:var(--text-muted)]">
                <div className="flex items-center gap-2">
                    <div className="w-4 h-4 animate-spin rounded-full border-2 border-primary-500 border-t-transparent" />
                    <span>Executing query...</span>
                </div>
            </div>
        );
    }

    if (error) {
        return (
            <div className="h-full p-4 overflow-auto">
                <div className="bg-red-500/10 border border-red-500/20 rounded-lg p-4">
                    <div className="text-red-500 font-medium text-sm mb-1">Error</div>
                    <pre className="text-red-400 text-sm whitespace-pre-wrap font-mono">{error}</pre>
                </div>
            </div>
        );
    }

    if (!result) {
        return (
            <div className="h-full flex items-center justify-center text-[color:var(--text-muted)]">
                <div className="text-center">
                    <p className="text-sm">Run a query to see results</p>
                    <p className="text-xs mt-1 text-[color:var(--text-muted)]">Press ⌘+Enter to execute</p>
                </div>
            </div>
        );
    }

    if (result.rows.length === 0) {
        return (
            <div className="h-full flex items-center justify-center text-[color:var(--text-muted)]">
                <div className="text-center">
                    <p className="text-sm">Query executed successfully</p>
                    <p className="text-xs mt-1">(0 rows returned)</p>
                </div>
            </div>
        );
    }

    return (
        <div className="h-full flex flex-col">
            {/* Header */}
            <div className="flex-shrink-0 border-b bg-[color:var(--bg-secondary)] overflow-hidden">
                <div className="flex" style={{ minWidth: columnWidths.reduce((a, b) => a + b, 0) }}>
                    {result.columns.map((col, i) => (
                        <div
                            key={col.name}
                            className="flex-shrink-0 px-3 py-2 text-xs font-semibold text-[color:var(--text-secondary)] uppercase tracking-wide border-r last:border-r-0"
                            style={{ width: columnWidths[i] }}
                        >
                            <div className="truncate">{col.name}</div>
                        </div>
                    ))}
                </div>
            </div>

            {/* Virtualized Rows */}
            <div
                ref={parentRef}
                className="flex-1 overflow-auto scrollbar-thin"
            >
                <div
                    style={{
                        height: `${rowVirtualizer.getTotalSize()}px`,
                        width: columnWidths.reduce((a, b) => a + b, 0),
                        position: 'relative',
                    }}
                >
                    {rowVirtualizer.getVirtualItems().map((virtualRow) => {
                        const row = result.rows[virtualRow.index];
                        return (
                            <div
                                key={virtualRow.index}
                                className={clsx(
                                    'flex absolute w-full border-b border-surface-100 dark:border-surface-850',
                                    virtualRow.index % 2 === 0
                                        ? 'bg-transparent'
                                        : 'bg-surface-50/50 dark:bg-surface-900/50'
                                )}
                                style={{
                                    height: `${virtualRow.size}px`,
                                    transform: `translateY(${virtualRow.start}px)`,
                                }}
                            >
                                {row.map((cell, cellIndex) => (
                                    <div
                                        key={cellIndex}
                                        className="flex-shrink-0 px-3 py-1.5 text-sm font-mono border-r last:border-r-0 border-surface-100 dark:border-surface-850"
                                        style={{ width: columnWidths[cellIndex] }}
                                    >
                                        <span className={clsx(
                                            'block truncate',
                                            cell === null && 'text-[color:var(--text-muted)] italic'
                                        )}>
                                            {cell === null ? 'NULL' : String(cell)}
                                        </span>
                                    </div>
                                ))}
                            </div>
                        );
                    })}
                </div>
            </div>
        </div>
    );
}
