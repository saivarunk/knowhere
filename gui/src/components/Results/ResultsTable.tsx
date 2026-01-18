import { useRef, useMemo, useState, useCallback } from 'react';
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
    const headerScrollRef = useRef<HTMLDivElement>(null);

    // Track column widths with state so they can be resized
    const [columnWidthOverrides, setColumnWidthOverrides] = useState<Record<number, number>>({});
    const [resizing, setResizing] = useState<{ index: number; startX: number; startWidth: number } | null>(null);

    const rowVirtualizer = useVirtualizer({
        count: result?.rows.length || 0,
        getScrollElement: () => parentRef.current,
        estimateSize: () => 32,
        overscan: 10,
    });

    // Calculate initial column widths based on content
    const defaultColumnWidths = useMemo(() => {
        if (!result) return [];

        return result.columns.map((col, colIndex) => {
            const headerWidth = col.name.length * 9 + 32;
            const maxContentWidth = result.rows.slice(0, 100).reduce((max, row) => {
                const cellValue = row[colIndex];
                const cellStr = cellValue === null ? 'NULL' : String(cellValue);
                return Math.max(max, cellStr.length * 8 + 24);
            }, 0);

            return Math.min(Math.max(headerWidth, maxContentWidth, 100), 500);
        });
    }, [result]);

    // Merge defaults with overrides
    const columnWidths = useMemo(() => {
        return defaultColumnWidths.map((w, i) => columnWidthOverrides[i] ?? w);
    }, [defaultColumnWidths, columnWidthOverrides]);

    // Handle resize start
    const handleResizeStart = useCallback((e: React.MouseEvent, index: number) => {
        e.preventDefault();
        setResizing({
            index,
            startX: e.clientX,
            startWidth: columnWidths[index],
        });
    }, [columnWidths]);

    // Handle resize move
    const handleResizeMove = useCallback((e: React.MouseEvent) => {
        if (!resizing) return;

        const delta = e.clientX - resizing.startX;
        const newWidth = Math.max(60, resizing.startWidth + delta);

        setColumnWidthOverrides(prev => ({
            ...prev,
            [resizing.index]: newWidth,
        }));
    }, [resizing]);

    // Handle resize end
    const handleResizeEnd = useCallback(() => {
        setResizing(null);
    }, []);

    // Sync header scroll with body scroll
    const handleBodyScroll = useCallback((e: React.UIEvent<HTMLDivElement>) => {
        if (headerScrollRef.current) {
            headerScrollRef.current.scrollLeft = e.currentTarget.scrollLeft;
        }
    }, []);

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

    const totalWidth = columnWidths.reduce((a, b) => a + b, 0);

    return (
        <div
            className="h-full flex flex-col select-none"
            onMouseMove={resizing ? handleResizeMove : undefined}
            onMouseUp={resizing ? handleResizeEnd : undefined}
            onMouseLeave={resizing ? handleResizeEnd : undefined}
        >
            {/* Header */}
            <div
                ref={headerScrollRef}
                className="flex-shrink-0 border-b overflow-hidden"
                style={{ backgroundColor: 'var(--bg-tertiary)' }}
            >
                <div className="flex" style={{ minWidth: totalWidth }}>
                    {result.columns.map((col, i) => (
                        <div
                            key={col.name}
                            className="flex-shrink-0 relative group"
                            style={{ width: columnWidths[i] }}
                        >
                            <div
                                className="px-3 py-2.5 text-xs font-semibold uppercase tracking-wide"
                                style={{
                                    color: 'var(--text-secondary)',
                                    borderRight: '1px solid var(--border)'
                                }}
                            >
                                <div className="truncate">{col.name}</div>
                            </div>
                            {/* Resize handle */}
                            <div
                                className={clsx(
                                    'absolute right-0 top-0 w-1 h-full cursor-col-resize transition-colors',
                                    resizing?.index === i ? 'bg-blue-500' : 'hover:bg-blue-400'
                                )}
                                onMouseDown={(e) => handleResizeStart(e, i)}
                            />
                        </div>
                    ))}
                </div>
            </div>

            {/* Virtualized Rows */}
            <div
                ref={parentRef}
                className="flex-1 overflow-auto scrollbar-thin"
                onScroll={handleBodyScroll}
            >
                <div
                    style={{
                        height: `${rowVirtualizer.getTotalSize()}px`,
                        width: totalWidth,
                        position: 'relative',
                    }}
                >
                    {rowVirtualizer.getVirtualItems().map((virtualRow) => {
                        const row = result.rows[virtualRow.index];
                        const isEven = virtualRow.index % 2 === 0;
                        return (
                            <div
                                key={virtualRow.index}
                                className="flex absolute w-full results-row"
                                style={{
                                    height: `${virtualRow.size}px`,
                                    transform: `translateY(${virtualRow.start}px)`,
                                    backgroundColor: isEven ? 'var(--bg-primary)' : 'var(--bg-secondary)',
                                }}
                            >
                                {row.map((cell, cellIndex) => (
                                    <div
                                        key={cellIndex}
                                        className="flex-shrink-0 px-3 py-1.5 text-sm font-mono"
                                        style={{
                                            width: columnWidths[cellIndex],
                                            borderRight: cellIndex < row.length - 1 ? '1px solid var(--border)' : 'none'
                                        }}
                                    >
                                        <span
                                            className="block truncate"
                                            style={{
                                                color: cell === null ? 'var(--text-muted)' : 'var(--text-primary)',
                                                fontStyle: cell === null ? 'italic' : 'normal'
                                            }}
                                        >
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
