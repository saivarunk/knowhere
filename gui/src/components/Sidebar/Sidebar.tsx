import { useState } from 'react';
import { Table2, ChevronRight, ChevronDown, Database, Columns } from 'lucide-react';
import { getSchema } from '../../lib/api';
import type { ColumnInfo } from '../../lib/types';
import clsx from 'clsx';

interface SidebarProps {
    tables: string[];
    onTableSelect: (tableName: string) => void;
    selectedTable: string | null;
}

export function Sidebar({ tables, onTableSelect, selectedTable }: SidebarProps) {
    const [expandedState, setExpandedState] = useState<Record<string, { expanded: boolean; columns: ColumnInfo[]; loading: boolean }>>({});

    // Build table items from props + local expanded state
    const items = tables.map(name => ({
        name,
        expanded: expandedState[name]?.expanded ?? false,
        columns: expandedState[name]?.columns ?? [],
        loading: expandedState[name]?.loading ?? false,
    }));

    async function toggleTable(tableName: string) {
        const current = expandedState[tableName] ?? { expanded: false, columns: [], loading: false };

        if (!current.expanded && current.columns.length === 0) {
            // Load schema
            setExpandedState(prev => ({
                ...prev,
                [tableName]: { ...current, loading: true },
            }));

            try {
                const columns = await getSchema(tableName);
                setExpandedState(prev => ({
                    ...prev,
                    [tableName]: { expanded: true, columns, loading: false },
                }));
            } catch (error) {
                console.error('Failed to load schema:', error);
                setExpandedState(prev => ({
                    ...prev,
                    [tableName]: { ...current, loading: false },
                }));
            }
        } else {
            setExpandedState(prev => ({
                ...prev,
                [tableName]: { ...current, expanded: !current.expanded },
            }));
        }
    }

    function handleTableClick(tableName: string) {
        onTableSelect(tableName);
    }

    return (
        <div className="h-full flex flex-col bg-[color:var(--bg-secondary)] border-r">
            {/* Header */}
            <div className="px-3 py-3 border-b">
                <div className="flex items-center gap-2 text-sm font-semibold text-[color:var(--text-primary)]">
                    <Database size={16} className="text-primary-500" />
                    <span>Explorer</span>
                </div>
            </div>

            {/* Tables Section */}
            <div className="flex-1 overflow-y-auto scrollbar-thin">
                <div className="px-2 py-2">
                    <div className="text-xs font-medium text-[color:var(--text-muted)] uppercase tracking-wider px-2 py-1">
                        Tables ({tables.length})
                    </div>

                    {tables.length === 0 ? (
                        <div className="px-2 py-4 text-sm text-[color:var(--text-muted)] text-center">
                            Open a file or folder to see tables
                        </div>
                    ) : (
                        <div className="space-y-0.5">
                            {items.map((table) => (
                                <div key={table.name}>
                                    <button
                                        onClick={() => toggleTable(table.name)}
                                        onDoubleClick={() => handleTableClick(table.name)}
                                        className={clsx(
                                            'w-full flex items-center gap-1.5 px-2 py-1 text-sm rounded transition-colors',
                                            'hover:bg-surface-100 dark:hover:bg-surface-800',
                                            selectedTable === table.name && 'bg-primary-500/10 text-primary-600 dark:text-primary-400'
                                        )}
                                    >
                                        {table.loading ? (
                                            <div className="w-4 h-4 animate-spin rounded-full border-2 border-primary-500 border-t-transparent" />
                                        ) : table.expanded ? (
                                            <ChevronDown size={14} className="text-[color:var(--text-muted)]" />
                                        ) : (
                                            <ChevronRight size={14} className="text-[color:var(--text-muted)]" />
                                        )}
                                        <Table2 size={14} className="text-accent-orange" />
                                        <span className="truncate">{table.name}</span>
                                    </button>

                                    {table.expanded && table.columns.length > 0 && (
                                        <div className="ml-6 border-l border-surface-200 dark:border-surface-800">
                                            {table.columns.map((col) => (
                                                <div
                                                    key={col.name}
                                                    className="flex items-center gap-1.5 px-2 py-0.5 text-xs text-[color:var(--text-secondary)]"
                                                >
                                                    <Columns size={12} className="text-accent-blue" />
                                                    <span className="truncate">{col.name}</span>
                                                    <span className="ml-auto text-2xs text-[color:var(--text-muted)] font-mono">
                                                        {col.data_type}
                                                    </span>
                                                </div>
                                            ))}
                                        </div>
                                    )}
                                </div>
                            ))}
                        </div>
                    )}
                </div>
            </div>
        </div>
    );
}
