import { useRef, useCallback, useEffect } from 'react';
import Editor from '@monaco-editor/react';
import type { OnMount, BeforeMount } from '@monaco-editor/react';
import type { editor, languages, IDisposable } from 'monaco-editor';
import type { Theme, ColumnInfo } from '../../lib/types';
import { getSchema } from '../../lib/api';

interface SqlEditorProps {
    value: string;
    onChange: (value: string) => void;
    onExecute: () => void;
    theme: Theme;
    tables: string[];
}

// SQL Keywords for suggestions
const SQL_KEYWORDS = [
    'SELECT', 'FROM', 'WHERE', 'AND', 'OR', 'NOT', 'IN', 'LIKE', 'BETWEEN',
    'ORDER BY', 'GROUP BY', 'HAVING', 'LIMIT', 'OFFSET', 'AS',
    'JOIN', 'LEFT JOIN', 'RIGHT JOIN', 'INNER JOIN', 'OUTER JOIN', 'CROSS JOIN',
    'ON', 'USING', 'UNION', 'UNION ALL', 'EXCEPT', 'INTERSECT',
    'INSERT INTO', 'VALUES', 'UPDATE', 'SET', 'DELETE FROM',
    'CREATE TABLE', 'DROP TABLE', 'ALTER TABLE', 'ADD', 'COLUMN',
    'DISTINCT', 'ALL', 'NULL', 'IS NULL', 'IS NOT NULL',
    'ASC', 'DESC', 'CASE', 'WHEN', 'THEN', 'ELSE', 'END',
    'COUNT', 'SUM', 'AVG', 'MIN', 'MAX', 'COALESCE', 'CAST',
];

// Cache for table schemas
const schemaCache: Record<string, ColumnInfo[]> = {};

// Parse SQL to find table aliases
function parseTableAliases(sql: string): Record<string, string> {
    const aliases: Record<string, string> = {};

    // Match patterns like: FROM table_name alias, FROM table_name AS alias
    // Also handles: JOIN table_name alias, JOIN table_name AS alias
    const patterns = [
        /(?:FROM|JOIN)\s+["']?(\w+)["']?\s+(?:AS\s+)?["']?(\w+)["']?/gi,
        /(?:FROM|JOIN)\s+["']?(\w+)["']?\s+["']?(\w+)["']?(?:\s+(?:ON|WHERE|LEFT|RIGHT|INNER|OUTER|CROSS|JOIN|ORDER|GROUP|LIMIT|$))/gi,
    ];

    for (const pattern of patterns) {
        let match;
        while ((match = pattern.exec(sql)) !== null) {
            const tableName = match[1];
            const alias = match[2];
            // Make sure alias isn't a keyword
            if (alias && !SQL_KEYWORDS.some(k => k.toUpperCase() === alias.toUpperCase())) {
                aliases[alias.toLowerCase()] = tableName;
            }
        }
    }

    return aliases;
}

// Custom Zed-inspired dark theme
const zedDarkTheme: editor.IStandaloneThemeData = {
    base: 'vs-dark',
    inherit: true,
    rules: [
        { token: 'keyword', foreground: 'c586c0' },
        { token: 'keyword.sql', foreground: 'c586c0' },
        { token: 'string', foreground: 'ce9178' },
        { token: 'number', foreground: 'b5cea8' },
        { token: 'comment', foreground: '6a9955', fontStyle: 'italic' },
        { token: 'operator', foreground: 'd4d4d4' },
        { token: 'identifier', foreground: '9cdcfe' },
        { token: 'type', foreground: '4ec9b0' },
        { token: 'function', foreground: 'dcdcaa' },
    ],
    colors: {
        'editor.background': '#0f0f12',
        'editor.foreground': '#d4d4d4',
        'editorLineNumber.foreground': '#4b5563',
        'editorLineNumber.activeForeground': '#9ca3af',
        'editor.selectionBackground': '#264f78',
        'editor.lineHighlightBackground': '#1f1f23',
        'editorCursor.foreground': '#aeafad',
        'editorIndentGuide.background': '#27272a',
        'editorIndentGuide.activeBackground': '#3f3f46',
    },
};

// Custom Zed-inspired light theme
const zedLightTheme: editor.IStandaloneThemeData = {
    base: 'vs',
    inherit: true,
    rules: [
        { token: 'keyword', foreground: 'a626a4' },
        { token: 'keyword.sql', foreground: 'a626a4' },
        { token: 'string', foreground: '50a14f' },
        { token: 'number', foreground: '986801' },
        { token: 'comment', foreground: 'a0a1a7', fontStyle: 'italic' },
        { token: 'operator', foreground: '383a42' },
        { token: 'identifier', foreground: '4078f2' },
        { token: 'type', foreground: '0184bc' },
        { token: 'function', foreground: 'c18401' },
    ],
    colors: {
        'editor.background': '#fafafa',
        'editor.foreground': '#383a42',
        'editorLineNumber.foreground': '#a0a1a7',
        'editorLineNumber.activeForeground': '#383a42',
        'editor.selectionBackground': '#d7e1f3',
        'editor.lineHighlightBackground': '#f4f4f5',
        'editorCursor.foreground': '#383a42',
        'editorIndentGuide.background': '#e4e4e7',
        'editorIndentGuide.activeBackground': '#d4d4d8',
    },
};

export function SqlEditor({ value, onChange, onExecute, theme, tables }: SqlEditorProps) {
    const editorRef = useRef<editor.IStandaloneCodeEditor | null>(null);
    const monacoRef = useRef<typeof import('monaco-editor') | null>(null);
    const completionDisposable = useRef<IDisposable | null>(null);

    const handleBeforeMount: BeforeMount = useCallback((monaco) => {
        monaco.editor.defineTheme('zed-dark', zedDarkTheme);
        monaco.editor.defineTheme('zed-light', zedLightTheme);
    }, []);

    const handleMount: OnMount = useCallback((editor, monaco) => {
        editorRef.current = editor;
        monacoRef.current = monaco;

        // Add Cmd/Ctrl+Enter to run query
        editor.addAction({
            id: 'run-query',
            label: 'Run Query',
            keybindings: [
                monaco.KeyMod.CtrlCmd | monaco.KeyCode.Enter,
            ],
            run: () => {
                onExecute();
            },
        });

        // Focus the editor
        editor.focus();
    }, [onExecute]);

    // Register completion provider when tables change
    useEffect(() => {
        const monaco = monacoRef.current;
        if (!monaco) return;

        // Dispose previous completion provider
        if (completionDisposable.current) {
            completionDisposable.current.dispose();
        }

        // Register new completion provider
        completionDisposable.current = monaco.languages.registerCompletionItemProvider('sql', {
            triggerCharacters: ['.', ' '],
            provideCompletionItems: async (model, position) => {
                const fullText = model.getValue();
                const word = model.getWordUntilPosition(position);
                const range = {
                    startLineNumber: position.lineNumber,
                    endLineNumber: position.lineNumber,
                    startColumn: word.startColumn,
                    endColumn: word.endColumn,
                };

                const suggestions: languages.CompletionItem[] = [];
                const lineText = model.getLineContent(position.lineNumber);
                const textBeforeCursor = lineText.substring(0, position.column - 1);

                // Check if we're after a dot (alias.column or table.column)
                const dotMatch = textBeforeCursor.match(/(\w+)\.\s*$/);
                if (dotMatch) {
                    const prefix = dotMatch[1].toLowerCase();
                    const aliases = parseTableAliases(fullText);

                    // Check if prefix is an alias or table name
                    let tableName = aliases[prefix] || (tables.find(t => t.toLowerCase() === prefix));

                    if (tableName) {
                        // Get columns for this table
                        let columns = schemaCache[tableName];
                        if (!columns) {
                            try {
                                columns = await getSchema(tableName);
                                schemaCache[tableName] = columns;
                            } catch (e) {
                                columns = [];
                            }
                        }

                        for (const col of columns) {
                            suggestions.push({
                                label: col.name,
                                kind: monaco.languages.CompletionItemKind.Field,
                                detail: col.data_type,
                                insertText: col.name,
                                range,
                            });
                        }
                    }

                    return { suggestions };
                }

                // Check if we're after FROM, JOIN keywords - suggest tables
                const afterFromJoin = /\b(?:FROM|JOIN)\s+\w*$/i.test(textBeforeCursor);
                if (afterFromJoin || word.word.length > 0) {
                    // Suggest tables
                    for (const table of tables) {
                        suggestions.push({
                            label: table,
                            kind: monaco.languages.CompletionItemKind.Class,
                            detail: 'Table',
                            insertText: `"${table}"`,
                            range,
                        });
                    }
                }

                // Always suggest keywords when not after a dot
                for (const kw of SQL_KEYWORDS) {
                    suggestions.push({
                        label: kw,
                        kind: monaco.languages.CompletionItemKind.Keyword,
                        insertText: kw,
                        range,
                    });
                }

                return { suggestions };
            },
        });

        return () => {
            if (completionDisposable.current) {
                completionDisposable.current.dispose();
            }
        };
    }, [tables]);

    const handleChange = useCallback((value: string | undefined) => {
        onChange(value || '');
    }, [onChange]);

    return (
        <div className="h-full w-full">
            <Editor
                height="100%"
                defaultLanguage="sql"
                value={value}
                onChange={handleChange}
                beforeMount={handleBeforeMount}
                onMount={handleMount}
                theme={theme === 'dark' ? 'zed-dark' : 'zed-light'}
                options={{
                    fontFamily: "'JetBrains Mono', 'Fira Code', monospace",
                    fontSize: 14,
                    lineHeight: 22,
                    minimap: { enabled: false },
                    scrollBeyondLastLine: false,
                    renderLineHighlight: 'line',
                    lineNumbers: 'on',
                    glyphMargin: false,
                    folding: true,
                    lineDecorationsWidth: 8,
                    lineNumbersMinChars: 3,
                    padding: { top: 12, bottom: 12 },
                    automaticLayout: true,
                    tabSize: 2,
                    wordWrap: 'on',
                    contextmenu: true,
                    quickSuggestions: true,
                    suggestOnTriggerCharacters: true,
                }}
            />
        </div>
    );
}
