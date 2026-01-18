import { useRef, useCallback } from 'react';
import Editor from '@monaco-editor/react';
import type { OnMount, BeforeMount } from '@monaco-editor/react';
import type { editor } from 'monaco-editor';
import type { Theme } from '../../lib/types';

interface SqlEditorProps {
    value: string;
    onChange: (value: string) => void;
    onExecute: () => void;
    theme: Theme;
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

export function SqlEditor({ value, onChange, onExecute, theme }: SqlEditorProps) {
    const editorRef = useRef<editor.IStandaloneCodeEditor | null>(null);

    const handleBeforeMount: BeforeMount = useCallback((monaco) => {
        monaco.editor.defineTheme('zed-dark', zedDarkTheme);
        monaco.editor.defineTheme('zed-light', zedLightTheme);
    }, []);

    const handleMount: OnMount = useCallback((editor, monaco) => {
        editorRef.current = editor;

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
                    quickSuggestions: false,
                }}
            />
        </div>
    );
}
