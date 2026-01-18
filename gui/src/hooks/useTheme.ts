import { useState, useEffect } from 'react';
import type { Theme } from '../lib/types';

export function useTheme() {
    const [theme, setTheme] = useState<Theme>(() => {
        if (typeof window !== 'undefined') {
            const stored = localStorage.getItem('theme') as Theme | null;
            if (stored) return stored;
            return window.matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light';
        }
        return 'dark';
    });

    useEffect(() => {
        const root = document.documentElement;
        if (theme === 'dark') {
            root.classList.add('dark');
        } else {
            root.classList.remove('dark');
        }
        localStorage.setItem('theme', theme);
    }, [theme]);

    const toggleTheme = () => {
        setTheme(prev => prev === 'dark' ? 'light' : 'dark');
    };

    return { theme, setTheme, toggleTheme };
}
