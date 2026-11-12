// Only display preferences are persisted. Credentials and row data stay in memory.
const key = (name) => `glamdb.ui.${name}`;
export function readPreference(name, fallback, storage) {
    try {
        return (storage ?? globalThis.localStorage)?.getItem(key(name)) ?? fallback;
    } catch {
        return fallback;
    }
}
export function savePreference(name, value, storage) {
    try {
        (storage ?? globalThis.localStorage)?.setItem(key(name), String(value));
    } catch {
        // Restricted browser storage must not prevent local UI controls.
    }
}
const darkDefaults = {
    background: '#16181d',
    surface: '#22252d',
    sidebar: '#1b1e25',
    ink: '#f3f4f6',
    muted: '#abb3c3',
    accent: '#f579b7',
    accent_hover: '#ff9acb',
    hot_pink: '#f579b7',
    blush: '#432639',
    powder: '#292d37',
    border: '#3b414f',
    white: '#ffffff',
    on_accent: '#16181d',
    shadow: '#00000033',
    danger: '#ff93aa',
    success: '#76d4b2',
};
export function applyAppearance(config, mode, root = globalThis.document?.documentElement) {
    if (!root) throw new Error('Appearance root is missing');
    const selected = mode === 'dark' ? 'dark' : 'light';
    for (const [name, value] of Object.entries(config.colors)) {
        if (name.startsWith('dark_')) continue;
        root.style.setProperty(
            `--color-${name.replaceAll('_', '-')}`,
            selected === 'dark'
                ? (config.colors[`dark_${name}`] ?? darkDefaults[name] ?? value)
                : value,
        );
    }
    root.dataset.appearance = selected;
    root.style.colorScheme = selected;
    return selected;
}
