const HEX = /^#(?:[0-9a-f]{3}|[0-9a-f]{4}|[0-9a-f]{6}|[0-9a-f]{8})$/i;
const FONT = /^[a-z0-9\s,'"-]{1,180}$/i;
const DIMENSION = /^(?:0|\d+(?:\.\d+)?(?:px|rem|em|%))$/;
const SECTIONS = ['version', 'brand', 'colors', 'typography', 'sizing', 'decoration', 'labels'];
const BRAND_ASSET = /^assets\/brand\/[a-zA-Z0-9_-]+\.(?:png|svg|webp)$/;
const object = (value) => value !== null && typeof value === 'object' && !Array.isArray(value);
function assert(condition, message) {
    if (!condition) throw new Error(message);
}
export function validateTheme(config) {
    assert(object(config) && config.version === 1, 'Unsupported theme version');
    assert(
        Object.keys(config).every((key) => SECTIONS.includes(key)),
        'Unknown theme section',
    );
    for (const section of SECTIONS.slice(1))
        assert(object(config[section]), `Missing theme ${section}`);
    for (const [key, value] of Object.entries(config.colors))
        assert(
            /^[a-z][a-z0-9_]*$/.test(key) && typeof value === 'string' && HEX.test(value),
            `Unsafe color ${key}`,
        );
    for (const [key, value] of Object.entries(config.typography))
        assert(
            /^[a-z][a-z0-9_]*$/.test(key) && typeof value === 'string' && FONT.test(value),
            `Unsafe font ${key}`,
        );
    for (const [key, value] of Object.entries(config.sizing))
        assert(
            /^[a-z][a-z0-9_]*$/.test(key) && typeof value === 'string' && DIMENSION.test(value),
            `Unsafe size ${key}`,
        );
    for (const value of Object.values(config.decoration))
        assert(typeof value === 'boolean', 'Decoration flags must be boolean');
    for (const section of ['brand', 'labels'])
        for (const value of Object.values(config[section]))
            assert(typeof value === 'string' && value.length <= 1000, 'Theme text is too long');
    assert(
        typeof config.brand.name === 'string' && config.brand.name.length > 0,
        'Brand name is required',
    );
    for (const key of ['wordmark', 'mark'])
        if (config.brand[key] !== undefined)
            assert(
                typeof config.brand[key] === 'string' && BRAND_ASSET.test(config.brand[key]),
                `Unsafe brand asset ${key}`,
            );
    for (const color of ['background', 'ink', 'accent'])
        assert(HEX.test(config.colors[color] ?? ''), `Missing ${color}`);
    return config;
}
export function brandAsset(config, key) {
    const path = config.brand[key];
    assert(typeof path === 'string' && BRAND_ASSET.test(path), `Unsafe brand asset ${key}`);
    return new URL(`../${path}`, import.meta.url).href;
}
export function themeVariables(config) {
    validateTheme(config);
    const result = {};
    for (const [section, prefix] of [
        ['colors', 'color'],
        ['typography', 'font'],
        ['sizing', 'size'],
    ]) {
        for (const [key, value] of Object.entries(config[section]))
            result[`--${prefix}-${key.replaceAll('_', '-')}`] = value;
    }
    return result;
}
export function applyTheme(config, root = globalThis.document?.documentElement) {
    const vars = themeVariables(config);
    if (!root) throw new Error('Theme root is missing');
    for (const [key, value] of Object.entries(vars)) root.style.setProperty(key, value);
    for (const [key, value] of Object.entries(config.decoration)) root.dataset[key] = String(value);
    return config;
}
