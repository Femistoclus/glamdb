import test from 'node:test';
import assert from 'node:assert/strict';
import { validateTheme, themeVariables, applyTheme, brandAsset } from '../../assets/theme.js';

const fixture = () => ({
    version: 1,
    brand: { name: 'GlamDB', eyebrow: 'DATA', tagline: 'Pink' },
    colors: { background: '#fff2fa', ink: '#46152f', accent: '#c41472' },
    typography: { body: 'Arial, sans-serif', display: 'Georgia, serif', mono: 'monospace' },
    sizing: { radius: '24px', gap: '1.5rem' },
    decoration: { sparkles: true, bows: true, kisses: false, motion: false },
    labels: { nav_data: 'Data studio' },
});
test('runtime config becomes safe CSS variables including fonts and spacing', () => {
    const vars = themeVariables(fixture());
    assert.equal(vars['--color-accent'], '#c41472');
    assert.equal(vars['--font-display'], 'Georgia, serif');
    assert.equal(vars['--size-radius'], '24px');
});
test('unsafe CSS, malformed dimensions and unsupported versions rejected', () => {
    for (const mutate of [
        (c) => (c.colors.accent = 'url(https://evil)'),
        (c) => (c.typography.body = 'Arial;display:none'),
        (c) => (c.sizing.gap = 'url(x)'),
        (c) => (c.version = 2),
        (c) => (c.decoration.bows = 'yes'),
    ]) {
        const config = fixture();
        mutate(config);
        assert.throws(() => validateTheme(config));
    }
});
test('theme updates decorations and labels without rebuilding DOM', () => {
    const properties = {};
    const root = { style: { setProperty: (k, v) => (properties[k] = v) }, dataset: {} };
    applyTheme(fixture(), root);
    assert.equal(properties['--color-accent'], '#c41472');
    assert.equal(root.dataset.bows, 'true');
    assert.equal(root.dataset.motion, 'false');
    const changed = fixture();
    changed.colors.accent = '#990066';
    changed.decoration.bows = false;
    applyTheme(changed, root);
    assert.equal(properties['--color-accent'], '#990066');
    assert.equal(root.dataset.bows, 'false');
});
test('brand assets are configurable local paths and reject remote or traversing URLs', () => {
    const config = fixture();
    config.brand.wordmark = 'assets/brand/wordmark.png';
    config.brand.mark = 'assets/brand/mark.png';
    assert.match(brandAsset(validateTheme(config), 'wordmark'), /\/assets\/brand\/wordmark.png$/);
    for (const path of [
        'https://example.com/logo.png',
        '//evil/logo.png',
        'assets/brand/../secret.png',
        'javascript:alert(1)',
        'assets/brand/logo.png?x=1',
    ]) {
        config.brand.wordmark = path;
        assert.throws(() => validateTheme(config));
    }
});

test('optional editor query limit is backward compatible and accepts only bounded integers', () => {
    assert.doesNotThrow(() => validateTheme(fixture()));
    const configured = fixture();
    configured.editor = { query_max_rows: 10000 };
    assert.equal(validateTheme(configured).editor.query_max_rows, 10000);
    for (const invalid of [0, 10001, true, 1.5, '10000', null]) {
        const config = fixture();
        config.editor = { query_max_rows: invalid };
        assert.throws(() => validateTheme(config));
    }
    assert.throws(() => validateTheme({ ...fixture(), editor: { extra: 1 } }));
});
