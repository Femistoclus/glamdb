import test from 'node:test';
import assert from 'node:assert/strict';

test('appearance and navigation helpers exist', async () => {
    const module = await import('../../assets/appearance.js');
    assert.equal(typeof module.applyAppearance, 'function');
    const properties = {};
    const root = { dataset: {}, style: { setProperty: (key, value) => (properties[key] = value) } };
    const theme = {
        colors: {
            background: '#ffffff',
            ink: '#222222',
            accent: '#c41472',
            dark_background: '#111111',
        },
    };
    module.applyAppearance(theme, 'dark', root);
    assert.equal(root.dataset.appearance, 'dark');
    assert.equal(properties['--color-background'], '#111111');
    module.applyAppearance(theme, 'light', root);
    assert.equal(properties['--color-background'], '#ffffff');
    const blocked = {
        getItem() {
            throw new Error('denied');
        },
        setItem() {
            throw new Error('denied');
        },
    };
    assert.equal(module.readPreference('appearance', 'light', blocked), 'light');
    assert.doesNotThrow(() => module.savePreference('appearance', 'dark', blocked));
});

test('creation times sort without guessing snapshot IDs or missing timestamps', async () => {
    const { sortSnapshots, snapshotCreatedAt } = await import('../../assets/snapshot-time.js');
    const old = { snapshot_id: '99', created_at_us: '1700000000000001' };
    const recent = { snapshot_id: '1', created_at_us: '1700000000000002' };
    assert.deepEqual(sortSnapshots([old, recent]), [recent, old]);
    const unknown = { snapshot_id: '100' };
    assert.deepEqual(sortSnapshots([old, unknown, recent]), [old, unknown, recent]);
    assert.equal(snapshotCreatedAt(unknown), null);
    assert.equal(
        snapshotCreatedAt({ created_at: '2026-11-12T12:00:00Z' }).includes('2026-11-12'),
        true,
    );
});
