function micros(snapshot) {
    for (const [name, scale] of [
        ['created_at_us', 1n],
        ['created_at_ms', 1000n],
        ['timestamp_ms', 1000n],
    ]) {
        const value = snapshot[name];
        if (
            (typeof value === 'string' && /^\d+$/.test(value)) ||
            (Number.isSafeInteger(value) && value >= 0)
        )
            return BigInt(value) * scale;
    }
    if (
        typeof snapshot.created_at === 'string' &&
        /^\d{4}-\d{2}-\d{2}T/.test(snapshot.created_at)
    ) {
        const time = Date.parse(snapshot.created_at);
        if (Number.isFinite(time)) return BigInt(time) * 1000n;
    }
    return null;
}
export function sortSnapshots(snapshots) {
    const tagged = snapshots.map((snapshot, index) => ({
        snapshot,
        index,
        time: micros(snapshot),
    }));
    if (tagged.some(({ time }) => time === null)) return [...snapshots];
    return tagged
        .sort((a, b) => (a.time > b.time ? -1 : a.time < b.time ? 1 : a.index - b.index))
        .map(({ snapshot }) => snapshot);
}
export function snapshotCreatedAt(snapshot) {
    const value = micros(snapshot);
    if (value === null) return null;
    const date = new Date(Number(value / 1000n));
    return Number.isFinite(date.getTime())
        ? date.toISOString().replace('T', ' ').replace('.000Z', ' UTC')
        : null;
}
