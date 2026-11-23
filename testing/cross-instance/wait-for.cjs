const assert = require('node:assert/strict');
const { setTimeout: delay } = require('node:timers/promises');

module.exports = async function waitFor(label, get, expected) {
    const deadline = Date.now() + 10000;
    while (true) {
        const actual = await get();
        try {
            assert.deepEqual(actual, expected, label);
            return;
        } catch (error) {
            if (Date.now() >= deadline) throw error;
        }
        await delay(25);
    }
};
