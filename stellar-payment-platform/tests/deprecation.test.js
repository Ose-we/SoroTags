'use strict';

const { findDeprecation } = require('../src/middleware/deprecation');

describe('deprecation registry matching', () => {
  it('ignores header-only entries when matching endpoint paths', () => {
    expect(
      findDeprecation('GET', '/federation', [
        { kind: 'header', header: 'X-Stellar-Tags-Signature' },
      ]),
    ).toBeNull();
  });
});
