'use strict';

const crypto = require('crypto');
const { prisma } = require('../../prismaClient');

const hashApiKey = (apiKey) =>
  crypto.createHash('sha256').update(apiKey).digest('hex');

const hashesMatch = (providedHash, storedHash) => {
  if (typeof storedHash !== 'string') return false;
  const provided = Buffer.from(providedHash, 'hex');
  const stored = Buffer.from(storedHash, 'hex');
  return provided.length === stored.length && crypto.timingSafeEqual(provided, stored);
};

const validateApiKey = async (apiKey) => {
  if (typeof apiKey !== 'string' || !apiKey.trim()) return null;

  const keyHash = hashApiKey(apiKey.trim());
  const record = await prisma.apiKey.findUnique({ where: { keyHash } });
  if (!record || !hashesMatch(keyHash, record.keyHash) || record.revokedAt) {
    return null;
  }
  if (record.expiresAt && record.expiresAt <= new Date()) return null;

  return record;
};

module.exports = { validateApiKey, hashApiKey };
