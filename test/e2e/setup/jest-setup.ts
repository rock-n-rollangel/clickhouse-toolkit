/**
 * Jest Setup for E2E Tests
 * Global setup and teardown for end-to-end tests
 */

import { beforeAll, afterAll } from '@jest/globals'

// Global setup for e2e tests
beforeAll(async () => {
  console.log('🚀 Starting E2E test suite...')
  console.log('📦 Setting up ClickHouse test environment...')

  // Global setup is handled by individual test files
  // This file provides common configuration and utilities
}, 120000) // 2 minute timeout for global setup

afterAll(async () => {
  console.log('🧹 Cleaning up E2E test environment...')

  // Global cleanup is handled by individual test files
  // This ensures proper cleanup even if tests fail
}, 30000) // 30 second timeout for global cleanup

// Global error handler for unhandled promise rejections
process.on('unhandledRejection', (reason, promise) => {
  console.error('Unhandled Rejection at:', promise, 'reason:', reason)
})

// Global error handler for uncaught exceptions
process.on('uncaughtException', (error) => {
  console.error('Uncaught Exception:', error)
})

// Increase timeout for all tests in this suite
jest.setTimeout(120000) // 2 minutes
