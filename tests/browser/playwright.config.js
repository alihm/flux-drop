import { defineConfig, devices } from '@playwright/test';

export default defineConfig({
  testDir: './specs',
  fullyParallel: true,
  workers: 3,
  forbidOnly: Boolean(process.env.CI),
  retries: 0,
  timeout: 20_000,
  reporter: 'list',
  use: {
    baseURL: process.env.DROP_TEST_BASE_URL || 'https://localhost:18443',
    ignoreHTTPSErrors: true,
    trace: 'retain-on-failure',
  },
  projects: [
    { name: 'chromium', use: { ...devices['Desktop Chrome'] } },
    { name: 'firefox', use: { ...devices['Desktop Firefox'] } },
    { name: 'webkit', use: { ...devices['Desktop Safari'] } },
  ],
});
