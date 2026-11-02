import { defineConfig, devices } from '@playwright/test';

export default defineConfig({
  testDir: './specs',
  fullyParallel: true,
  workers: process.env.CI ? 2 : 3,
  forbidOnly: Boolean(process.env.CI),
  retries: 0,
  // Cold WebKit page creation consumed ~19s of the old 20s test budget on CI.
  // Allow browser setup plus the test body; keep assertions bounded at 5s.
  timeout: 60_000,
  expect: { timeout: 5_000 },
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
