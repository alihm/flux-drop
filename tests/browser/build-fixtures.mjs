// Generated bundles contain local, pinned framework dependencies; no CDN needed.
import { build } from 'esbuild';
import { mkdir } from 'node:fs/promises';

await mkdir('generated', { recursive: true });
for (const framework of ['react', 'vue']) {
  await build({
    entryPoints: [`fixtures/${framework}.js`],
    outfile: `generated/${framework}.js`,
    bundle: true,
    minify: true,
    format: 'esm',
    define: { 'process.env.NODE_ENV': '"production"' },
  });
}
