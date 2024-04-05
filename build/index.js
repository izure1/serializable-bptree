const esbuild = require('esbuild')

const common = {
  target: 'esnext',
  entryPoints: [
    { in: 'src/index.ts', out: 'index' }
  ]
}

esbuild.build({
  ...common,
  outdir: 'dist/esm',
  outExtension: {
    '.js': '.mjs'
  },
  bundle: true,
  format: 'esm',
})

esbuild.build({
  ...common,
  outdir: 'dist/cjs',
  outExtension: {
    '.js': '.cjs'
  },
  bundle: true,
  format: 'cjs',
})
