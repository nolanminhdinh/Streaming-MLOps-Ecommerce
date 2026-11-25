const path = require('node:path');
const sharp = require('C:/Program Files/WindowsApps/OpenAI.CodexPrimaryRuntime.v26-1105-1297-0_26.1105.1297.0_x64__3k8sg7r9htsxt/dependencies/node/node_modules/sharp');
const out = path.resolve(__dirname, '../docs/07-bao-cao-do-an/evidence/forecast-improvement-2026-11-08');
(async () => {
  for (const name of ['wape-bias', 'source-monthly']) {
    await sharp(path.join(out, name + '.svg'), {density: 180}).png().toFile(path.join(out, name + '.png'));
  }
  process.stdout.write('Rendered both forecast analysis figures.\n');
})().catch(e => { process.stderr.write(String(e)); process.exitCode = 1; });
