import fs from 'node:fs/promises';
import {createRequire} from 'node:module';
const require=createRequire('C:/Users/MINH/.cache/codex-runtimes/codex-primary-runtime/dependencies/node/package.json');
const {FileBlob,SpreadsheetFile}=await import((await import('node:url')).pathToFileURL(require.resolve('@oai/artifact-tool')).href); 
const w=await SpreadsheetFile.importXlsx(await FileBlob.load('C:/Users/MINH/Downloads/Project Management Document (1).xlsx'));
const p=await w.render({sheetName:'DATA DICTIONARY',range:'A1:H10',scale:1,format:'png'});
await fs.writeFile('template-preview.png',new Uint8Array(await p.arrayBuffer()));
console.log('Rendered template');


