// Run with Node and an installed @electric-sql/pglite module path as argv[2].
// PostgreSQL runs in memory. No production database is touched.
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { execFileSync } from 'node:child_process';
import { resolve } from 'node:path';
import { pathToFileURL } from 'node:url';

const { PGlite } = await import(process.argv[2] ? pathToFileURL(resolve(process.argv[2])).href : '@electric-sql/pglite');
const db = new PGlite();
const ddl = readFileSync('warehouse/ddl/01_star_schema.sql', 'utf8');
const views = readFileSync('powerbi/views_for_powerbi.sql', 'utf8');
const oldDdl = execFileSync('git', ['show', '6f78af7:warehouse/ddl/01_star_schema.sql'], { encoding: 'utf8' });
const oldViews = execFileSync('git', ['show', '6f78af7:powerbi/views_for_powerbi.sql'], { encoding: 'utf8' });
let checks = 0;

async function isolated(fn) {
  await db.exec('BEGIN; CREATE SCHEMA audit_test; SET LOCAL search_path TO audit_test;');
  try { await fn(); checks++; } finally { await db.exec('ROLLBACK;'); }
}
async function install() { await db.exec(ddl); await db.exec(views); }
async function product(sku) {
  const { rows } = await db.query('INSERT INTO Dim_Products(sku, product_name) VALUES ($1,$1) RETURNING product_key', [sku]);
  return rows[0].product_key;
}
async function live(sku, day, qty, revenue, id) {
  const { rows } = await db.query('SELECT product_key FROM Dim_Products WHERE sku=$1', [sku]);
  await db.query(`INSERT INTO Fact_Orders(product_key,order_id,platform,order_status,quantity,buyer_total_amount,create_time)
    VALUES($1,$2,'shopee','COMPLETED',$3,$4,$5)`, [rows[0].product_key, id, qty, revenue, day]);
}
async function historical(sku, day, qty, dataset, id) {
  await db.query(`INSERT INTO Fact_Historical_Order_Lines(source_dataset,source_record_id,data_origin,platform,sku,order_status,quantity,create_time)
    VALUES($1,$2,'public_anonymized_historical_csv','shopee',$3,'COMPLETED',$4,$5)`, [dataset,id,sku,qty,day]);
}
async function forecast(key, origin, datasets, day='2026-07-02') {
  await db.query(`INSERT INTO Fact_Forecast_Predictions(product_key,target_date,predicted_quantity,model_name,model_version,model_source,history_source,history_end_date,forecast_generated_at,data_origin,source_datasets)
    VALUES($1,$2,5,'test','1','mlflow_registry','warehouse','2026-07-01','2026-07-01T00:00:00Z',$3,$4::JSONB)`, [key,day,origin,JSON.stringify(datasets)]);
}
async function coverage(origin, dataset, day='2026-07-02') {
  await db.query('INSERT INTO Warehouse_Daily_Coverage(data_origin,source_dataset,full_date) VALUES($1,$2,$3)',[origin,dataset,day]);
}

try {
  await isolated(async () => {
    await db.exec(oldDdl); await db.exec(oldViews);
    const key = await product('OLD');
    await db.query(`INSERT INTO Fact_Forecast_Predictions(product_key,target_date,predicted_quantity,model_name,model_version,model_source,history_source,forecast_generated_at)
      VALUES($1,'2026-07-02',5,'test','1','mlflow_registry','warehouse','2026-07-01T00:00:00Z')`,[key]);
    await install(); await install();
    const row = (await db.query('SELECT data_origin,actual_demand,actual_complete FROM vw_powerbi_forecast_vs_actual')).rows[0];
    assert.equal(row.data_origin,'unknown'); assert.equal(row.actual_demand,null); assert.equal(row.actual_complete,false);
  });
  console.log('PASS: upgrade existing views, preserve unknown forecast provenance, rerun migration');

  const cases = JSON.parse(readFileSync(process.argv[3], 'utf8'));
  for (const sample of cases) {
    await isolated(async () => {
      await install();
      for (const sku of new Set(sample.orders.map(x=>x.sku))) await product(sku);
      for (const [i,r] of sample.orders.entries()) await live(r.sku,r.create_time,r.quantity,r.buyer_total_amount,`O${i}`);
      await db.exec("INSERT INTO Fact_Inventory_Daily(product_key,date_key,stock_on_hand) SELECT product_key,(SELECT date_key FROM Dim_Dates WHERE full_date='2026-07-03'),100 FROM Dim_Products");
      const rows = (await db.query('SELECT * FROM vw_powerbi_abc_xyz_by_origin')).rows;
      const inventory = (await db.query('SELECT * FROM vw_powerbi_inventory_health')).rows;
      for (const e of sample.expected) {
        const actual = rows.find(x=>x.sku===e.sku);
        assert.equal(actual.abc_class,e.abc_class); assert.equal(actual.xyz_class,e.xyz_class);
        assert.ok(Math.abs(Number(actual.mean_demand_exact)-e.avg_daily_demand)<1e-9);
        assert.ok(Math.abs(Number(actual.std_demand_exact)-e.std_daily_demand)<1e-9);
        const stock = inventory.find(x=>x.sku===e.sku);
        assert.equal(Number(stock.safety_stock),e.safety_stock);
        assert.equal(Number(stock.reorder_point),e.reorder_point);
      }
    });
  }
  console.log(`PASS: SQL/Python ABC/XYZ parity for ${cases.length} datasets`);

  await isolated(async () => {
    await install(); const key = await product('SAME');
    await live('SAME','2026-07-02',99,100,'L1');
    await historical('SAME','2026-07-02',3,'file-a','A1');
    await historical('SAME','2026-07-02',4,'file-b','B1');
    await historical('SAME','2026-07-02',1000,'unrelated-file','C1');
    await forecast(key,'public_anonymized_historical_csv',['file-a','file-b']);
    await forecast(key,'unverified_warehouse',[]);
    let rows = (await db.query('SELECT * FROM vw_powerbi_forecast_vs_actual')).rows;
    assert.equal(rows.length,2); assert.ok(rows.every(r=>r.actual_demand===null));
    await coverage('public_anonymized_historical_csv','file-a');
    rows = (await db.query('SELECT * FROM vw_powerbi_forecast_vs_actual')).rows;
    assert.ok(rows.every(r=>r.actual_demand===null));
    await coverage('public_anonymized_historical_csv','file-b');
    await coverage('unverified_warehouse','');
    rows = (await db.query('SELECT * FROM vw_powerbi_forecast_vs_actual')).rows;
    assert.equal(Number(rows.find(r=>r.data_origin==='public_anonymized_historical_csv').actual_demand),7);
    assert.equal(Number(rows.find(r=>r.data_origin==='unverified_warehouse').actual_demand),99);
    assert.equal(Number(rows.find(r=>r.data_origin==='public_anonymized_historical_csv').absolute_error),2);
    await forecast(key,'public_anonymized_historical_csv',['file-a','file-b'],'2026-07-03');
    await coverage('public_anonymized_historical_csv','file-a','2026-07-03');
    await coverage('public_anonymized_historical_csv','file-b','2026-07-03');
    const zero = (await db.query("SELECT * FROM vw_powerbi_forecast_vs_actual WHERE date='2026-07-03'")).rows[0];
    assert.equal(Number(zero.actual_demand),0); assert.equal(zero.actual_complete,true);
  });
  console.log('PASS: same SKU across sources, fingerprint filtering, partial/full coverage, confirmed zero-sales day');
  if (process.argv[4]) {
    await isolated(async () => {
      await install(); await historical('HISTORY-ONLY','2026-07-02',7,'file-a','H1');
      const queries = JSON.parse(readFileSync(process.argv[4], 'utf8'));
      for (const query of queries) await db.query(query);
    });
    console.log('PASS: all Power BI exporter queries execute on CSV-only warehouse');
  }
  console.log(`${checks} PostgreSQL checks passed`);
} finally { await db.close(); }
