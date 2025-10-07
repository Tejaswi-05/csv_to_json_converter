require('dotenv').config();
const fs = require('fs');
const { Pool } = require('pg');
const express = require('express');
const CSV_PATH = process.env.CSV_PATH || './data/users.csv';
const DATABASE_URL = process.env.DATABASE_URL;
const BATCH_SIZE = parseInt(process.env.BATCH_SIZE || '1000', 10);
const PORT = parseInt(process.env.PORT || '3000', 10);

if (!DATABASE_URL) {
  console.error('Please set DATABASE_URL in .env');
  process.exit(1);
}
const pool = new Pool({ connectionString: DATABASE_URL });

function setNested(obj, path, value) {
  const parts = path.split('.');
  let cur = obj;
  for (let i = 0; i < parts.length; i++) {
    const p = parts[i];
    if (i === parts.length - 1) cur[p] = value;
    else {
      if (!(p in cur)) cur[p] = {};
      if (typeof cur[p] !== 'object') cur[p] = {};
      cur = cur[p];
    }
  }
}

function parseCSVText(text) {
  const rows = [];
  let i = 0;
  const len = text.length;

  function readLineAsFields() {
    const fields = [];
    let cur = '';
    let inQuotes = false;
    while (i < len) {
      const ch = text[i];
      if (ch === '"') {
        if (inQuotes && i + 1 < len && text[i + 1] === '"') {
          cur += '"';
          i += 2;
          continue;
        }
        inQuotes = !inQuotes;
        i++;
        continue;
      }
      if (!inQuotes && ch === ',') {
        fields.push(cur);
        cur = '';
        i++;
        continue;
      }
      if (!inQuotes && (ch === '\n' || ch === '\r')) {
        fields.push(cur);
        cur = '';
        if (ch === '\r' && i + 1 < len && text[i + 1] === '\n') i += 2; else i++;
        break;
      }
      cur += ch;
      i++;
    }
    if (i >= len) {
      if (cur !== '' || fields.length > 0) fields.push(cur);
    }
    return fields;
  }
  if (text.charCodeAt(0) === 0xfeff) i = 1;
  const headerFields = readLineAsFields();
  if (!headerFields || headerFields.length === 0) throw new Error('No headers found in CSV');
  const headers = headerFields.map(h => h.trim());

  while (i < len) {
    const fields = readLineAsFields();
    if (fields.length === 1 && fields[0] === '' && i >= len) break;
    while (fields.length < headers.length) fields.push('');
    const rec = {};
    for (let k = 0; k < headers.length; k++) rec[headers[k]] = fields[k] !== undefined ? fields[k].trim() : '';
    rows.push(rec);
  }
  return rows;
}

function mapRecordToDb(rec) {
  const firstName = (rec['name.firstName'] || '').trim();
  const lastName = (rec['name.lastName'] || '').trim();
  const ageRaw = (rec['age'] || '').trim();
  if (!firstName || !lastName || ageRaw === '') {
    throw new Error(`Mandatory fields missing: ${JSON.stringify({ firstName, lastName, age: ageRaw })}`);
  }
  const age = parseInt(ageRaw, 10);
  if (Number.isNaN(age)) throw new Error(`Invalid age value: "${ageRaw}"`);
  const nameCombined = `${firstName} ${lastName}`;

  const address = {};
  const additional = {};
  for (const [k, vRaw] of Object.entries(rec)) {
    const v = vRaw === undefined ? '' : vRaw;
    if (k === 'name.firstName' || k === 'name.lastName' || k === 'age') continue;
    if (k.startsWith('address.')) {
      const subPath = k.slice('address.'.length);
      setNested(address, subPath, v);
    } else {
      setNested(additional, k, v);
    }
  }
  return {
    name: nameCombined,
    age,
    address: Object.keys(address).length ? address : null,
    additional_info: Object.keys(additional).length ? additional : null
  };
}

async function insertBatch(client, rows) {
  if (!rows.length) return;
  const cols = ['"name"', 'age', 'address', 'additional_info'];
  const vals = [];
  const placeholders = [];
  let paramIdx = 1;
  for (const r of rows) {
    placeholders.push(`($${paramIdx++}, $${paramIdx++}, $${paramIdx++}::jsonb, $${paramIdx++}::jsonb)`);
    vals.push(r.name);
    vals.push(r.age);
    vals.push(r.address ? JSON.stringify(r.address) : null);
    vals.push(r.additional_info ? JSON.stringify(r.additional_info) : null);
  }
  const query = `INSERT INTO public.users (${cols.join(',')}) VALUES ${placeholders.join(',')}`;
  await client.query(query, vals);
}

async function printAgeDistribution(client) {
  const totalRes = await client.query('SELECT COUNT(*)::int as cnt FROM public.users');
  const total = parseInt(totalRes.rows[0].cnt, 10) || 0;
  if (total === 0) { console.log('No records in users table.'); return; }
  const q = `
    SELECT
      SUM(CASE WHEN age < 20 THEN 1 ELSE 0 END) AS lt20,
      SUM(CASE WHEN age >=20 AND age <=40 THEN 1 ELSE 0 END) AS btw20_40,
      SUM(CASE WHEN age >40 AND age <=60 THEN 1 ELSE 0 END) AS btw40_60,
      SUM(CASE WHEN age >60 THEN 1 ELSE 0 END) AS gt60
    FROM public.users;
  `;
  const res = await client.query(q);
  const row = res.rows[0];
  const pct = n => (total === 0 ? 0 : Math.round((n / total) * 100));
  console.log('Age-Group % Distribution');
  console.log(`< 20 : ${pct(parseInt(row.lt20 || 0, 10))}`);
  console.log(`20 to 40 : ${pct(parseInt(row.btw20_40 || 0, 10))}`);
  console.log(`40 to 60 : ${pct(parseInt(row.btw40_60 || 0, 10))}`);
  console.log(`> 60 : ${pct(parseInt(row.gt60 || 0, 10))}`);
}
async function importCsvFile(filePath) {
  console.log(`Starting import from: ${filePath}`);
  const text = fs.readFileSync(filePath, { encoding: 'utf8' });
  console.log('Parsing CSV...');
  const records = parseCSVText(text);
  console.log(`Parsed ${records.length} records.`);
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    let batch = [];
    let inserted = 0;
    for (let i = 0; i < records.length; i++) {
      const rec = records[i];
      let mapped;
      try { mapped = mapRecordToDb(rec); }
      catch (err) { console.error(`Skipping row ${i + 2} due to mapping error:`, err.message); continue; }
      batch.push(mapped);
      if (batch.length >= BATCH_SIZE) { await insertBatch(client, batch); inserted += batch.length; console.log(`Inserted ${inserted} rows...`); batch = []; }
    }
    if (batch.length > 0) { await insertBatch(client, batch); inserted += batch.length; console.log(`Inserted ${inserted} rows (final).`); }
    await client.query('COMMIT');
    console.log('Import committed.');
    await printAgeDistribution(client);
  } catch (err) {
    await client.query('ROLLBACK').catch(() => {});
    console.error('Import failed, rolled back. Error:', err.message);
    throw err;
  } finally { client.release(); }
}

const app = express();

async function startServer() {
  try {
    console.log(`Server starting on port ${PORT}...`);
    app.get('/', (req, res) => res.send('CSV -> JSON -> Postgres Importer running...'));

    // Start listening
    app.listen(PORT, async () => {
      console.log(`✅ Server running on port ${PORT}`);
      console.log(`📁 Using CSV_PATH=${CSV_PATH}`);
      console.log(`🚀 Starting CSV import automatically...\n`);

      try {
        await importCsvFile(CSV_PATH);
        console.log('Import finished successfully.');
      } catch (err) {
        console.error('Import failed:', err.message);
      } finally {
        console.log('\nServer is still running. Press Ctrl + C to stop.');
      }
    });
  } catch (err) {
    console.error('Failed to start server:', err.message);
    process.exit(1);
  }
}

startServer();