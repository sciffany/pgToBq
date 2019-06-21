const fs = require("fs");
const { Pool } = require("pg");
const copyTo = require("pg-copy-streams").to;
const { BigQuery } = require("@google-cloud/bigquery");
import { config } from "dotenv";
config();

const projectId = process.env.BQPROJECT;
const datasetId = process.env.BQDATASET;

const tableFolder = "tables";
const tableNamesFile = "tableNames.txt";

const pool = new Pool({
  user: process.env.PGUSER,
  host: process.env.PGHOST,
  database: process.env.PGDATABASE,
  password: process.env.PGPASSWORD,
  port: process.env.PGPORT,
  max: 25
});

async function getTablesNames() {
  return fs
    .readFileSync(tableNamesFile, "utf8")
    .split("\n")
    .map(tableName => tableName.replace(/ /g, ""));
}

async function downloadTable(table) {
  return new Promise(async (resolve, reject) => {
    const client = await pool.connect();

    // await client.query(`COPY (SELECT * FROM "Shifts") TO 'tables/Shifts.csv'`);
    const stream = await client.query(
      copyTo(`COPY "${table}" TO STDOUT With CSV HEADER`)
    );
    const fileStream = await fs.createWriteStream(
      `${tableFolder}/${table}.csv`
    );

    stream.pipe(fileStream);
    fileStream.on("error", console.log);
    stream.on("error", console.log);
    stream.on("end", err => {
      console.log(`Downloaded ${table}`);
      resolve();
      client.release();
    });
  }).catch(console.log);
}

async function downloadTables(tables) {
  tables = ["Shifts"];
  return await Promise.all(
    tables.map(async table => {
      await downloadTable(table);
    })
  );
}

async function uploadTable(table) {
  const filename = `${tableFolder}/${table}.csv`;

  const bigquery = new BigQuery({ projectId, datasetId });
  const metadata = {
    sourceFormat: "CSV",
    skipLeadingRows: 1,
    autodetect: true,
    location: "asia-southeast1",
    writeDisposition: "WRITE_TRUNCATE"
  };
  const [job] = await bigquery

    .dataset(datasetId)
    .table(table)
    .load(filename, metadata);

  console.log(`${table}\n Job ${job.id} completed. `);

  const errors = job.status.errors;
  if (errors && errors.length > 0) {
    throw errors;
  }
}

async function pgToBQ() {
  try {
    const tables = await getTablesNames();
    await downloadTables(tables);
    await tables.map(table => uploadTable(table));
  } catch (e) {
    console.log(e.stack);
  }
}

pgToBQ();
