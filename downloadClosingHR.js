import { constringDBEDP } from "./config.js";
import mysql from "mysql2";
import mysqlpromise from "mysql2/promise";
import { writeFile, unlink, appendFile } from "fs/promises";
import fs from "fs";
import { stringify } from "csv-stringify";

import moment from "moment";

let conDBEdp = await mysql.createConnection(constringDBEDP);

var tgl_akhir = moment().add(-40, "days");
var tgl_awal = moment().add(-40, "days").startOf("month");
// var tgl_akhir = moment("2022-03-31", "YYYY-MM-DD");
// var tgl_awal = moment("2022-03-01", "YYYY-MM-DD");

var namaFile = `./tmp/ABSENHRREKAP${tgl_akhir.format("YYMMDD")}.csv`;
const outputStream = fs.createWriteStream(namaFile, {
  encoding: "utf8",
});

const finishedWriting = new Promise((resolve, reject) =>
  outputStream.on("finish", resolve).on("error", reject)
);

const BOM = "\ufeff"; // Microsoft Excel needs this
outputStream.write(BOM);

let query = "";
// var tgl_akhir = moment().add(-2, "days");
// var tgl_awal = moment().add(-2, "days").startOf("month");

for (let i = tgl_awal.format("D"); i <= tgl_akhir.format("D"); i++) {
  let tgl = tgl_awal.date(i);
  query += `SELECT MID(file_harian, 3, 6) AS tgl,kodegudang,kdtk,(SELECT fullname FROM tb_user WHERE nickname=m.kelompok ) AS pic,CASE WHEN note LIKE '%GAGALCLOSING%'  THEN  'CLOSING ULANG' 
  WHEN note LIKE '%ERRORGAGAL%'  THEN  'CLOSING ULANG ADA ERROR'
  ELSE 'COLLECT MANUAL'
  END AS note FROM harian${tgl.format("YYMMDD")} m WHERE kelompok!='' `;
  if (tgl.format("YYMMDD") != tgl_akhir.format("YYMMDD")) {
    query += " UNION ALL ";
  }
}

//console.log(query);
//process.exit();

console.log(` Now Collecting RekapAbsenHR`);

let recordsProcessed = 0;
try {
  await new Promise((resolve, reject) => {
    // const generator = conWRC.query(query);

    conDBEdp
      .query(query)
      .on("error", (error) => {
        reject(` has Error : ` + error.sqlMessage);
      })
      .on("result", (row) => ++recordsProcessed) // Counting rows just as an example
      .stream({ highWaterMark: 10 })
      .on("error", (error) => {
        reject(`has Error : ` + error.sqlMessage);
      })
      .on("end", resolve)
      .pipe(stringify({ header: true, delimiter: "," }))
      .pipe(outputStream)
      .on("error", (error) => {
        conDBEdp.destroy();
        reject(error);
      });
  });
} catch (err) {
  console.log(err);
} finally {
}

await finishedWriting;

console.log(
  `Finish  processing ${recordsProcessed} records oufile written in file ${namaFile}`
);

process.exit();
