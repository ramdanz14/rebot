import { constringDBEDP } from "./config.js";
import mysql from "mysql2";
import mysqlpromise from "mysql2/promise";
import { writeFile, unlink, appendFile } from "fs/promises";
import fs from "fs";
import { stringify } from "csv-stringify";

import moment from "moment";

let conDBEdp = await mysql.createConnection(constringDBEDP);

var tgl_akhir = moment().add(-2, "days");
var tgl_awal = moment().add(-2, "days").startOf("month");

for (let i = tgl_awal.format("D"); i <= tgl_akhir.format("D"); i++) {
  let tgl = moment().date(i);
}

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

query += "SELECT kodegudang,kodetoko,namatoko,tipe_koneksi_primary,amgr,aspv";
for (let i = tgl_awal.format("D"); i <= tgl_akhir.format("D"); i++) {
  query += `,hr${i}.cek as TGL${i}`;
}

query += " FROM tb_toko ";
for (let i = tgl_awal.format("D"); i <= tgl_akhir.format("D"); i++) {
  let tgl = tgl_awal.date(i);
  query += `LEFT JOIN 
  (SELECT kdtk AS kodetoko, updtime, CASE
  WHEN(updtime <= CONCAT(DATE_ADD(STR_TO_DATE(MID(file_harian, 3, 6), '%y%m%d'), INTERVAL 1 DAY), ' 05:00') AND data_ok = 'Y') THEN 'OK'
  WHEN(updtime BETWEEN CONCAT(DATE_ADD(STR_TO_DATE(MID(file_harian, 3, 6), '%y%m%d'), INTERVAL 1 DAY), ' 05:00') AND CONCAT(DATE_ADD(STR_TO_DATE(MID(file_harian, 3, 6), '%y%m%d'), INTERVAL 1 DAY), ' 09:00') AND note NOT LIKE '%gagal%' AND data_ok = 'Y') THEN 'TK'
  WHEN(updtime BETWEEN CONCAT(DATE_ADD(STR_TO_DATE(MID(file_harian, 3, 6), '%y%m%d'), INTERVAL 1 DAY), ' 09:00')AND CONCAT(DATE_ADD(STR_TO_DATE(MID(file_harian, 3, 6), '%y%m%d'), INTERVAL 7 DAY), ' 23:59')  AND note NOT LIKE '%gagal%' AND data_ok = 'Y') THEN 'TN'
  WHEN(note LIKE '%GAGALCLOSING%' AND data_ok = 'Y') THEN 'GC'
  WHEN(note LIKE '%ERRORGAGAL%' AND data_ok = 'Y') THEN 'EG'
  WHEN note LIKE '%STOCKOUT TIDAK WAJAR%'  THEN 'SW'
  WHEN note LIKE '%LIBUR%'  THEN 'LB'
  WHEN note LIKE '%TUTUP SEMENTARA%' THEN 'TS'
  ELSE ''
   END AS cek FROM harian${tgl.format("YYMMDD")}) hr${i} USING(kodetoko)`;
}

query += " WHERE toko_aktif='Y';";

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
conDBEdp.end();

console.log(
  `Finish  processing ${recordsProcessed} records oufile written in file ${namaFile}`
);

process.exit();
