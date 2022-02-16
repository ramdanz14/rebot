import { constringDBEDP, getConToko } from "./config.js";
import mysql from "mysql2/promise";
import { writeFile, unlink } from "fs/promises";
import fs from "fs";
import { Parser } from "json2csv";
import moment from "moment";
import ping from "ping";

const poolOptions = { connectionLimit: 20, ...constringDBEDP };
const pool = await mysql.createPool(poolOptions);
//const conEDP = await mysql.createConnection(constringDBEDP);

const d = moment(new Date()).add(-1, "days");
const tableName = "harian" + moment(d).format("YYMMDD");
const query = `SELECT h.kodegudang,kodetoko,namatoko,kelompok, tipe_koneksi_primary,ifnull(ipaddress,'172.0.0.1') as ip_induk,data_ok,note FROM tb_toko LEFT JOIN rekap_ip m  ON kdtk=kodetoko LEFT JOIN ${tableName} h ON kodetoko=h.kdtk where toko_aktif='Y' AND   (m.jenis = 'INDUK' or jenis is null)   and tglbuka<=curdate() AND note not in('LIBUR','TUTUP SEMENTARA') AND data_ok!='Y'`;
let listToko = [];
try {
  const [rows, fields] = await pool.query(query);
  if (rows) {
    listToko = rows;
  }
} catch (error) {
  console.error(
    moment(new Date()).format("YYYY-MM-DD hh:mm:ss") + " - ADA " + error
  );
}
if (listToko) {
  await Promise.all(
    listToko.map(async (toko) => {
      if (toko.note.includes("TIDAK WAJAR")) {
        console.info(`TOKO ${toko.kodetoko} SKIP CHECK NOTE : ${toko.note}`);
      } else if(toko.note.includes("CLOSING")){
	console.info(`TOKO ${toko.kodetoko} SKIP CHECK NOTE : ${toko.note}`);
	}else{
        let res = await ping.promise.probe(toko.ip_induk);
        if (res.alive) {
          // console.info(`KONEKSI TOKO ${toko.kodetoko} ON NOTE : ${toko.note}`);

          try {
            const contoko = await mysql.createConnection(
              await getConToko(toko.ip_induk)
            );
            const [rows] =
              await contoko.execute(`SELECT CASE WHEN (recid='C' AND cardno IS NULL) THEN 'GAGALCLOSING'  WHEN (recid='P' AND cardno IS NULL) THEN 'BELUMCLOSING' ELSE '' END  AS cek FROM (
              SELECT GROUP_CONCAT(DISTINCT recid) AS recid,GROUP_CONCAT(DISTINCT cardno) AS cardno FROM initial WHERE tanggal='${moment(
                d
              ).format("YYYY-MM-DD")}') a`);
            // console.log(rows);

            if (rows[0].cek != "") {
              console.warn(
                `${toko.kodegudang} - ${toko.kodetoko} : ${rows[0].cek} `
              );
              await pool.execute(
                `UPDATE  ${tableName}  set note='${rows[0].cek}' where kdtk='${toko.kodetoko}' AND note='${toko.note}';`
              );
            } else {
              console.warn(
                `${toko.kodegudang} - ${toko.kodetoko} : TIDAK GAGAL CLOSING `
              );
            }
          } catch (error) {
            console.error(
              `Error checking : ${toko.kodegudang} - ${toko.kodetoko}  ` + error
            );
          }
        } else {
          console.warn(`KONEKSI TOKO ${toko.kodetoko} OFF NOTE : ${toko.note}`);
        }
      }
    })
  );
}

await pool.end();
process.exit();
