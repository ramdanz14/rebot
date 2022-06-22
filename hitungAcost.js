import { constringDBEDP, getConBulanan } from "./config.js";
import mysql from "mysql2";
import mysqlpromise from "mysql2/promise";
import { writeFile, unlink, appendFile } from "fs/promises";
import fs from "fs";
import { stringify } from "csv-stringify";

import moment from "moment";
import { Console } from "console";

var hasilFinal = [];

let kode_cab = "G117";
try {
  const conBulanan = await mysqlpromise.createConnection(
    await getConBulanan(kode_cab)
  );

  const query = `SELECT prdcd FROM mstran_tnpe_2205 WHERE  rtype='B' group by prdcd`;
  let [rows] = await conBulanan.query(query);

  await Promise.all(
    rows.map(async (row) => {
      const query = `SELECT tgl1,prdcd,qty,price,gross FROM mstran_tnpe_2205 WHERE prdcd=${row.prdcd} AND rtype='B' ORDER BY tgl1`;
      let [rows] = await conBulanan.query(query);
      let saldo_akh = 0;
      let rp_saldo_akh = 0;
      for (let i = 0; i < rows.length; i++) {
        let acost_seharusnya = 0;
        let barisakhir = i == rows.length - 1 ? "Y" : "N";
        if (i == 0) {
          acost_seharusnya = rows[i].price;
          saldo_akh += parseFloat(rows[i].qty);
          rp_saldo_akh += parseFloat(rows[i].gross);
          console.log(
            `TGL ${rows[i].tgl1} : PRDCD : ${rows[i].prdcd}  ACOST : ${acost_seharusnya} SALDO_AKH : ${rp_saldo_akh}`
          );

          hasilFinal.push({
            ...rows[i],
            acostbetul: acost_seharusnya,
            barisakhir: barisakhir,
          });
        } else {
          acost_seharusnya =
            (rp_saldo_akh + parseFloat(rows[i].gross)) /
            (saldo_akh + parseFloat(rows[i].qty));
          saldo_akh += parseFloat(rows[i].qty);
          rp_saldo_akh += parseFloat(rows[i].gross);
          console.log(
            `TGL ${rows[i].tgl1} : PRDCD  ${rows[i].prdcd} :  ACOST : ${acost_seharusnya} SALDO_AKH : ${rp_saldo_akh}`
          );
          hasilFinal.push({
            ...rows[i],
            acostbetul: acost_seharusnya,
            barisakhir: barisakhir,
          });
        }
      }
    })
  );

  console.log(kode_cab + "Finish");
  //console.log(hasilFinal);
} catch (error) {
  console.log(error);
}

var namaFile = `./tmp/hasil_acost_tnpe.csv`;
const outputStream = fs.createWriteStream(namaFile, {
  encoding: "utf8",
});
const finishedWriting = new Promise((resolve, reject) =>
  outputStream.on("finish", resolve).on("error", reject)
);

stringify(hasilFinal, {
  header: true,
  bom: true,
}).pipe(outputStream);

await finishedWriting;
console.log(moment().format("DD-MM-YYYY HH:mm:ss") + " - Finish Import to CSV");

process.exit();
