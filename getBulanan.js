import { constringDBEDP, getConBulanan } from "./config.js";
import mysql from "mysql2";
import mysqlpromise from "mysql2/promise";
import { writeFile, unlink, appendFile } from "fs/promises";
import fs from "fs";
import { stringify } from "csv-stringify";

import moment from "moment";

let conDBEdp = await mysqlpromise.createConnection(constringDBEDP);
let listCabang = [];

// query database
try {
  const [rows, fields] = await conDBEdp.execute(
    "SELECT * FROM `DCMAST` "
    // "SELECT * FROM `DCMAST` "
  );
  listCabang = rows;
} catch (error) {
  console.warn(
    moment(new Date()).format("YYYY-MM-DD hh:mm:ss") + " - ADA " + error
  );
}

if (listCabang) {
  var hasilFinal = [];
  await Promise.all(
    listCabang.map(async (cab) => {
      let kode_cab = cab.kode_cab;
      try {
        const conWRC = await mysqlpromise.createConnection(
          await getConBulanan(kode_cab)
        );
        //      const query = `select * from (select kode_toko,sum(saldo_akh) as saldo_akh from kodetoko_2201 group by kode_toko) t left join
        //    (SELECT kode_toko,SUM(begbal) begbal FROM st_220203 GROUP BY kode_toko) s using(kode_toko) having begbal!=saldo_akh;`;
        //	const query = `SHOW VARIABLES LIKE 'innodb_buffer_pool_size' ;`;
        const query = fs.readFileSync("./selectBulanan.txt").toString();
        console.log("Now select from " + kode_cab);
        let [rows] = await conWRC.query(query);
        let header_cab = { cabang: kode_cab };
        let mergedArray = [];
        rows.forEach((row) => {
          let combo = { ...header_cab, ...row };
          mergedArray.push(combo);
        });

        hasilFinal.push(...mergedArray);
        console.log(kode_cab + "Finish");
      } catch (error) {
        console.log(error);
      }
    })
  ).catch((err) => {
    console.error(err); // Oops!
  });
}

var namaFile = `./hasilbulanan/query.csv`;
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
