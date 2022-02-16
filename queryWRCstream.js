import { constringDBEDP, getConWRC } from "./config.js";
import mysql from "mysql2";
import mysqlpromise from "mysql2/promise";
import { writeFile, unlink, appendFile } from "fs/promises";
import fs from "fs";
import { stringify } from "csv-stringify";

import moment from "moment";

let conDBEdp = await mysqlpromise.createConnection(constringDBEDP);
let listCabang = [{ kode_cab: "G117" }];

// query database
try {
  const [rows, fields] = await conDBEdp.execute(
    "SELECT * FROM `DCMAST` where kode_cab not in('G117') "
    // "SELECT * FROM `DCMAST` "
  );
  listCabang = rows;
} catch (error) {
  console.warn(
    moment(new Date()).format("YYYY-MM-DD hh:mm:ss") + " - ADA " + error
  );
}

if (listCabang) {
  let hasilFinal = [];

  //USING PARAREL ASYNC
  await Promise.all(
    listCabang.map(async (cab) => {
      let kode_cab = cab.kode_cab;

      var namaFile = `./tmp/tes-${kode_cab}.csv`;
      const outputStream = fs.createWriteStream(namaFile, {
        encoding: "utf8",
      });

      const conWRC = mysql.createConnection(await getConWRC(kode_cab));

      const finishedWriting = new Promise((resolve, reject) =>
        outputStream.on("finish", resolve).on("error", reject)
      );

      const BOM = "\ufeff"; // Microsoft Excel needs this
      outputStream.write(BOM);

      const query = fs.readFileSync("./isiquery.txt").toString();
      //console.log(query);
      console.log("NOW SELECTING FROM " + kode_cab);
      const generator = conWRC.query(query);
      let recordsProcessed = 0;

      try {
        await new Promise((resolve, reject) => {
          conWRC.on("error", reject);

          generator
            .on("result", (row) => ++recordsProcessed) // Counting rows just as an example
            .stream({ highWaterMark: 10 })
            .on("error", reject)
            .on("end", resolve)
            .pipe(stringify({ header: true, delimiter: "|" }))
            .pipe(outputStream)
            .on("error", (error) => {
              conWRC.destroy();
              reject(error);
            });
        });
      } finally {
        conWRC.on("error", (error) => console.log(error));
      }

      await finishedWriting;

      console.log(
        `Finish ${kode_cab} processing ${recordsProcessed} records oufile written in file ${namaFile}`
      );
    })
  );
}

process.exit();
