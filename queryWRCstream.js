import { constringDBEDP, getConWRC } from "./config.js";
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
    "SELECT * FROM `DCMAST` where kode_cab in('G117')  "
    // "SELECT * FROM `DCMAST` "
  );
  listCabang = rows;
  conDBEdp.destroy();
} catch (error) {
  console.warn(
    moment(new Date()).format("YYYY-MM-DD hh:mm:ss") + " - ADA " + error
  );
}
conDBEdp.destroy();

if (listCabang) {
  const AllQuery = fs.readFileSync("./selectQuery.txt").toString().split(";");

  for (const query of AllQuery) {
    let position = query.search(":");
    let header = query.substring(0, position).trim();
    header = header.replaceAll(" ", "");
    if (query.length > 10) {
      //USING PARAREL ASYNC
      await Promise.all(
        listCabang.map(async (cab) => {
          try {
            let kode_cab = cab.kode_cab;

            var namaFile = `./hasilwrc/${kode_cab}${header}.csv`;
            const outputStream = fs.createWriteStream(namaFile, {
              encoding: "utf8",
            });

            const conWRC = mysql.createConnection(await getConWRC(kode_cab));

            const finishedWriting = new Promise((resolve, reject) =>
              outputStream.on("finish", resolve).on("error", reject)
            );

            const BOM = "\ufeff"; // Microsoft Excel needs this
            outputStream.write(BOM);

            //const query = fs.readFileSync("./isiquery.txt").toString();
            //console.log(query);
            console.log(`${kode_cab} Now Collecting ${header}`);

            let recordsProcessed = 0;
            try {
              await new Promise((resolve, reject) => {
                // const generator = conWRC.query(query);

                conWRC
                  .query(query)
                  .on("error", (error) => {
                    reject(
                      `Cabang : ${kode_cab} has Error : ` + error.sqlMessage
                    );
                  })
                  .on("result", (row) => ++recordsProcessed) // Counting rows just as an example
                  .stream({ highWaterMark: 10 })
                  .on("error", (error) => {
                    reject(
                      `Cabang : ${kode_cab} has Error : ` + error.sqlMessage
                    );
                  })
                  .on("end", resolve)
                  .pipe(stringify({ header: true, delimiter: "|" }))
                  .pipe(outputStream)
                  .on("error", (error) => {
                    conWRC.destroy();
                    reject(error);
                  });
              });
            } catch (err) {
              console.log(err);
            } finally {
              conWRC.destroy();
            }

            try {
              await finishedWriting;
            } catch (err) {
              console.log("ADA ERROR CAB " + kode_cab);
              console.log(err);
            }

            console.log(
              `Finish ${kode_cab} processing ${recordsProcessed} records oufile written in file ${namaFile}`
            );
          } catch (error) {
            console.log(error);
          }
        })
      );
    }
  }
}

process.exit();
