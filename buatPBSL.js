import { getConWRC } from "./config.js";
import mysql from "mysql2/promise";
import { writeFile, unlink } from "fs/promises";
import fs from "fs";
import { Parser } from "json2csv";
import moment from "moment";

//console.time("start");
const args = process.argv.slice(2);

if (args.length <= 2) {
  console.log(
    "Please use argument as parameter ex : node buatPBSL.js G113 2022-02-01"
  );
  process.exit();
}
const tgl = args[1];
const cabang = args[0];
const conWRC = await mysql.createConnection(await getConWRC(cabang));
const d = new Date(tgl);
let listToko = [];
let listPBS = [];
try {
  console.log("START SELECT LIST TOKO");
  const [rows, fields] = await conWRC.execute(
    "SELECT DISTINCT toko FROM pbs_" + moment(d).format("YYMMDD") + "  "
  );
  listToko = rows;
} catch (error) {
  console.warn(
    moment(new Date()).format("YYYY-MM-DD hh:mm:ss") + " - ADA " + error
  );
}

// try {
//   console.log("START AMBIL ALL PBS");
//   const query =
//     "SELECT RECID,RTYPE,DOCNO,PRDCD,SINGKAT,`DIV`,QTY,TOKO,TGL_PB,PKM_AKH,ROUND(`LENGTH`,2) `LENGTH`,ROUND(WIDTH,2) WIDTH,ROUND(HEIGHT,2)HEIGHT,ROUND(MINOR) AS MINOR,GUDANG,ROUND(`MAX`) AS `MAX`,PKM_G,QTYM1,SPD,PRICE,GROSS,PTAG,ROUND(QTY_MAN) QTY_MAN, IF(ROUND(PPN)=0,'',ROUND(PPN)) PPN,IF(KM=0,'',KM) KM,JAMIN,JAMOUT,NO_SJ   FROM pbs_" +
//     moment(d).format("YYMMDD") +
//     "  WHERE TOKO in('TR72','TB40')";
//   const [rows, fields] = await conWRC.execute(query);
//   listPBS = rows;
// } catch (error) {
//   console.warn(
//     moment(new Date()).format("YYYY-MM-DD hh:mm:ss") + " - ADA " + error
//   );
// }

if (listToko) {
  await Promise.all(
    listToko.map(async (toko) => {
      const query =
        "SELECT RECID,RTYPE,DOCNO,PRDCD,SINGKAT,`DIV`,QTY,TOKO,TGL_PB,PKM_AKH,ROUND(`LENGTH`,2) `LENGTH`,ROUND(WIDTH,2) WIDTH,ROUND(HEIGHT,2)HEIGHT,ROUND(MINOR) AS MINOR,GUDANG,ROUND(`MAX`) AS `MAX`,PKM_G,QTYM1,SPD,PRICE,GROSS,PTAG,ROUND(QTY_MAN) QTY_MAN, IF(ROUND(PPN)=0,'',ROUND(PPN)) PPN,IF(KM=0,'',KM) KM,JAMIN,JAMOUT,NO_SJ   FROM pbs_" +
        moment(d).format("YYMMDD") +
        "  WHERE TOKO in('" +
        toko.toko +
        "')";
      const [rows, fields] = await conWRC.execute(query);

      //console.log(query);
      var namaFile = `./HASIL/${moment(d).format("DD")}/PBSLDEC${cabang}${
        toko.toko
      }${moment(d).format("YYMMDD")}.CSV`;

      if (rows.length > 0) {
        const json2csvParser = new Parser({ delimiter: "|", quote: "" });

        const csv = json2csvParser.parse(rows);

        //console.table(csv);
        if (csv.length > 1) {
          await writeFile(namaFile, csv + "\r\n", { encoding: "utf-8" });
          console.log("Finish CREATE " + namaFile);
        }
      } else {
        console.log(toko.toko + "HASIL KOSONG");
      }
    })
  );

  console.log("FINISH");
}
process.exit();
