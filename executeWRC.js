import { constringDBEDP, getConWRC } from "./config.js";
import mysql from "mysql2/promise";
import { writeFile, unlink, appendFile } from "fs/promises";
import fs from "fs";
import { stringify } from "csv-stringify";
import moment from "moment";

let conDBEdp = await mysql.createConnection(constringDBEDP);
let listCabang = [];

// query database
// try {
//   const [rows, fields] = await conDBEdp.execute("SELECT * FROM `DCMAST` ");
//   listCabang = rows;
// } catch (error) {
//   console.warn(
//     moment(new Date()).format("YYYY-MM-DD hh:mm:ss") + " - ADA " + error
//   );
// }
// conDBEdp.destroy();

var fileQuery = "";
const args = process.argv.slice(2);
if (args.length <= 0) {
  try {
    const [rows, fields] = await conDBEdp.execute("SELECT * FROM `DCMAST`");
    listCabang = rows;
  } catch (error) {
    console.warn(
      moment(new Date()).format("YYYY-MM-DD hh:mm:ss") + " - ADA " + error
    );
  }
} else {
  if (args[0] == "ALL") {
    try {
      const [rows, fields] = await conDBEdp.execute("SELECT * FROM `DCMAST`");
      listCabang = rows;
      conDBEdp.destroy();
    } catch (error) {
      console.warn(
        moment(new Date()).format("YYYY-MM-DD hh:mm:ss") + " - ADA " + error
      );
    }
    fileQuery = "./query/isiQuery.txt";
    

  } else {
    let cabangs = args[0].split(",");
    cabangs.forEach((cab) => {
      listCabang.push({ kode_cab: cab, nama_cab: "" });
    });
  }
  if (fs.existsSync("./query/" + args[1])) {
    fileQuery = "./query/" +args[1];
  }

  
}
var fileLog = "./logerrrorwrc.txt";

if (listCabang && fs.existsSync(fileQuery)) {
  //USING PARAREL ASYNC
  let total = listCabang.length;
  var reportProses = [];
  var logError = [];
  await Promise.all(
    listCabang.map(async (cab) => {
      let kode_cab = cab.kode_cab;
      const conWRC = await mysql.createConnection(await getConWRC(kode_cab));
      const query = fs.readFileSync(fileQuery).toString();
      // const query =
      //   "UPDATE REKSSN_220401 a,FILEJENIS b SET  a.JENIS=b.`JENIS`,a.SINGKATAN=b.`SINGKATAN`,a.JENIS2=b.`JENIS2`,a.UMUR=b.`UMUR` where a.PRDCD=b.PRDCD; ";
      const listQuery = query.split(";");

      let totQuery = listQuery.length;
      reportProses.push({
        kode_cab: cab.kode_cab,
        totalError: 0,
        totalQuery: totQuery,
        remainQuery: totQuery,
      });
      for (const que of listQuery) {
        if (que.length > 10) {
          try {
            const hasils = await conWRC.query({
              sql: que,
              infileStreamFactory: (path) => fs.createReadStream(path),
            });
            if (que.length < 100) {
              console.log(kode_cab + " Query Executed :  " + que);
            } else {
              console.log(
                kode_cab + " Query Executed : " + que.substring(0, 100)
              );
            }
            totQuery = totQuery - 1;
            let objIndex = reportProses.findIndex(
              (obj) => obj.kode_cab == kode_cab
            );
            reportProses[objIndex].remainQuery -= 1;
            console.table(reportProses);
            //console.log(`Remain Query : ${totQuery}`);
            //console.log(hasils);
            // for (let i = 0; i < hasils.length; i++) {
            //   console.log(True);
            // }
          } catch (err) {
            //console.log(err.sqlMessage);
            console.log("ADA ERROR " + kode_cab + " - " + err);
            console.log(err);
            logError.push({
              kode_cab: kode_cab,
              Query: que,
              Error: err.sqlMessage,
            });
            let objIndex = reportProses.findIndex(
              (obj) => obj.kode_cab == kode_cab
            );
            reportProses[objIndex].totalError += 1;
            reportProses[objIndex].remainQuery -= 1;
            console.table(reportProses);
          }
        } else {
          let objIndex = reportProses.findIndex(
            (obj) => obj.kode_cab == kode_cab
          );
          reportProses[objIndex].totalQuery -= 1;
          reportProses[objIndex].remainQuery -= 1;
        }
      }
      total--;
      console.log(kode_cab + " FINISH ALL QUERY ");
      console.log(`Sisa cabang belum selesai : ${total} `);
      console.table(reportProses);
      const outputStream = fs.createWriteStream(fileLog, {
        encoding: "utf8",
      });
      const finishedWriting = new Promise((resolve, reject) =>
        outputStream.on("finish", resolve).on("error", reject)
      );

      stringify(logError, {
        header: true,
        bom: true,
        delimiter: "|",
      }).pipe(outputStream);

      await finishedWriting;
      conWRC.destroy();
    })
  );

  console.log("FINISH ALL CABANG");
}
process.exit();
