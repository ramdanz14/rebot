import { constringDBEDP, getConWRC, getConPOSRT } from "./config.js";
import mysql from "mysql2/promise";
import { writeFile, unlink } from "fs/promises";
import fs from "fs";
import { Parser } from "json2csv";
import moment from "moment";

let conDBEdp = await mysql.createConnection(constringDBEDP);
let listCabang = [];

const args = process.argv.slice(2);

// query database
try {
  const [rows, fields] = await conDBEdp.execute(
    "SELECT * FROM `DCMAST` where kode_cab in ('G117') "
  );
  listCabang = rows;
} catch (error) {
  console.warn(
    moment(new Date()).format("YYYY-MM-DD hh:mm:ss") + " - ADA " + error
  );
}

if (listCabang) {
  //const d = new Date(2022, 0, 24);
  const d = moment(args[0]);
  const jenis = "NP";
  await Promise.all(
    listCabang.map(async (cab) => {
      try {
        let kode_cab = cab.kode_cab;
        console.info(
          `Now Checking intransit for ${kode_cab} date ${moment(d).format(
            "DD MMM yyyy"
          )} `
        );
        //   const concab = await getConWRC(kode_cab);
        //   console.log(concab);
        const conWRC = await mysql.createConnection(await getConWRC(kode_cab));
        const query = `select kodedc,Toko,docno,tgl_docno,NamaFile from poscabang.brd_npb_sum where tgl_docno='${moment(
          d
        ).format("YYYY-MM-DD")}' and  namafile like '${jenis}%' ;`;
        //console.info(query);
        let [rows, fields] = await conWRC.execute(query);
        const jsonBRD = JSON.parse(JSON.stringify(rows));

        const conposrt = await mysql.createConnection(
          await getConPOSRT(kode_cab)
        );

        const queryposrt = `SELECT * FROM npb_task WHERE namafile LIKE '${jenis}%${moment(
          d
        ).format("YYMMDD")}%';`;
        // console.info(query);
        [rows, fields] = await conposrt.execute(queryposrt);

        const jsonMonitorNP = JSON.parse(JSON.stringify(rows));

        var mergedArray = [];

        //compare dari hasil brd dan monitor np

        for (var brd of Object.values(jsonBRD)) {
          var findItem = Object.values(jsonMonitorNP).find(function (np) {
            return np.NamaFile === brd.NamaFile;
          });

          if (findItem) {
            var { Toko, NamaFile, ...monitor } = findItem;
            //  is  called "desctructring".  membuat object array baru dengan membuang key/fields yang tidak digunakan
            var cmp = { ...brd, ...monitor };

            mergedArray.push(cmp);
          } else {
            const kosong = {
              TglServerTerima: "TIDAK ADA",
              TglTokoAmbil: "TIDAK ADA",
              TglTokoProses: "TIDAK ADA",
              IdTask: "",
              StatusAmbil: "",
              StatusProses: "",
              AddId: "",
              AddTime: "",
            };
            cmp = { ...brd, ...kosong };
            mergedArray.push(cmp);
          }
        }

        var namaFile = `./tmp/CEKINT-${kode_cab}-${moment(d).format(
          "DDMMYYYY"
        )}.csv`;
        if (fs.existsSync(namaFile)) {
          //file exists
          await unlink(namaFile);
        }

        const json2csvParser = new Parser();
        const csv = json2csvParser.parse(mergedArray);
        if (csv.length > 1) {
          await writeFile(namaFile, csv, { encoding: "utf-8" });
        }
        if (fs.existsSync(namaFile)) {
          console.info(`SUCCRESS WRITING ${namaFile}`);
        }
      } catch (error) {
        console.error(error);
      }
    })
  ).catch((err) => {
    console.error(err.message); // Oops!
  });
}

console.info("PROSES SELESAI");
process.exit();
