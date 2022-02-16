import { constringDBEDP, getConWRC } from "./config.js";
import mysql from "mysql2/promise";
import { writeFile, unlink, appendFile } from "fs/promises";
import fs from "fs";
import { Parser } from "json2csv";
import moment from "moment";

let conDBEdp = await mysql.createConnection(constringDBEDP);
let listCabang = [];

// query database
try {
  const [rows, fields] = await conDBEdp.execute("SELECT * FROM `DCMAST` ");
  listCabang = rows;
} catch (error) {
  console.warn(
    moment(new Date()).format("YYYY-MM-DD hh:mm:ss") + " - ADA " + error
  );
}

if (listCabang) {
  let hasilFinal = [];

  //USING SERIES BLOCKING
  // for (let cab of listCabang) {
  //   let kode_cab = cab.kode_cab;
  //   const conWRC = await mysql.createConnection(await getConWRC(kode_cab));
  //   const query = `SELECT DISTINCT bulan_tahun, 'dc_target_seasonal_t' AS tb FROM dc_target_seasonal_t UNION ALL
  //       SELECT DISTINCT bulan_tahun,'dc_target_seasonal_t_dc' AS tb FROM dc_target_seasonal_t_dc UNION ALL
  //       SELECT  DISTINCT YEAR(tgl_alokasi),'dc_alokasi_seasonal_v' FROM dc_alokasi_seasonal_v UNION ALL
  //       SELECT  DISTINCT YEAR(tgl_alokasi),'dc_alokasi_seasonal_v_dc' FROM dc_alokasi_seasonal_v_dc;`;

  //   let [rows] = await conWRC.execute(query);
  //   let header_cab = { cabang: kode_cab };
  //   let mergedArray = [];
  //   rows.forEach((row) => {
  //     let combo = { ...header_cab, ...row };
  //     mergedArray.push(combo);
  //   });

  //   hasilFinal.push(...mergedArray);
  //   console.log(kode_cab + "Finish");
  // }

  //USING PARAREL ASYNC
  await Promise.all(
    listCabang.map(async (cab) => {
      let kode_cab = cab.kode_cab;
      const conWRC = await mysql.createConnection(await getConWRC(kode_cab));
//      const query = `select * from (select kode_toko,sum(saldo_akh) as saldo_akh from kodetoko_2201 group by kode_toko) t left join 
//    (SELECT kode_toko,SUM(begbal) begbal FROM st_220203 GROUP BY kode_toko) s using(kode_toko) having begbal!=saldo_akh;`;
	const query = `SHOW VARIABLES LIKE 'innodb_buffer_pool_size' ;`;

      let [rows] = await conWRC.execute(query);
      let header_cab = { cabang: kode_cab };
      let mergedArray = [];
      rows.forEach((row) => {
        let combo = { ...header_cab, ...row };
        mergedArray.push(combo);
      });

      hasilFinal.push(...mergedArray);
      console.log(kode_cab + "Finish");
    })
  ).catch((err) => {
    console.error(err.message); // Oops!
  });

  var namaFile = `./tmp/HASILWRC-${moment(new Date()).format(
    "YYYYMMDDhhmmss"
  )}.csv`;

  const json2csvParser = new Parser({ header: true });

  const csv = json2csvParser.parse(hasilFinal);

  //console.table(csv);
  if (csv.length > 1) {
    await writeFile(namaFile, csv + "\r\n", { encoding: "utf-8" });
    console.log("Finish All");
  } else {
    console.log("HASIL KOSONG");
  }
}
