import { constringDBEDP, getConWRC } from "./config.js";
import mysql from "mysql2/promise";
import { writeFile, unlink, appendFile } from "fs/promises";
import fs from "fs";
import { Parser } from "json2csv";
import moment from "moment";
import { setInterval } from "timers/promises";

let conDBEdp = await mysql.createConnection(constringDBEDP);
let listCabang = [];

// query database
try {
  const [rows, fields] = await conDBEdp.execute("SELECT * FROM `DCMAST`;");
  listCabang = rows;
} catch (error) {
  console.warn(
    moment(new Date()).format("YYYY-MM-DD hh:mm:ss") + " - ADA " + error
  );
} finally {
  conDBEdp.destroy();
}

const jeda = 3 * 1000;
var format = "HH:mm:ss";
for await (const startTime of setInterval(jeda, Date.now())) {
  var time = moment(),
    beforeTime = moment("10:00:00", format),
    afterTime = moment("13:00:00", format);
  if (time.isBetween(beforeTime, afterTime)) {
    //TODO: panggil service disini
    console.log(`Starting application ` + time.format("DD-MM-YYYY HH:mm:ss"));
    if (listCabang) {
      //USING PARAREL ASYNC
      let total = listCabang.length;
      await Promise.all(
        listCabang.map(async (cab) => {
          var conWRC = null;
          let kode_cab = cab.kode_cab;
          try {
            conWRC = await mysql.createConnection(await getConWRC(kode_cab));
            //  const query = fs.readFileSync("./isiquery.txt").toString();
            const query = `SET @s:='';
            select @s:=concat('KILL ',id,';') from information_schema.processlist where host like '%192.168.133.7%' and state='sleep';    
            PREPARE stmt FROM @s;
            EXECUTE stmt;`;
            const hasils = await conWRC.query({
              sql: query,
              infileStreamFactory: (path) => fs.createReadStream(path),
            });
            if (query.length < 100) {
              console.log(kode_cab + " Query Executed :  " + query);
            } else {
              console.log(
                kode_cab + " Query Executed : " + query.substring(0, 50)
              );
            }

            //console.log(hasils);
            // for (let i = 0; i < hasils.length; i++) {
            //   console.log(True);
            // }
          } catch (err) {
            //console.log(err.sqlMessage);
            console.log(kode_cab + JSON.stringify(err));
          } finally {
            if (conWRC != null) {
              await conWRC.destroy();
            }
            total--;
          }

          console.log(`Sisa cabang belum selesai : ${total} `);
        })
      );

      console.log("FINISH ALL CABANG");
    }
  } else {
    console.log(`bukan waktunya` + time);
  }
}
