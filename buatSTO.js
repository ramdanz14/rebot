import { constringDBEDP, getConWRC } from "./config.js";
import qs from "qs";
import axios from "axios";
import mysql from "mysql2/promise";
import fs from "fs";
import moment from "moment";
import { stringify } from "csv-stringify";
import ftp from "basic-ftp";
import path from "path";
import nodemailer from "nodemailer";
console.time("start");

let conDBEdp = await mysql.createConnection(constringDBEDP);
let listCabang = [];

var d = moment().add(-1, "days");

const args = process.argv.slice(2);
if (args.length <= 1) {
  try {
    const [rows, fields] = await conDBEdp.execute(
      "SELECT * FROM `DCMAST` WHERE kode_cab in('G117')"
    );
    listCabang = rows;
  } catch (error) {
    console.warn(
      moment(new Date()).format("YYYY-MM-DD hh:mm:ss") + " - ADA " + error
    );
  }
} else {
  if (args[0] == "ALL") {
    try {
      const [rows, fields] = await conDBEdp.execute(
        "SELECT * FROM `DCMAST` WHERE kode_cab in('G117')"
      );
      listCabang = rows;
    } catch (error) {
      console.warn(
        moment(new Date()).format("YYYY-MM-DD hh:mm:ss") + " - ADA " + error
      );
    }
  } else {
    listCabang.push({ kode_cab: args[0], nama_cab: "" });
  }

  var d = moment(args[1], "YYYY-MM-DD");
}

async function kirimEmail(mailOptions) {
  return new Promise((resolve, reject) => {
    var transporter = nodemailer.createTransport({
      host: "192.168.133.201",
      port: 587,
      secure: false, // upgrade later with STARTTLS
      auth: {
        user: "edp_reg_spv_1",
        pass: "edpregbgr",
      },
      tls: {
        // do not fail on invalid certs
        rejectUnauthorized: false,
      },
    });
    transporter.sendMail(mailOptions, function (error, info) {
      if (error) {
        reject(error);
      } else {
        resolve(" Email sent: " + info.response);
      }
    });
  });
}

var TotalProses = [];

await Promise.all(
  listCabang.map(async (cab) => {
    let cabang = cab.kode_cab;

    let dir1 = `./HASILSTO/`;

    if (!fs.existsSync(dir1)) {
      fs.mkdirSync(dir1);
    }
    var namaFile = `${dir1}/${cabang}-STO${moment(d).format("YYMMDD")}.CSV`;

    let listUpload = [];
    const conWRC = await mysql.createConnection(await getConWRC(cabang));
    if (!fs.existsSync(namaFile)) {
      const query = "SELECT * FROM sto_" + moment(d).format("YYMMDD");
      const [rows, fields] = await conWRC.query(query);

      //console.log(query);

      if (rows.length > 0) {
        const outputStream = fs.createWriteStream(namaFile, {
          encoding: "utf8",
        });
        const finishedWriting = new Promise((resolve, reject) =>
          outputStream.on("finish", resolve).on("error", reject)
        );

        stringify(rows, {
          header: true,
          bom: true,
          delimiter: "|",
        }).pipe(outputStream);

        await finishedWriting;
        console.log("Finish CREATE " + namaFile);
        listUpload.push(namaFile);
      } else {
        console.log(toko.toko + "HASIL KOSONG");
      }
    }

    if (fs.existsSync(dir1) && listUpload.length > 0) {
      //upload jika ada
      const conDBEdp = await mysql.createConnection(constringDBEDP);
      var cekIPftp = await conDBEdp.query(
        `select nilai from config_cabang where kode_cab='${cabang}' and rkey='ftpiptampung'`
      );

      const client = new ftp.Client();
      //client.ftp.verbose = true;
      try {
        await client.access({
          host: cekIPftp[0][0].nilai,
          user: "sto",
          password: "sto",
        });

        //await client.uploadFromDir(dir3, `/${moment(d).format("DD")}`);
        for (const fileupload of listUpload) {
          console.log("Uploading " + path.basename(fileupload));

          await client.uploadFrom(fileupload, `/${path.basename(fileupload)}`);
          console.log(
            `Finish Upload ${cabang} from ${fileupload} to   ftp://${
              cekIPftp[0][0].nilai
            }/${moment(d).format("DD")}`
          );
        }
      } catch (err) {
        console.log(err);
      } finally {
        client.close();
      }
    }
    conWRC.destroy();

    console.log("FINISH - " + cabang);
  })
);
conDBEdp.destroy();
console.log("FINISHALL");

process.exit();
