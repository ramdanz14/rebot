import { constringDBEDP } from "./config.js";
import mysql from "mysql2/promise";
import { writeFile, unlink } from "fs/promises";
import fs from "fs";
import { Parser } from "json2csv";
import nodemailer from "nodemailer";
import moment from "moment";
import path from "path";

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
  await Promise.all(
    listCabang.map(async (cab) => {
      let kode_cab = cab.kode_cab;
      let nama_cab = cab.nama_cab;
      const d = new Date();
      d.setDate(d.getDate() - 1);
      let namatable = "harian" + moment(d).format("YYMMDD");
      let queryHarian = `SELECT MID(file_harian,3,6) AS tgl,kdtk,namatoko, ASPV FROM ${namatable} LEFT JOIN tb_TOKO ON kodetoko=kdtk WHERE note LIKE '%gagal%closing%' AND tb_TOKO.kodegudang='${kode_cab}' ORDER BY tgl,amgr,aspv;`;
      try {
        const [rows, fields] = await conDBEdp.execute(queryHarian);
        const jsonData = JSON.parse(JSON.stringify(rows));
        var namaFile = `./tmp/CLOSING-${kode_cab}-${moment(d).format(
          "DDMMYYYY"
        )}.csv`;
        if (fs.existsSync(namaFile)) {
          //file exists
          await unlink(namaFile);
        }
        const json2csvParser = new Parser();
        const csv = json2csvParser.parse(jsonData);
        if (csv.length > 1) {
          await writeFile(namaFile, csv, "utf8");
        }

        if (fs.existsSync(namaFile)) {
          console.info(
            moment(new Date()).format("YYYY-MM-DD hh:mm:ss") +
              " - SUCCESS WRITING FILE " +
              namaFile
          );
          var domain = "";
          switch (kode_cab) {
            case "G113":
              domain = "bgr";
              break;

            case "G117":
              domain = "bgr2";
              break;

            case "G107":
              domain = "prg";
              break;

            case "G026":
              domain = "tgr";
              break;

            case "G033":
              domain = "tgr2";
              break;

            case "G157":
              domain = "lbk";
              break;
          }

          var mailOptions = {
            from: {
              name: "RAMDAN P |EDP REG1",
              address: "edp_reg_spv_1@regbgr.indomaret.co.id",
            },
            to: "edp_reg_spv_1@regbgr.indomaret.co.id",
            to: `edp_reg_mgr@regbgr.indomaret.co.id, edp_mgr@${domain}.indomaret.co.id, cs_spv@regbgr.indomaret.co.id`,
            subject: `${kode_cab} TOKO CLOSING HARIAN TIDAK SELESAI PERIODE ${moment(
              d
            ).format("DD MMM YYYY")}`,
            attachments: [
              {
                filename: path.basename(namaFile),
                path: namaFile,
              },
            ],
            html: `<p>Yth Bapak EDP Manager Cabang</p>
                <p>di tempat,</p>
                </br>
                </br>                
                <p>Berikut kami kirimkan List Toko-toko yang tidak selesai dalam menjalankan proses closing harian sampai data hariannya terbentuk.</p>
                <p>Atas perhatian dan kerjasamanya kami sampaikan terima kasih.</p></br>
                </br>
                <p>Hormat Kami,</p>
                </br>                
                <i><b>RAMDAN PERMADI</b></i></br>
                <i>EDP REGIONAL 1</i></br>`,
          };

          transporter.sendMail(mailOptions, function (error, info) {
            if (error) {
              console.log(error);
            } else {
              console.log(kode_cab + " Email sent: " + info.response);
            }
          });
        }
      } catch (error) {
        console.warn(
          moment(new Date()).format("YYYY-MM-DD hh:mm:ss") + " - ADA " + error
        );
      }
    })
  );
}

setTimeout(() => {
  process.exit();
}, 50000);

//process.exit();
