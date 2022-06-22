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

    const conWRC = await mysql.createConnection(await getConWRC(cabang));

    let listToko = [];

    try {
      console.log(cabang + " - START SELECT LIST TOKO");
      const [rows, fields] = await conWRC.execute(
        "SELECT DISTINCT shop as toko FROM rtt_" +
          moment(d).format("YYMMDD") +
          "  "
      );
      listToko = rows;
    } catch (error) {
      console.warn(
        moment(new Date()).format("YYYY-MM-DD hh:mm:ss") +
          ` - ${cabang} ADA ` +
          error
      );
      return;
    }

    //update kolom ppn_rate dulu

    const cekRateKolom = await conWRC.query(
      `SELECT COUNT(*) cek FROM information_schema.columns WHERE table_name='rtt_${moment(
        d
      ).format("YYMMDD")}' AND column_name='ppn_rate';`
    );
    if (cekRateKolom[0][0].cek == 0) {
      ///alter struktur di
      await conWRC.query(
        `ALTER TABLE \`rtt_${moment(d).format(
          "YYMMDD"
        )}\` ADD \`PPN_RATE\` VARCHAR(100) AFTER \`TIPE_GDG\`;`
      );
    }
    for (let i = 0; i < 5; i++) {
      const cekPPNull = await conWRC.query(
        `SELECT count(*) as cek FROM rtt_${moment(d).format(
          "YYMMDD"
        )} WHERE ppn_rate IS NULL or ppn_rate='';`
      );

      if (cekPPNull[0][0].cek > 0) {
        console.log(
          cabang +
            " - WARNING MASIH  ADA PPN_RATE NULL DI TABLE RTT_" +
            moment(d).format("YYMMDD") +
            " UPDATE DARI TABLE PR_" +
            moment(d)
              .add(i * -1, "days")
              .format("YYMMDD")
        );
        let query = `UPDATE rtt_${moment(d).format("YYMMDD")} a, pr_${moment(d)
          .add(i * -1, "days")
          .format(
            "YYMMDD"
          )} b SET a.ppn_rate=b.rate_ppn WHERE a.shop=b.toko AND a.prdcd=b.prdcd AND (a.ppn_rate IS NULL or ppn_rate='');`;
        await conWRC.query(query);
      } else {
        console.log(
          cabang +
            " - SUDAH TIDAK ADA PPN_RATE NULL DI TABLE RTT_" +
            moment(d).format("YYMMDD")
        );
        break;
      }
    }

    if (listToko) {
      let listUpload = [];
      await Promise.all(
        listToko.map(async (toko) => {
          const query =
            "SELECT DOCNO,DOCNO2,`DIV`,TOKO,TOKO_1,GUDANG,PRDCD,QTY,ROUND(PRICE,3) AS PRICE,ROUND(GROSS,2) GROSS,ROUND(PPN,6) PPN,DATE_FORMAT(TANGGAL,'%d-%m-%Y')TANGGAL,DATE_FORMAT(TANGGAL2,'%d-%m-%Y') AS TANGGAL2,SHOP,ISTYPE,ROUND(PRICE_IDM,6) PRICE_IDM,PPNBM_IDM,ROUND(PPNRP_IDM,6)PPNRP_IDM,SCTYPE,BKP,SUB_BKP,CABANG,TIPE_GDG,PPN_RATE  FROM rtt_" +
            moment(d).format("YYMMDD") +
            "  WHERE shop in('" +
            toko.toko +
            "')";
          const [rows, fields] = await conWRC.execute(query);

          //console.log(query);
          let dir1 = `./HASILRTT/`;
          let dir2 = `./HASILRTT/${cabang}/`;
          let dir3 = `./HASILRTT/${cabang}/${moment(d).format("DD")}/`;

          if (!fs.existsSync(dir1)) {
            fs.mkdirSync(dir1);
          }
          if (!fs.existsSync(dir2)) {
            fs.mkdirSync(dir2);
          }
          if (!fs.existsSync(dir3)) {
            fs.mkdirSync(dir3);
          }
          var namaFile = `${dir3}/RTT${cabang}${toko.toko}${moment(d).format(
            "YYMMDD"
          )}.CSV`;

          if (!fs.existsSync(namaFile)) {
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
        })
      );
      let dir3 = `./HASILRTT/${cabang}/${moment(d).format("DD")}/`;

      if (fs.existsSync(dir3) && listUpload.length > 0) {
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
            user: "rtt",
            password: "rtt",
          });
          await client.ensureDir(`/${moment(d).format("DD")}`);
          //await client.uploadFromDir(dir3, `/${moment(d).format("DD")}`);
          for (const fileupload of listUpload) {
            console.log("Uploading" + path.basename(fileupload));

            await client.uploadFrom(
              fileupload,
              `/${moment(d).format("DD")}/${path.basename(fileupload)}`
            );
          }
          console.log(
            `Finish Upload ${cabang} from ${dir3} to   ftp://${
              cekIPftp[0][0].nilai
            }/${moment(d).format("DD")}`
          );
        } catch (err) {
          console.log(err);
        } finally {
          client.close();
        }

        TotalProses.push({
          kode_cab: cab.kode_cab,
          nama_cab: cab.nama_cab,
          totalRTT: listToko.length,
        });

        const [rows, fields] = await conDBEdp.query(
          `SELECT * FROM notif_email WHERE rkey='RTT' AND kode_cab='${cabang}'`
        );

        let mailOptions = {
          from: {
            name: "RAMDAN P |EDP REG1",
            address: "edp_reg_spv_1@regbgr.indomaret.co.id",
          },
          to: rows[0].list_email + ",cs_edp@regbgr.indomaret.co.id",

          subject: `${cabang} INFORMASI DATA RTT PERIODE  ${moment(d).format(
            "DD MMM YYYY"
          )}`,

          html: `<p>Yth Tim DC/EDP/IC Cabang</p>
              <p>di tempat,</p>
              </br>
              </br>                
              <p>Berikut informasi terkait TAT Beda Pemilik pada tanggal ${moment(
                d
              ).format("DD MMM YYYY")}  sebanyak *${
            listToko.length
          } toko*, data bisa diakses pada alamat FTP berikut :</br>
                  ftp://${cekIPftp[0][0].nilai}/${moment(d).format("DD")}</br>
                  user : rtt</br>
                  password : rtt</br>
      
                  </br>Dan untuk pengiriman data RTT yang sudah diproses oleh DC menjadi NPP,dikirimkan ke alamat :</br>
                  ftp://192.168.133.3</br>
                  user : rtt_reg1</br>
                  pass : rtt_reg1</br>
                  folder : ${cabang}</br>
      </p>
              <p>Atas perhatian dan kerjasamanya kami sampaikan terima kasih.</p></br>
              </br>
              <p>Hormat Kami,</p>
              </br>                
              <i><b>RAMDAN PERMADI</b></i></br>
              <i>EDP REGIONAL 1</i></br>`,
        };

        let hasilEmail = await kirimEmail(mailOptions);
        console.log(hasilEmail);

        let pesan = `_*Bapak EDPM Cabang ${cabang} - ${cab.nama_cab}*_,

        Berikut informasi terkait TAT Beda Pemilik pada tanggal ${moment(
          d
        ).format("DD MMM YYYY")}  sebanyak *${
          listToko.length
        } toko*, data bisa diakses pada alamat FTP berikut :
            ftp://${cekIPftp[0][0].nilai}/${moment(d).format("DD")}
            user : rtt
            password : rtt

        Dan untuk pengiriman data RTT yang sudah diproses oleh DC menjadi NPP,dikirimkan ke alamat :
            ftp://192.168.133.3
            user : rtt_reg1
            pass : rtt_reg1
            folder : ${cabang}

        Mohon informasi ini bisa diteruskan ke IC dan DC Cabang.
          Terima kasih🙏`;
        // console.log("Start Sending notif WA");
        // await axios
        //   .post("http://192.168.133.60:9000/send-message", {
        //     number: "08562225929",
        //     message: pesan,
        //   })
        //   .then(function (response) {
        //     console.log("RESPON KIRIM WA : " + response.statusText);
        //   });
      }
      conWRC.destroy();

      console.log("FINISH - " + cabang);
    }
  })
);
conDBEdp.destroy();
console.log("FINISHALL");

if (TotalProses.length > 0) {
  let pesan = `Informasi file RTT yang dikirim ke FTP untuk DC periode tanggal  ${moment(
    d
  ).format("DD MMM YYYY")} sebagai berikut : 
`;
  for (const p of TotalProses) {
    pesan += `${p.kode_cab} - ${p.nama_cab} = ${p.totalRTT} Files\r\n`;
  }
  pesan += "\r\nTerima kasih.";
  // console.log("Start Sending notif WA");
  // await axios
  //   .post("http://192.168.133.60:9000/send-message", {
  //     number: "08562225929",
  //     message: pesan,
  //   })
  //   .then(function (response) {
  //     console.log("RESPON KIRIM WA : " + response.statusText);
  //   });
}

process.exit();
