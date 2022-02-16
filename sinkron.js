import { constringDBEDP, getConPOSRT } from "./config.js";
import mysql from "mysql2/promise";
import { writeFile, unlink } from "fs/promises";
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
const constringPosrt = await getConPOSRT("G113");

// start import toko_lr
if (listCabang) {
  console.info(
    moment(new Date()).format("YYYY-MM-DD hh:mm:ss") + " - START IMPORT TOKO_LR"
  );
  let kodecabang = [];
  listCabang.forEach((cabang) => {
    kodecabang.push("'" + cabang.kode_cab + "'");
  });
  let querysyntax = `SELECT * FROM toko_lr WHERE kodetoko IN(SELECT kodetoko FROM toko WHERE kodegudang IN(${kodecabang.join(
    ","
  )} ))`;

  const conPosrt = await mysql.createConnection(constringPosrt);
  let queryinsert = "";
  try {
    const [rows, fields] = await conPosrt.execute(querysyntax);
    rows.forEach((baris) => {
      queryinsert += `replace into toko_lr set Kodetoko='${baris.Kodetoko}',HariLibur='${baris.HariLibur}';`;
    });
  } catch (error) {
    console.warn(
      moment(new Date()).format("YYYY-MM-DD hh:mm:ss") + " - ADA " + error
    );
  } finally {
    await conDBEdp.query(queryinsert);
    console.info(
      moment(new Date()).format("YYYY-MM-DD hh:mm:ss") +
        " - Finishing import toko_lr"
    );
  }
}

//start import toko_ts
if (listCabang) {
  let kodecabang = [];
  console.info(
    moment(new Date()).format("YYYY-MM-DD hh:mm:ss") + " - START IMPORT TOKO_TS"
  );
  listCabang.forEach((cabang) => {
    kodecabang.push("'" + cabang.kode_cab + "'");
  });
  let querysyntax = `SELECT * FROM toko_ts WHERE kode_toko IN(SELECT kodetoko FROM toko WHERE kodegudang IN(${kodecabang.join(
    ","
  )} ))`;

  const conPosrt = await mysql.createConnection(constringPosrt);
  let queryinsert = "";
  try {
    const [rows, fields] = await conPosrt.execute(querysyntax);

    rows.forEach((baris) => {
      queryinsert += `INSERT IGNORE INTO toko_ts SET KODE_TOKO='${baris.KODE_TOKO}',TGL_TUTUP='${baris.TGL_TUTUP}', TGL_KETERANGAN='${baris.TGL_KETERANGAN}';`;
    });
  } catch (error) {
    console.warn(
      moment(new Date()).format("YYYY-MM-DD hh:mm:ss") + " - ADA " + error
    );
  } finally {
    await conDBEdp.query(queryinsert);
    console.log(
      moment(new Date()).format("YYYY-MM-DD hh:mm:ss") +
        " - Finishing import toko_ts"
    );
  }
}

//start import toko
if (listCabang) {
  let kodecabang = [];
  console.info(
    moment(new Date()).format("YYYY-MM-DD hh:mm:ss") + " - START IMPORT TOKO"
  );
  listCabang.forEach((cabang) => {
    kodecabang.push("'" + cabang.kode_cab + "'");
  });
  let querysyntax = `SELECT * FROM toko WHERE kodegudang IN(${kodecabang.join(
    ","
  )} ) `;

  const conPosrt = await mysql.createConnection(constringPosrt);
  let queryinsert = "";
  try {
    const [rows, fields] = await conPosrt.execute(querysyntax);
    const jsonData = JSON.parse(JSON.stringify(rows));
    var namaFile = `.//toko.csv`;
    if (fs.existsSync(namaFile)) {
      //file exists
      await unlink(namaFile);
    }

    const json2csvParser = new Parser();
    const csv = json2csvParser.parse(jsonData);

    await writeFile(namaFile, csv, "utf8");

    queryinsert += "truncate toko;";
    queryinsert += `LOAD DATA LOCAL INFILE '${namaFile}'  INTO TABLE TOKO FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\r\n' IGNORE 1 LINES;`;
    queryinsert += `insert into tb_toko(kodegudang,kodetoko,toko_aktif) select toko.kodegudang,toko.kodetoko,'Y' from toko left join tb_toko using(kodetoko) where tb_toko.kodetoko is null and toko.tglbuka<=curdate();
    UPDATE tb_toko a , toko b SET a.NamaToko=REPLACE(b.namatoko,'/005',''),a.AlamatToko=b.AlamatToko,a.KotaToko=b.KotaToko,a.UPDID=b.UPDID,a.UPDTIME=b.UPDTIME,a.TglBuka=b.TglBuka,a.TypeToko=b.TypeToko,a.TypeHarga=b.TypeHarga,a.TypeRak=b.TypeRak,a.KodeCID=b.KodeCID,a.TypeToko24=b.TypeToko24,a.TypeTokoConv=b.TypeTokoConv,a.KodePosToko=b.KodePosToko,a.TokoApka=b.TokoApka,a.DetailToko24=b.DetailToko24,a.TOK_KECAMATAN=b.TOK_KECAMATAN,a.TOK_KELURAHAN=b.TOK_KELURAHAN,a.dual_screen=b.dual_screen,a.luas_gudang=b.luas_gudang,a.isIkiosk=b.isIkiosk,a.jenistokoeco=b.jenistokoeco,a.long_decimal=b.long_decimal,a.lat_decimal=b.lat_decimal, a.kodegudang=b.kodegudang, a.amgr=SUBSTRING_INDEX(b.amgr_name,'-',-1),a.aspv=SUBSTRING_INDEX(b.aspv_name,'-',-1)
    WHERE a.kodetoko=b.kodetoko;
    update tb_toko set namatoko=REPLACE(namatoko,'/008','');
    update tb_toko set namatoko=REPLACE(namatoko,'/005','');
    update tb_toko set namatoko=REPLACE(namatoko,'/001','');
    update tb_toko set namatoko=REPLACE(namatoko,'/000','');
    update tb_toko set namatoko=REPLACE(namatoko,'/','');
    UPDATE tb_toko set toko_aktif='Y' where tglbuka=curdate();
    DELETE FROM tb_toko WHERE toko_aktif IS NULL;`;
    await conDBEdp.query({
      sql: queryinsert,
      infileStreamFactory: (path) => fs.createReadStream(path),
    });
  } catch (error) {
    console.warn(
      moment(new Date()).format("YYYY-MM-DD hh:mm:ss") + " - ADA ERROR" + error
    );
  } finally {
    console.log(
      moment(new Date()).format("YYYY-MM-DD hh:mm:ss") +
        " - Finishing import table toko"
    );
    //process.exit(1);
  }
}

process.exit();
