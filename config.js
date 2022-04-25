import mysql from "mysql2/promise";
var constringDBEDP = {
  host: "192.168.133.3",
  user: "edp",
  password: "cUm4l!h4t@datA",
  database: "db_edp",
  multipleStatements: true,
};



async function getConBulanan(cab) {
  let conDBEdp = await mysql.createConnection(constringDBEDP);

  const [rows, fields] = await conDBEdp.execute(
    `select * from config_cabang where rkey in('constringbulanan') and kode_cab='${cab}'`
  );
  let constring = rows[0].nilai.split(";");
  let config = {};
  for (const obj of constring) {
    let cek = obj.trim().split("=");
    switch (cek[0]) {
      case "server":
        config["host"] = cek[1];
        break;

      case "uid":
        config["user"] = cek[1];
        break;

      case "pwd":
        config["password"] = cek[1];
        break;

      case "database":
        config["database"] = cek[1];
        break;
    }
  }

  var constrinBln = {
    host: config.host,
    user: config.user,
    password: config.password,
    database: config.database,
    multipleStatements: true,
    dateStrings: ["DATE", "DATETIME"],
  };
  conDBEdp.destroy();
  return Promise.resolve(constrinBln);
}

async function getConWRC(cab) {
  let conDBEdp = await mysql.createConnection(constringDBEDP);

  const [rows, fields] = await conDBEdp.execute(
    `select * from config_cabang where rkey in('ftpipwrc','dbwrc') and kode_cab='${cab}'`
  );
  let ipwrc = "";
  let dbwrc = "poscabang";
  rows.forEach((row) => {
    if (row.rkey == "ftpipwrc") {
      ipwrc = row.nilai;
    } else if (row.rkey == "dbwrc") {
      dbwrc = row.nilai;
    }
  });

  var constrinWRC = {
    host: ipwrc,
    user: "root",
    password: "$d3@pr15mata",
    database: dbwrc,
    multipleStatements: true,
    dateStrings: ["DATE", "DATETIME"],
  };
  conDBEdp.destroy();
  return Promise.resolve(constrinWRC);
}

async function getConPOSRT(cab) {
  let conDBEdp = await mysql.createConnection(constringDBEDP);

  const [rows, fields] = await conDBEdp.execute(
    `select * from config_cabang where rkey in('dbposrt') and kode_cab='${cab}'`
  );
  let iphost = "";
  rows.forEach((row) => {
    if (row.rkey == "dbposrt") iphost = row.nilai;
  });

  var constrinWRC = {
    host: iphost,
    user: "cabang",
    password: "cabang123",
    database: "posservice_base",
    multipleStatements: true,
    dateStrings: ["DATE", "DATETIME"],
  };
  conDBEdp.destroy();
  return Promise.resolve(constrinWRC);
}

async function getConToko(iphost) {
  var constringToko = {
    host: iphost,
    user: "edp",
    password: "cUm4l!h4t@datA",
    database: "pos",
    multipleStatements: true,
    dateStrings: ["DATE", "DATETIME"],
    connectTimeout: 30000,
  };
  return Promise.resolve(constringToko);
}

export { constringDBEDP, getConWRC, getConPOSRT, getConToko, getConBulanan };
