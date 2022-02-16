import mysql from "mysql2/promise";
var constringDBEDP = {
  host: "192.168.133.3",
  user: "edp",
  password: "cUm4l!h4t@datA",
  database: "db_edp",
  multipleStatements: true,
};

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

export { constringDBEDP, getConWRC, getConPOSRT, getConToko };
