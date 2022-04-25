import ftp from "basic-ftp";

const client = new ftp.Client();
//client.ftp.verbose = true;
try {
  await client.access({
    host: "192.168.81.7",
    user: "rtt",
    password: "rtt",
  });
  console.log(await client.list());
  await client.ensureDir("/05");
  await client.uploadFromDir("/05", "/05");
} catch (err) {
  console.log(err);
}
client.close();
