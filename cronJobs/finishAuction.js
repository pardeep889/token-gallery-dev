require("dotenv").config({ path: "/var/www/nft-microservices/.env" });
const mysql = require("mysql2");
const ethers = require("ethers");
const Interface = require("ethers/lib/utils");
const CONFIG = require("../config/config");
const NFTMarketplace = require("../artifacts/contracts/NFTMarketplace.sol/NFTMarketplace.json");
const { owner_private_key } = require("../config/config");

const connection = mysql.createConnection({
  host: process.env.DB_HOST,
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  database: process.env.DB_NAME,
  port: "/var/run/mysqld/mysqld.sock",
  multipleStatements: true,
});
connection.connect();

function sql(query) {
  return new Promise((resolve, reject) => {
    connection.query(query, (err, results) => {
      if (err) return reject(err);
      return resolve(results);
    });
  });
}

(async function () {
  try {
    const finishedAuctionsListQuery =
      "SELECT * FROM `auctions` WHERE `transaction_type`= 0 AND `is_enabled`=1 AND `end_time`<=CURRENT_TIMESTAMP;";
    const finishedAuctionList = await sql(finishedAuctionsListQuery);
    console.log(finishedAuctionList)
    if (!finishedAuctionList) {
      return true;
    }
    if (finishedAuctionList.length) {
      const abiIFace = new Interface.Interface(NFTMarketplace.abi);
      const rpcProvider = new ethers.providers.JsonRpcProvider(
        CONFIG.RPC_Node_provider
      );
      const wallet = new ethers.Wallet(owner_private_key, rpcProvider);
      const contract = new ethers.Contract(
        CONFIG.marketplaceAddress,
        abiIFace,
        rpcProvider
      );
      for (
        let tempIndex = 0;
        tempIndex < finishedAuctionList.length;
        tempIndex++
      ) {
        const transaction = await contract.connect(wallet).finish(
          finishedAuctionList[tempIndex].auction_id,
          {
            gasLimit: 600000,
          }
        );
        await transaction.wait();
        const updateAuctionsStatusQuery =
          "UPDATE `auctions` SET `is_enabled`=0 WHERE `auction_id`="+finishedAuctionList[tempIndex].auction_id+";";
        const updateAuctionsStatusQueryRes = await sql(
          updateAuctionsStatusQuery
        );
        console.log(updateAuctionsStatusQueryRes)
      }
    }
  } catch (err) {
    console.log(err);
  }
  process.exit(1);
})();
