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
    const ownerAddress = await contract.connect(wallet).getOwnerAddress();
    if (!ownerAddress) {
      return true;
    }

    const settingsOwnerAddressQuery =
      "SELECT * FROM `settings` WHERE `name`='owner_address' AND `group`='general'";
    const settingsOwnerAddressQueryRes = await sql(settingsOwnerAddressQuery);
    console.log(settingsOwnerAddressQueryRes);
    if (!settingsOwnerAddressQueryRes) {
      return true;
    }
    if (settingsOwnerAddressQueryRes.length) {
      if (ownerAddress != settingsOwnerAddressQueryRes[0].payload) {
        const updateOwnerAddressQuery =
          'UPDATE `settings` SET `payload`="' +
          ownerAddress +
          '" WHERE `name`="owner_address"';
        const updateOwnerAddressQueryRes = await sql(updateOwnerAddressQuery);
        console.log(updateOwnerAddressQueryRes);
      }
    } else {
      const insertOwnerAddressQuery =
        'INSERT INTO `settings` (`group`,`name`, `locked`,`payload`, `created_at`,`updated_at`) VALUES ("general","owner_address",0,"' +
        ownerAddress +
        '",CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)';
      const insertOwnerAddressQueryRes = await sql(insertOwnerAddressQuery);
      console.log(insertOwnerAddressQueryRes);
    }

    const listingFees = await contract.connect(wallet).getListingPrice();
    if (!listingFees) {
      return true;
    }

    const settingsCommisionQuery =
      "SELECT * FROM `settings` WHERE `name`='commision' AND `group`='general'";
    const settingsCommisionQueryRes = await sql(settingsCommisionQuery);
    console.log(settingsCommisionQueryRes);
    if (!settingsCommisionQueryRes) {
      return true;
    }
    if (settingsCommisionQueryRes.length) {
      if (listingFees != settingsCommisionQueryRes[0].payload) {
        const updateCommisionQuery =
          'UPDATE `settings` SET `payload`="' +
          listingFees +
          '" WHERE `name`="commision"';
        const updateCommisionQueryRes = await sql(updateCommisionQuery);
        console.log(updateCommisionQueryRes);
      }
    } else {
      const insertCommsionQuery =
        'INSERT INTO `settings` (`group`,`name`, `locked`,`payload`, `created_at`,`updated_at`) VALUES ("general","commision",0,"' +
        listingFees +
        '",CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)';
      const insertCommsionQueryRes = await sql(insertCommsionQuery);
      console.log(insertCommsionQueryRes);
    }

    const royaltyFeesRange = await contract.connect(wallet).getRoyaltyFeesRange();
    if (!royaltyFeesRange) {
      return true;
    }

    const settingsRoyaltyRangeQuery =
      "SELECT * FROM `settings` WHERE `name`='royalty_max' AND `group`='general'";
    const settingsRoyaltyRangeQueryRes = await sql(settingsRoyaltyRangeQuery);
    console.log(settingsRoyaltyRangeQueryRes);
    if (!settingsRoyaltyRangeQueryRes) {
      return true;
    }
    if (settingsRoyaltyRangeQueryRes.length) {
      if (royaltyFeesRange != settingsRoyaltyRangeQueryRes[0].payload) {
        const updateRoyaltyRangeQuery =
          'UPDATE `settings` SET `payload`="' +
          royaltyFeesRange +
          '" WHERE `name`="royalty_max"';
        const updateRoyaltyRangeQueryRes = await sql(updateRoyaltyRangeQuery);
        console.log(updateRoyaltyRangeQueryRes);
      }
    } else {
      const insertRoyaltyRangeQuery =
        'INSERT INTO `settings` (`group`,`name`, `locked`,`payload`, `created_at`,`updated_at`) VALUES ("general","royalty_max",0,"' +
        royaltyFeesRange +
        '",CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)';
      const insertRoyaltyRangeQueryRes = await sql(insertRoyaltyRangeQuery);
      console.log(insertRoyaltyRangeQueryRes);
    }

  } catch (err) {
    console.log(err);
  }
  process.exit(1);
})();
