require("dotenv").config();

const CONFIG = {
  app: process.env.APP || "dev",
  port: process.env.PORT || "3001",
  marketplaceAddress: "0x2D079650cd7C3fd1c3EA6B86c6e54f8107ce8579",
  firstBlockNumber: 29092467,
  RPC_Node_provider:process.env.RPC_NODE_PROVIDER || "",
  owner_private_key: process.env.OWNER_PRIVATE_KEY || "",
};

module.exports = CONFIG;
