require('dotenv').config({ path: '/var/www/nft-microservices/.env' });
const mysql = require('mysql2');
const ethers = require("ethers");
const axios = require("axios");
const sqlstring = require('sqlstring');
const Interface = require("ethers/lib/utils");
const CONFIG = require("../config/config");
const NFTMarketplace = require("../artifacts/contracts/NFTMarketplace.sol/NFTMarketplace.json");
const keccak256 = require("js-sha3").keccak256;

const connection = mysql.createConnection({
    host: process.env.DB_HOST,
    user: process.env.DB_USER,
    password: process.env.DB_PASSWORD,
    database: process.env.DB_NAME,
    port: '/var/run/mysqld/mysqld.sock',
    multipleStatements: true
});
// const lastBlockNumber = 26568608;
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
        const syncLogQuery = 'SELECT sl.id, sl.created_at, sl.block_number FROM sync_log as sl WHERE sl.isError=0 ORDER BY sl.block_number DESC LIMIT 1;';
        const syncLogQueryRes = await sql(syncLogQuery);
        // const syncLogQueryRes = [{block_number:26771800}];
        console.log("Prev Sync Log", syncLogQueryRes);
        if (!syncLogQueryRes) {
            console.log("// Exiting loop - Sync Log Data not found");
            return true;
        }
        var firstBlock = 0;
        if (syncLogQueryRes.length) {
            firstBlock = syncLogQueryRes[0].block_number + 1;
        } else {
            firstBlock = CONFIG.firstBlockNumber;
        }
        const abiIFace = new Interface.Interface(NFTMarketplace.abi);
        const rpcProvider = new ethers.providers.JsonRpcProvider(CONFIG.RPC_Node_provider)
        const contract = new ethers.Contract(CONFIG.marketplaceAddress, abiIFace, rpcProvider)
        const currentBlockNumber = await rpcProvider.getBlockNumber();
        const contractAddress = contract.address;
        if (contractAddress) {
            var newNFTsArr = [];
            var NFTSoldArr = [];
            var newAuctionsArr = [];
            var newAuctionBidsArr = [];
            var finishedAuctionsArr = [];
            var cancelledAuctionsArr = [];
            var MarketItemListedForSaleArr = [];
            var CancleMarketItemSalesArr = [];
            for (let startBlock = firstBlock, EndBlock = ((startBlock + 1999) > currentBlockNumber ? currentBlockNumber : (startBlock + 1999));
                startBlock <= currentBlockNumber && EndBlock <= currentBlockNumber;
                startBlock = startBlock + 2000) {
                console.log(startBlock, EndBlock, EndBlock - startBlock);
                const MarketItemCreated = await contract.queryFilter("MarketItemCreated", startBlock, EndBlock);
                for (let i = 0; i < MarketItemCreated.length; i++) {
                    var newNFT = {};
                    const MarketItemCreatedTopic = await abiIFace.decodeEventLog("MarketItemCreated", MarketItemCreated[i].data, MarketItemCreated[i].topics);
                    newNFT.blockNumber = MarketItemCreated[i].blockNumber;
                    newNFT.transactionHash = MarketItemCreated[i].transactionHash;
                    newNFT.tokenId = MarketItemCreatedTopic.tokenId.toNumber();
                    newNFT.tokenURI = await contract.tokenURI(newNFT.tokenId);
                    var tempData = await sql("SELECT * FROM `temp_nfts` WHERE `transaction_hash`='" + MarketItemCreated[i].transactionHash + "';");
                    if (tempData && tempData[0] && tempData[0].data) {
                        newNFT.tempData = JSON.parse(tempData[0].data);
                        if (newNFT.tempData && newNFT.tempData.metadataURL && newNFT.tempData.metadataURL != "") {
                            const meta = await axios.get(newNFT.tempData.metadataURL);
                            newNFT.name = meta.data.name;
                            newNFT.description = meta.data.description;
                            newNFT.image = meta.data.image;
                            newNFT.creator = MarketItemCreatedTopic.creator;
                            newNFT.seller = MarketItemCreatedTopic.seller;
                            newNFT.owner = MarketItemCreatedTopic.owner;
                            newNFT.price = ethers.utils.formatUnits(MarketItemCreatedTopic.price.toString(), 'ether');
                            newNFT.listingFees = 0;
                            newNFT.royaltyFees = ethers.utils.formatUnits(MarketItemCreatedTopic.royaltyFees.toString(), 'wei');
                            newNFT.sold = MarketItemCreatedTopic.sold;
                            if (newNFT.tempData.fileType && newNFT.tempData.fileType != "") {
                                newNFT.fileType = newNFT.tempData.fileType;
                            }else{
                                newNFT.fileType = "";
                            }
                            newNFT.nftHash = keccak256("" + newNFT.tokenId);
                            if (newNFT.tempData.sellingChoice && newNFT.tempData.sellingChoice != "") {
                                newNFT.listingChoice = JSON.stringify(newNFT.tempData.sellingChoice);
                                newNFTsArr.push(newNFT);
                            }
                        }
                    }
                }
                const MarketItemSold = await contract.queryFilter("MarketItemSold", startBlock, EndBlock);
                for (let i = 0; i < MarketItemSold.length; i++) {
                    let newNFTSold = {};
                    const MarketItemSoldTopic = await abiIFace.decodeEventLog("MarketItemSold", MarketItemSold[i].data, MarketItemSold[i].topics);
                    newNFTSold.blockNumber = MarketItemSold[i].blockNumber;
                    newNFTSold.transactionHash = MarketItemSold[i].transactionHash;
                    newNFTSold.tokenId = MarketItemSoldTopic.tokenId.toNumber();
                    newNFTSold.creator = MarketItemSoldTopic.creator;
                    newNFTSold.seller = MarketItemSoldTopic.seller;
                    newNFTSold.owner = MarketItemSoldTopic.owner;
                    newNFTSold.price = ethers.utils.formatUnits(MarketItemSoldTopic.price.toString(), 'ether');
                    newNFTSold.listingFees = ethers.utils.formatUnits(MarketItemSoldTopic.listingFees.toString(), 'ether');
                    newNFTSold.royaltyFees = ethers.utils.formatUnits(MarketItemSoldTopic.royaltyFees.toString(), 'wei');
                    newNFTSold.marketPlaceFees = ethers.utils.formatUnits(MarketItemSoldTopic.marketPlaceFees.toString(), 'wei');
                    newNFTSold.isFixedPriceSale = MarketItemSoldTopic.isFixedPriceSale;
                    newNFTSold.isAuctionSale = MarketItemSoldTopic.isAuctionSale;
                    NFTSoldArr.push(newNFTSold);
                }
                const AuctionCreated = await contract.queryFilter("AuctionCreated", startBlock, EndBlock);
                for (let i = 0; i < AuctionCreated.length; i++) {
                    let newAuction = {};
                    const AuctionCreatedTopic = await abiIFace.decodeEventLog("AuctionCreated", AuctionCreated[i].data, AuctionCreated[i].topics);
                    newAuction.block_number = AuctionCreated[i].blockNumber;
                    newAuction.transaction_hash = AuctionCreated[i].transactionHash;
                    newAuction.auction_id = AuctionCreatedTopic.auctionId.toNumber();
                    newAuction.nft_id = AuctionCreatedTopic.tokenId.toNumber();
                    newAuction.seller = AuctionCreatedTopic.seller;
                    newAuction.starting_price = ethers.utils.formatUnits(AuctionCreatedTopic.startingPrice.toString(), 'ether');
                    newAuction.listing_fees = ethers.utils.formatUnits(AuctionCreatedTopic.listingFees.toString(), 'ether');
                    newAuction.start_time = new Date(AuctionCreatedTopic.startTime.toNumber()*1000).toISOString().slice(0, 19).replace('T', ' ');
                    newAuction.end_time = new Date(AuctionCreatedTopic.endTime.toNumber()*1000).toISOString().slice(0, 19).replace('T', ' ');
                    newAuctionsArr.push(newAuction);
                }
                const AuctionBidden = await contract.queryFilter("AuctionBidden", startBlock, EndBlock);
                for (let i = 0; i < AuctionBidden.length; i++) {
                    let newAuctionBid = {};
                    const AuctionBiddenTopic = await abiIFace.decodeEventLog("AuctionBidden", AuctionBidden[i].data, AuctionBidden[i].topics);
                    newAuctionBid.block_number = AuctionBidden[i].blockNumber;
                    newAuctionBid.transaction_hash = AuctionBidden[i].transactionHash;
                    newAuctionBid.auction_id = AuctionBiddenTopic.auctionId.toNumber();
                    newAuctionBid.bidder = AuctionBiddenTopic.bidder;
                    newAuctionBid.price = ethers.utils.formatUnits(AuctionBiddenTopic.price.toString(), 'ether');
                    newAuctionBidsArr.push(newAuctionBid);
                }
                const AuctionCancelled = await contract.queryFilter("AuctionCancelled",startBlock,EndBlock);
                for(let i=0;i<AuctionCancelled.length;i++){
                    let cancelledAuction = {};
                    const AuctionCancelledTopic = await abiIFace.decodeEventLog("AuctionCancelled", AuctionCancelled[i].data, AuctionCancelled[i].topics);
                    cancelledAuction.block_number = AuctionCancelled[i].blockNumber;
                    cancelledAuction.transaction_hash = AuctionCancelled[i].transactionHash;
                    cancelledAuction.auction_id = AuctionCancelledTopic.auctionId.toNumber();
                    cancelledAuction.nft_id = AuctionCancelledTopic.tokenId.toNumber();
                    cancelledAuction.owner = AuctionCancelledTopic.owner;
                    cancelledAuction.listing_fees = ethers.utils.formatUnits(AuctionCancelledTopic.returnedListingPrice.toString(), 'ether');
                    cancelledAuctionsArr.push(cancelledAuction);                
                }
                const AuctionFinished = await contract.queryFilter("AuctionFinished", startBlock, EndBlock);
                for (let i = 0; i < AuctionFinished.length; i++) {
                    let finishedAuction = {};
                    const AuctionFinishedTopic = await abiIFace.decodeEventLog("AuctionFinished", AuctionFinished[i].data, AuctionFinished[i].topics);
                    finishedAuction.block_number = AuctionFinished[i].blockNumber;
                    finishedAuction.transaction_hash = AuctionFinished[i].transactionHash;
                    finishedAuction.auction_id = AuctionFinishedTopic.auctionId.toNumber();
                    finishedAuction.nft_id = AuctionFinishedTopic.tokenId.toNumber();
                    finishedAuction.seller = AuctionFinishedTopic.seller;
                    finishedAuction.owner = AuctionFinishedTopic.owner;
                    finishedAuction.sell_price = ethers.utils.formatUnits(AuctionFinishedTopic.sellPrice.toString(), 'ether');
                    finishedAuction.listing_fees = ethers.utils.formatUnits(AuctionFinishedTopic.listingFees.toString(), 'ether');
                    finishedAuction.royalty_fees = ethers.utils.formatUnits(AuctionFinishedTopic.royaltyFees.toString(), 'ether');
                    finishedAuction.marketplace_fees = ethers.utils.formatUnits(AuctionFinishedTopic.marketPlaceFees.toString(), 'ether');
                    finishedAuction.isFixedPriceSale = AuctionFinishedTopic.isFixedPriceSale;
                    finishedAuction.isAuctionSale = AuctionFinishedTopic.isAuctionSale;
                    finishedAuctionsArr.push(finishedAuction);
                }
                const MarketItemListedForSale = await contract.queryFilter("MarketItemListedForSale", startBlock, EndBlock);
                for (let i = 0; i < MarketItemListedForSale.length; i++) {
                    let marketItemListedForSale = {};
                    const MarketItemListedForSaleTopic = await abiIFace.decodeEventLog("MarketItemListedForSale", MarketItemListedForSale[i].data, MarketItemListedForSale[i].topics);
                    marketItemListedForSale.blockNumber = MarketItemListedForSale[i].blockNumber;
                    marketItemListedForSale.transactionHash = MarketItemListedForSale[i].transactionHash;
                    marketItemListedForSale.tokenId = MarketItemListedForSaleTopic.tokenId.toNumber();
                    marketItemListedForSale.seller = MarketItemListedForSaleTopic.seller;
                    marketItemListedForSale.price = ethers.utils.formatUnits(MarketItemListedForSaleTopic.price.toString(), 'ether');
                    marketItemListedForSale.listingFees = ethers.utils.formatUnits(MarketItemListedForSaleTopic.listingFees.toString(), 'ether');
                    marketItemListedForSale.isFixedPriceSale = MarketItemListedForSaleTopic.isFixedPriceSale;
                    MarketItemListedForSaleArr.push(marketItemListedForSale);
                }
                const CancleMarketItemSale = await contract.queryFilter("CancleMarketItemSale", startBlock, EndBlock);
                if (CancleMarketItemSale.length != 0) {
                    for (let i = 0; i < CancleMarketItemSale.length; i++) {
                        let cancleMarketItemSale = {};
                        const CancleMarketItemSaleTopic = await abiIFace.decodeEventLog("CancleMarketItemSale", CancleMarketItemSale[i].data, CancleMarketItemSale[i].topics);
                        cancleMarketItemSale.blockNumber = CancleMarketItemSale[i].blockNumber;
                        cancleMarketItemSale.transactionHash = CancleMarketItemSale[i].transactionHash;
                        cancleMarketItemSale.tokenId = CancleMarketItemSaleTopic.tokenId.toNumber();
                        cancleMarketItemSale.owner = CancleMarketItemSaleTopic.owner;
                        cancleMarketItemSale.listingFees = ethers.utils.formatUnits(CancleMarketItemSaleTopic.listingFeesReturned.toString(), 'ether');
                        CancleMarketItemSalesArr.push(cancleMarketItemSale);
                    }
                }
                newNFTsArr.sort((a, b) => {
                    if (b.blockNumber > a.blockNumber)
                        return -1;
                    else if (b.blockNumber < a.blockNumber)
                        return 1;
                    else
                        return 0;
                })
                for (var tempBlock = startBlock; tempBlock <= EndBlock; tempBlock++) {
                    let tempNewNFTsArr = newNFTsArr.filter((item  ) => {
                        return item.blockNumber == tempBlock;
                    })
                    let tempNFTSoldArr = NFTSoldArr.filter((item  ) => {
                        return item.blockNumber == tempBlock;
                    })
                    let tempNewAuctionsArr = newAuctionsArr.filter((item  ) => {
                        return item.block_number == tempBlock;
                    })
                    let tempNewAuctionBidsArr = newAuctionBidsArr.filter((item  ) => {
                        return item.block_number == tempBlock;
                    })
                    let tempCancelledAuctionsArr = cancelledAuctionsArr.filter((item) => {
                        return item.block_number == tempBlock;
                    })
                    let tempFinishedAuctionsArr = finishedAuctionsArr.filter((item  ) => {
                        return item.block_number == tempBlock;
                    })
                    let tempMarketItemListedForSaleArr = MarketItemListedForSaleArr.filter((item  ) => {
                        return item.blockNumber == tempBlock;
                    })
                    let tempCancleMarketItemSalesArr = CancleMarketItemSalesArr.filter((item  ) => {
                        return item.blockNumber == tempBlock;
                    })
                    if (tempNewNFTsArr.length)
                        console.log("NFTs created", tempNewNFTsArr);
                    if (tempNFTSoldArr.length)
                        console.log("NFTs sell history", tempNFTSoldArr);
                    if (tempNewAuctionsArr.length)
                        console.log("Auctions Created", tempNewAuctionsArr);
                    if (tempNewAuctionBidsArr.length)
                        console.log("Auctions Bids history", tempNewAuctionBidsArr);
                    if (tempCancelledAuctionsArr.length)
                        console.log("Cancelled Auctions history", tempCancelledAuctionsArr);
                    if (tempFinishedAuctionsArr.length)
                        console.log("Finished Auctions history", tempFinishedAuctionsArr);
                    if (tempMarketItemListedForSaleArr.length)
                        console.log("NFT Listed history", tempMarketItemListedForSaleArr);
                    if (tempCancleMarketItemSalesArr.length)
                        console.log("NFT Cancle Listing history", tempCancleMarketItemSalesArr);
                    const nftsInsertQuery = "INSERT INTO `nfts` (`block_number`,`transaction_hash`,`token_id`,`nft_hash`,`name`, `description`, `image`, `file_type`, `creator`, `seller`, `owner`, `price`,`listing_fees`, `royalty_fees`, `base_fees`,`listing_choice`, `sold`, `is_collection_item`,`collection_id`,`views_count`,`is_listed_for_sale`,`is_fixed_price_sale`,`created_at`,`updated_at`) VALUES ";
                    const metadataInsertQuery = "INSERT INTO `metadata` (`token_id`,`metadata_url`,`created_at`,`updated_at`) VALUES ";
                    const tagsInsertQuery = "INSERT INTO `nft_tags` (`token_id`,`tag_id`,`created_at`,`updated_at`) VALUES ";
                    const propertiesInsertQuery = "INSERT INTO `properties` (`trait_type`,`value`,`nft_id`,`created_at`,`updated_at`) VALUES ";
                    var dataInsertionQueryRes = [];
                    let queryValue = "";
                    var metadataInsertQueryValue = "";
                    var tagsInsertQueryValue = "";
                    var propertiesInsertQueryValue = "";
                    for (let tx = 0; tx < tempNewNFTsArr.length; tx = tx + 10) {
                        queryValue = nftsInsertQuery;
                        metadataInsertQueryValue = metadataInsertQuery;
                        for (let paginateIndex = tx; paginateIndex < tx + 10 && paginateIndex < tempNewNFTsArr.length; paginateIndex++) {
                            queryValue = queryValue + "(" +
                                tempNewNFTsArr[paginateIndex].blockNumber + ",'" +
                                tempNewNFTsArr[paginateIndex].transactionHash + "'," +
                                tempNewNFTsArr[paginateIndex].tokenId + ",'" +
                                tempNewNFTsArr[paginateIndex].nftHash + "','" +
                                tempNewNFTsArr[paginateIndex].name + "','"+
                                tempNewNFTsArr[paginateIndex].description + "','"+
                                tempNewNFTsArr[paginateIndex].image + "','" +
                                tempNewNFTsArr[paginateIndex].fileType + "','" +
                                tempNewNFTsArr[paginateIndex].creator + "','" +
                                tempNewNFTsArr[paginateIndex].seller + "','" +
                                tempNewNFTsArr[paginateIndex].owner + "'," +
                                tempNewNFTsArr[paginateIndex].price + "," +
                                tempNewNFTsArr[paginateIndex].listingFees + "," +
                                tempNewNFTsArr[paginateIndex].royaltyFees + ",0,'" +
                                tempNewNFTsArr[paginateIndex].listingChoice + "'," +
                                ((tempNewNFTsArr[paginateIndex].sold) == true ? 1 : 0) + "," +
                                ((tempNewNFTsArr[paginateIndex].tempData.selectedCollections && tempNewNFTsArr[paginateIndex].tempData.selectedCollections.length != 0) == true ? 1 : 0) + "," +
                                ((tempNewNFTsArr[paginateIndex].tempData.selectedCollections && tempNewNFTsArr[paginateIndex].tempData.selectedCollections.length != 0) == true ? tempNewNFTsArr[paginateIndex].tempData.selectedCollections[0] : "NULL") +
                                ",0,0,0,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)";
                            metadataInsertQueryValue = metadataInsertQueryValue + "(" +
                                tempNewNFTsArr[paginateIndex].tokenId + ",'" +
                                tempNewNFTsArr[paginateIndex].tempData.metadataURL + "',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)";
                            if (tempNewNFTsArr[paginateIndex].tempData.selectedTags && tempNewNFTsArr[paginateIndex].tempData.selectedTags.length != 0) {
                                tagsInsertQueryValue = tagsInsertQueryValue + tagsInsertQuery;
                                for (let tempTagsIndex = 0; tempTagsIndex < tempNewNFTsArr[paginateIndex].tempData.selectedTags.length; tempTagsIndex++) {
                                    tagsInsertQueryValue = tagsInsertQueryValue + "(" +
                                        tempNewNFTsArr[paginateIndex].tokenId + "," +
                                        tempNewNFTsArr[paginateIndex].tempData.selectedTags[tempTagsIndex] +
                                        ",CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)" +
                                        ((tempTagsIndex < tempNewNFTsArr[paginateIndex].tempData.selectedTags.length - 1) ? "," : ";");
                                }
                            }
                            if (tempNewNFTsArr[paginateIndex].tempData.properties && tempNewNFTsArr[paginateIndex].tempData.properties.length != 0) {
                                propertiesInsertQueryValue = propertiesInsertQueryValue + propertiesInsertQuery;
                                for (let tempPropertiesIndex = 0; tempPropertiesIndex < tempNewNFTsArr[paginateIndex].tempData.properties.length; tempPropertiesIndex++) {
                                    propertiesInsertQueryValue = propertiesInsertQueryValue + "('" +
                                        tempNewNFTsArr[paginateIndex].tempData.properties[tempPropertiesIndex].trait_type + "','" +
                                        tempNewNFTsArr[paginateIndex].tempData.properties[tempPropertiesIndex].value + "'," +
                                        tempNewNFTsArr[paginateIndex].tokenId + "," +
                                        "CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)" +
                                        ((tempPropertiesIndex < tempNewNFTsArr[paginateIndex].tempData.properties.length - 1) ? "," : ";");
                                }
                            }
                            if (paginateIndex != (tx + 10 - 1) && paginateIndex != (tempNewNFTsArr.length - 1)) {
                                queryValue = queryValue + ","
                                metadataInsertQueryValue = metadataInsertQueryValue + ","
                            } else {
                                queryValue = queryValue + ";"
                                metadataInsertQueryValue = metadataInsertQueryValue + ";"
                            }
                        }
                        dataInsertionQueryRes.push(await sql(queryValue));
                        dataInsertionQueryRes.push(await sql(metadataInsertQueryValue));
                        if(tagsInsertQueryValue!=""){
                            dataInsertionQueryRes.push(await sql(tagsInsertQueryValue));
                        }
                        if(propertiesInsertQueryValue!=""){
                            dataInsertionQueryRes.push(await sql(propertiesInsertQueryValue));
                        }
                    }
                    const marketItemListedInsertQuery = "INSERT INTO `nft_sell_history` (`block_number`,`transaction_type`,`transaction_hash`,`token_id`,`seller`, `owner`, `price`,`listing_fees`, `royalty_fees`, `marketplace_fees`, `created_at`,`updated_at`) VALUES ";
                    queryValue = "";
                    var updateNFTsQuery = "";
                    for (let tx = 0; tx < tempMarketItemListedForSaleArr.length; tx = tx + 10) {
                        queryValue = marketItemListedInsertQuery;
                        updateNFTsQuery = "";
                        for (let paginateIndex = tx; paginateIndex < tx + 10 && paginateIndex < tempMarketItemListedForSaleArr.length; paginateIndex++) {
                            queryValue = queryValue + "(" +
                                tempMarketItemListedForSaleArr[paginateIndex].blockNumber +
                                ",0,'" +
                                tempMarketItemListedForSaleArr[paginateIndex].transactionHash + "'," +
                                tempMarketItemListedForSaleArr[paginateIndex].tokenId + ",'" +
                                tempMarketItemListedForSaleArr[paginateIndex].seller +
                                "','0x0000000000000000000000000000000000000000'," +
                                tempMarketItemListedForSaleArr[paginateIndex].price + "," +
                                tempMarketItemListedForSaleArr[paginateIndex].listingFees +
                                ",0,0,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)";
                                let nftsUpdateQuery = "UPDATE `nfts` SET `seller`='" + tempMarketItemListedForSaleArr[paginateIndex].seller + "', `price`=" + tempMarketItemListedForSaleArr[paginateIndex].price + ",`listing_fees`=" + tempMarketItemListedForSaleArr[paginateIndex].listingFees + ",`sold`=0,`is_listed_for_sale`=1,`is_fixed_price_sale`=1 WHERE `token_id`=" + tempMarketItemListedForSaleArr[paginateIndex].tokenId + ";";
                            updateNFTsQuery = updateNFTsQuery + nftsUpdateQuery;
                            if (paginateIndex != (tx + 10 - 1) && paginateIndex != (tempMarketItemListedForSaleArr.length - 1)) {
                                queryValue = queryValue + ","
                            } else {
                                queryValue = queryValue + ";"
                            }
                        }
                        dataInsertionQueryRes.push(await sql(queryValue));
                        dataInsertionQueryRes.push(await sql(updateNFTsQuery));
                    }
                    const cancleMarketItemListingInsertQuery = "INSERT INTO `nft_sell_history` (`block_number`,`transaction_type`,`transaction_hash`,`token_id`,`seller`, `owner`, `price`,`listing_fees`, `royalty_fees`, `marketplace_fees`,`created_at`,`updated_at`) VALUES ";
                    queryValue = "";
                    var updateQueryValue = "";
                    for (let tx = 0; tx < tempCancleMarketItemSalesArr.length; tx = tx + 10) {
                        queryValue = cancleMarketItemListingInsertQuery;
                        updateQueryValue = "";
                        for (let paginateIndex = tx; paginateIndex < tx + 10 && paginateIndex < tempCancleMarketItemSalesArr.length; paginateIndex++) {
                            queryValue = queryValue + "(" +
                                tempCancleMarketItemSalesArr[paginateIndex].blockNumber +
                                ",1,'" +
                                tempCancleMarketItemSalesArr[paginateIndex].transactionHash + "'," +
                                tempCancleMarketItemSalesArr[paginateIndex].tokenId +
                                ",'0x0000000000000000000000000000000000000000','" +
                                tempCancleMarketItemSalesArr[paginateIndex].owner +
                                "',0," +
                                tempCancleMarketItemSalesArr[paginateIndex].listingFees +
                                ",0,0,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)";
                                let nftsUpdateQuery = "UPDATE `nfts` SET `seller`='0x0000000000000000000000000000000000000000', `listing_fees`=" + tempCancleMarketItemSalesArr[paginateIndex].listingFees + ",`sold`=1,`is_listed_for_sale`=0,`is_fixed_price_sale` = 0 WHERE `token_id`=" + tempCancleMarketItemSalesArr[paginateIndex].tokenId + ";";
                            updateQueryValue = updateQueryValue + nftsUpdateQuery;
                            if (paginateIndex != (tx + 10 - 1) && paginateIndex != (tempCancleMarketItemSalesArr.length - 1)) {
                                queryValue = queryValue + ","
                            } else {
                                queryValue = queryValue + ";"
                            }
                        }
                        dataInsertionQueryRes.push(await sql(queryValue));
                        dataInsertionQueryRes.push(await sql(updateQueryValue));
                    }
                    const nftstransactionsInsertQuery = "INSERT INTO `nft_sell_history` (`block_number`,`transaction_type`,`transaction_hash`,`token_id`,`seller`, `owner`, `price`,`listing_fees`, `royalty_fees`, `marketplace_fees`, `created_at`,`updated_at`) VALUES ";
                    const nftSaleNotificationsQuery = "INSERT INTO `notifications` (`type`,`notifiable_type`,`notifiable_id`,`data`,`created_at`,`updated_at`) VALUES ";
                    queryValue = "";
                     updateQueryValue = "";
                    var nftSaleNotificationsQueryValue = "";
                    for (let tx = 0; tx < tempNFTSoldArr.length; tx = tx + 10) {
                        queryValue = nftstransactionsInsertQuery;
                        updateQueryValue = "";
                        nftSaleNotificationsQueryValue = nftSaleNotificationsQuery;
                        for (let paginateIndex = tx; paginateIndex < tx + 10 && paginateIndex < tempNFTSoldArr.length; paginateIndex++) {
                            queryValue = queryValue + "(" +
                                tempNFTSoldArr[paginateIndex].blockNumber +
                                ",2,'" +
                                tempNFTSoldArr[paginateIndex].transactionHash + "'," +
                                tempNFTSoldArr[paginateIndex].tokenId + ",'" +
                                tempNFTSoldArr[paginateIndex].seller + "','" +
                                tempNFTSoldArr[paginateIndex].owner + "'," +
                                tempNFTSoldArr[paginateIndex].price + "," +
                                tempNFTSoldArr[paginateIndex].listingFees + "," +
                                tempNFTSoldArr[paginateIndex].royaltyFees + "," +
                                tempNFTSoldArr[paginateIndex].marketPlaceFees +
                                ",CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)";
                            let tempNFT = await sql("SELECT `nft_hash` FROM `nfts` WHERE `token_id`=" + tempNFTSoldArr[paginateIndex].tokenId + ";");
                            if (tempNFT && tempNFT[0] && tempNFT[0].nft_hash) {
                                tempNFT = tempNFT[0];
                            }else{
                                tempNFT = null;
                            }
                            let nftsUpdateQuery = "UPDATE `nfts` SET `seller`='" + tempNFTSoldArr[paginateIndex].seller + "',`owner`='" + tempNFTSoldArr[paginateIndex].owner + "', `price`=" + tempNFTSoldArr[paginateIndex].price + ",`listing_fees`=" + tempNFTSoldArr[paginateIndex].listingFees + ",`sold`=1,`is_listed_for_sale`=0,`is_fixed_price_sale` = 0,`is_collection_item`= 0,`collection_id`=NULL WHERE `token_id`=" + tempNFTSoldArr[paginateIndex].tokenId + ";";
                            updateQueryValue = updateQueryValue + nftsUpdateQuery;
                            let tempNotificationData = JSON.stringify({ "nft_hash" : tempNFT.nft_hash, "account_id" : tempNFTSoldArr[paginateIndex].owner});
                            nftSaleNotificationsQueryValue = nftSaleNotificationsQueryValue + "('1','0','" +
                            tempNFTSoldArr[paginateIndex].seller + "','" +
                            tempNotificationData + 
                            "',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP),";
                            nftSaleNotificationsQueryValue = nftSaleNotificationsQueryValue + "('1','1','" +
                            tempNFTSoldArr[paginateIndex].creator + "','" +
                            tempNotificationData + 
                            "',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)";
                            
                            if (paginateIndex != (tx + 10 - 1) && paginateIndex != (tempNFTSoldArr.length - 1)) {
                                queryValue = queryValue + ","
                                nftSaleNotificationsQueryValue = nftSaleNotificationsQueryValue +","
                            } else {
                                queryValue = queryValue + ";"
                                nftSaleNotificationsQueryValue = nftSaleNotificationsQueryValue +";"
                            }
                        }
                        dataInsertionQueryRes.push(await sql(queryValue));
                        dataInsertionQueryRes.push(await sql(updateQueryValue));
                        dataInsertionQueryRes.push(await sql(nftSaleNotificationsQueryValue));
                    }

                    const auctionsInsertionQuery = "INSERT INTO `auctions` (`block_number`,`transaction_type`, `transaction_hash`,`auction_id`, `seller`,`owner`, `is_nft`,`nft_id`, `is_enabled`, `starting_price`, `start_time`, `end_time`, `listing_fees`, `created_at`, `updated_at`) VALUES ";
                    const auctionStartedNotificationsQuery = "INSERT INTO `notifications` (`type`,`notifiable_type`,`notifiable_id`,`data`,`created_at`,`updated_at`) VALUES ";
                    queryValue = "";
                     updateQueryValue = "";
                    var auctionStartedNotificationsQueryValue = "";
                    for (let tx = 0; tx < tempNewAuctionsArr.length; tx = tx + 10) {
                        queryValue = auctionsInsertionQuery;
                        updateQueryValue = "";
                        auctionStartedNotificationsQueryValue = auctionStartedNotificationsQuery;
                        for (let paginateIndex = tx; paginateIndex < tx + 10 && paginateIndex < tempNewAuctionsArr.length; paginateIndex++) {
                            queryValue = queryValue + "(" +
                                tempNewAuctionsArr[paginateIndex].block_number + ",0,'" +
                                tempNewAuctionsArr[paginateIndex].transaction_hash + "'," +
                                tempNewAuctionsArr[paginateIndex].auction_id + ",'" +
                                tempNewAuctionsArr[paginateIndex].seller +
                                "','0x0000000000000000000000000000000000000000',1," +
                                tempNewAuctionsArr[paginateIndex].nft_id + ",1," +
                                tempNewAuctionsArr[paginateIndex].starting_price + ",'" +
                                tempNewAuctionsArr[paginateIndex].start_time + "','" +
                                tempNewAuctionsArr[paginateIndex].end_time + "'," +
                                tempNewAuctionsArr[paginateIndex].listing_fees +
                                ",CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)";
                            let tempNFT = await sql("SELECT `nft_hash` FROM `nfts` WHERE `token_id`=" + tempNewAuctionsArr[paginateIndex].nft_id + ";");
                            if (tempNFT && tempNFT[0] && tempNFT[0].nft_hash) {
                                tempNFT = tempNFT[0];
                            }else{
                                tempNFT = null;
                            }
                            let nftsUpdateQuery = "UPDATE `nfts` SET `seller`='" + tempNewAuctionsArr[paginateIndex].seller + "', `price`=" + tempNewAuctionsArr[paginateIndex].starting_price + ",`listing_fees`=" + tempNewAuctionsArr[paginateIndex].listing_fees + ",`sold`=0,`is_listed_for_sale`=1,`is_fixed_price_sale`=0 WHERE `token_id`=" + tempNewAuctionsArr[paginateIndex].nft_id + ";";
                            updateQueryValue = updateQueryValue + nftsUpdateQuery;
                            let tempNotificationData = JSON.stringify({ "nft_hash" : tempNFT.nft_hash});
                            auctionStartedNotificationsQueryValue = auctionStartedNotificationsQueryValue + "('3','0','" +
                            tempNewAuctionsArr[paginateIndex].seller + "','" +
                            tempNotificationData + 
                            "',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)";

                            if (paginateIndex != (tx + 10 - 1) && paginateIndex != (tempNewAuctionsArr.length - 1)) {
                                queryValue = queryValue + ",";
                                auctionStartedNotificationsQueryValue = auctionStartedNotificationsQueryValue + ",";
                            } else {
                                queryValue = queryValue + ";"
                                auctionStartedNotificationsQueryValue = auctionStartedNotificationsQueryValue + ";";
                            }
                        }
                        dataInsertionQueryRes.push(await sql(queryValue));
                        dataInsertionQueryRes.push(await sql(updateQueryValue));
                        dataInsertionQueryRes.push(await sql(auctionStartedNotificationsQueryValue));
                    }
                    
                    const auctionsCancelledQuery = "INSERT INTO `auctions` (`block_number`,`transaction_type`, `transaction_hash`,`auction_id`, `seller`,`owner`, `is_nft`,`nft_id`, `is_enabled`, `listing_fees`, `created_at`, `updated_at`) VALUES ";
                    queryValue = "";
                    var updateNFTsQueryValue = "";
                    let updateAuctionsQueryValue = "";
                    for (let tx = 0; tx < tempCancelledAuctionsArr.length; tx = tx + 10) {
                        queryValue = auctionsCancelledQuery;
                        updateQueryValue = "";
                        for (let paginateIndex = tx; paginateIndex < tx + 10 && paginateIndex < tempCancelledAuctionsArr.length; paginateIndex++) {
                            queryValue = queryValue + "(" +
                                tempCancelledAuctionsArr[paginateIndex].block_number + ",1,'" +
                                tempCancelledAuctionsArr[paginateIndex].transaction_hash + "'," +
                                tempCancelledAuctionsArr[paginateIndex].auction_id + 
                                ",'0x0000000000000000000000000000000000000000','" +
                                tempCancelledAuctionsArr[paginateIndex].owner + "',1," +
                                tempCancelledAuctionsArr[paginateIndex].nft_id + ",0," +
                                tempCancelledAuctionsArr[paginateIndex].listing_fees +
                                ",CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)";
                                let nftsUpdateQuery = "UPDATE `nfts` SET `seller`='0x0000000000000000000000000000000000000000', `owner`='" + tempCancelledAuctionsArr[paginateIndex].owner + "',`listing_fees`=" + tempCancelledAuctionsArr[paginateIndex].listing_fees + ",`sold`=1,`is_listed_for_sale`=0,`is_fixed_price_sale`=0 WHERE `token_id`=" + tempCancelledAuctionsArr[paginateIndex].nft_id + ";";
                            updateNFTsQueryValue = updateNFTsQueryValue + nftsUpdateQuery;

                            let auctionsUpdateQuery = "UPDATE `auctions` SET `is_enabled`=0 WHERE `auction_id`="+tempCancelledAuctionsArr[paginateIndex].auction_id+";";
                            updateAuctionsQueryValue = updateAuctionsQueryValue + auctionsUpdateQuery;
                  
                            if (paginateIndex != (tx + 10 - 1) && paginateIndex != (tempCancelledAuctionsArr.length - 1)) {
                                queryValue = queryValue + ",";
                            } else {
                                queryValue = queryValue + ";"
                            }
                        }
                        dataInsertionQueryRes.push(await sql(queryValue));
                        dataInsertionQueryRes.push(await sql(updateNFTsQueryValue));
                        dataInsertionQueryRes.push(await sql(updateAuctionsQueryValue));
                    }
                    
                    const bidsInsertionQuery = "INSERT INTO `bids` (`block_number`, `transaction_hash`,`auction_id`, `bidder`, `price`,`created_at`, `updated_at`) VALUES ";
                    queryValue = "";
                    for (let tx = 0; tx < tempNewAuctionBidsArr.length; tx = tx + 10) {
                        queryValue = bidsInsertionQuery;
                        for (let paginateIndex = tx; paginateIndex < tx + 10 && paginateIndex < tempNewAuctionBidsArr.length; paginateIndex++) {
                            queryValue = queryValue + "(" +
                                tempNewAuctionBidsArr[paginateIndex].block_number + ",'" +
                                tempNewAuctionBidsArr[paginateIndex].transaction_hash + "'," +
                                tempNewAuctionBidsArr[paginateIndex].auction_id + ",'" +
                                tempNewAuctionBidsArr[paginateIndex].bidder + "'," +
                                tempNewAuctionBidsArr[paginateIndex].price +
                                ",CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)";
                            if (paginateIndex != (tx + 10 - 1) && paginateIndex != (tempNewAuctionBidsArr.length - 1)) {
                                queryValue = queryValue + ","
                            } else {
                                queryValue = queryValue + ";"
                            }
                        }
                        dataInsertionQueryRes.push(await sql(queryValue));
                    }
                    const auctionsFinishedQuery = "INSERT INTO `auctions` (`block_number`,`transaction_type`, `transaction_hash`,`auction_id`, `seller`,`owner`, `is_nft`,`nft_id`, `is_enabled`, `listing_fees`, `sell_price`, `royalty_fees`, `marketplace_fees`, `created_at`, `updated_at`) VALUES ";
                    const auctionSaleNotificationsQuery = "INSERT INTO `notifications` (`type`,`notifiable_type`,`notifiable_id`,`data`,`created_at`,`updated_at`) VALUES ";
                    queryValue = "";
                     updateQueryValue = "";
                    var updateWinnerQuery = "";
                    updateAuctionsQueryValue = "";
                    var auctionSaleNotificationsQueryValue = "";
                    for (let tx = 0; tx < tempFinishedAuctionsArr.length; tx = tx + 10) {
                        queryValue = auctionsFinishedQuery;
                        updateQueryValue = "";
                        auctionSaleNotificationsQueryValue = auctionSaleNotificationsQuery;
                        for (let paginateIndex = tx; paginateIndex < tx + 10 && paginateIndex < tempFinishedAuctionsArr.length; paginateIndex++) {
                            queryValue = queryValue + "(" +
                                tempFinishedAuctionsArr[paginateIndex].block_number + ",2,'" +
                                tempFinishedAuctionsArr[paginateIndex].transaction_hash + "'," +
                                tempFinishedAuctionsArr[paginateIndex].auction_id + ",'" +
                                tempFinishedAuctionsArr[paginateIndex].seller + "','" +
                                tempFinishedAuctionsArr[paginateIndex].owner + "',1," +
                                tempFinishedAuctionsArr[paginateIndex].nft_id + ",0," +
                                tempFinishedAuctionsArr[paginateIndex].listing_fees + "," +
                                tempFinishedAuctionsArr[paginateIndex].sell_price + "," +
                                tempFinishedAuctionsArr[paginateIndex].royalty_fees + "," +
                                tempFinishedAuctionsArr[paginateIndex].marketplace_fees +
                                ",CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)";
                            let tempNFT = await sql("SELECT `nft_hash` FROM `nfts` WHERE `token_id`=" + tempFinishedAuctionsArr[paginateIndex].nft_id + ";");
                            if (tempNFT && tempNFT[0] && tempNFT[0].nft_hash) {
                                tempNFT = tempNFT[0];
                            }else{
                                tempNFT = null;
                            }
                            let nftsUpdateQuery = "UPDATE `nfts` SET `seller`='" + tempFinishedAuctionsArr[paginateIndex].seller + "',`owner`='" + tempFinishedAuctionsArr[paginateIndex].owner + "', `price`=" + tempFinishedAuctionsArr[paginateIndex].sell_price + ",`listing_fees`=" + tempFinishedAuctionsArr[paginateIndex].listing_fees + ",`sold`=1,`is_listed_for_sale`=0,`is_fixed_price_sale`= 0,`is_collection_item`= 0,`collection_id`=NULL WHERE `token_id`=" + tempFinishedAuctionsArr[paginateIndex].nft_id + ";";
                            let winnerQuery = "UPDATE `bids` SET `is_winner`=1 WHERE `bidder`='" + tempFinishedAuctionsArr[paginateIndex].owner + "' AND `auction_id`=" + tempFinishedAuctionsArr[paginateIndex].auction_id + ";"
                            updateQueryValue = updateQueryValue + nftsUpdateQuery;
                            updateWinnerQuery = updateWinnerQuery + winnerQuery;

                            let auctionsUpdateQuery = "UPDATE `auctions` SET `is_enabled`=0 WHERE `auction_id`="+tempFinishedAuctionsArr[paginateIndex].auction_id+";";
                            updateAuctionsQueryValue = updateAuctionsQueryValue + auctionsUpdateQuery;

                            let tempNotificationData = JSON.stringify({ "nft_hash" : tempNFT.nft_hash, "account_id" : tempFinishedAuctionsArr[paginateIndex].owner});
                            auctionSaleNotificationsQueryValue = auctionSaleNotificationsQueryValue + "('2','0','" +
                            tempFinishedAuctionsArr[paginateIndex].seller + "','" +
                            tempNotificationData + 
                            "',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP),";
                            auctionSaleNotificationsQueryValue = auctionSaleNotificationsQueryValue + "('2','1','" +
                            tempNFT.creator + "','" +
                            tempNotificationData + 
                            "',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)";
                            
                            if (paginateIndex != (tx + 10 - 1) && paginateIndex != (tempFinishedAuctionsArr.length - 1)) {
                                queryValue = queryValue + ","
                                auctionSaleNotificationsQueryValue = auctionSaleNotificationsQueryValue + ","
                            } else {
                                queryValue = queryValue + ";"
                                auctionSaleNotificationsQueryValue = auctionSaleNotificationsQueryValue + ";"
                            }
                        }
                        dataInsertionQueryRes.push(await sql(queryValue));
                        dataInsertionQueryRes.push(await sql(updateQueryValue));
                        dataInsertionQueryRes.push(await sql(updateWinnerQuery));
                        dataInsertionQueryRes.push(await sql(auctionSaleNotificationsQueryValue))
                        dataInsertionQueryRes.push(await sql(updateAuctionsQueryValue));
                    }
                    var syncLogInsertionQuery = "INSERT INTO `sync_log` (block_number,isError,created_at,updated_at) VALUES";
                    await Promise.all(dataInsertionQueryRes).then(async () => {
                        syncLogInsertionQuery = syncLogInsertionQuery + "(" + tempBlock + ",0,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)";
                        await sql(syncLogInsertionQuery);
                        if (dataInsertionQueryRes.length != 0) {
                            console.log("NFT Data insertion done");
                        }
                    }).catch(async (err) => {
                        syncLogInsertionQuery = syncLogInsertionQuery + "(" + tempBlock + ",1,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)";
                        await sql(syncLogInsertionQuery);
                        console.log("Error in insertion of NFT data", err);
                    });
                }
                if (EndBlock == currentBlockNumber) {
                    EndBlock = currentBlockNumber + 1;
                } else {
                    EndBlock = ((EndBlock + 2000) > currentBlockNumber ? currentBlockNumber : (EndBlock + 2000));
                }
            }
        }
    } catch (err) {
        console.log("Blockchain Data Fetching Error: ", err);
    }
    process.exit(1);
})();

