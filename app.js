const MongoClient = require('mongodb').MongoClient;
const geoDist = require("./geoDist")
let collections = {
    raneLogsHistory: 'Log_Rane_History',
    raneLogsLive: 'Log_Rane_Live'
};
let dbCredentials = {
    localHost: "mongodb://api-dev.lime.ai:27017/",
    dbName: "db",
};

MongoClient.connect(dbCredentials.localHost, { useNewUrlParser: true, useUnifiedTopology: true },
    async function (err, client) {
        var db = client.db(dbCredentials.dbName);
        let projection = {
            imei: 1,
            gpsValidity: 1,
            date: 1,
            time: 1,
            lat: 1,
            long: 1,
            cummulativeCharge: 1,
            cummulativeDischarge: 1,
            numOfTempSens: 1,
            batTemp1: 1,
            batTemp2: 1,
            batTemp3: 1,
            batTemp4: 1

        }
        let data = await db.collection(collections.raneLogsHistory).aggregate([
            { $project: projection },
            {
                $group: {
                    _id: { imei: "$imei", date: "$date" },
                    docs: { $push: "$$ROOT" }
                }
            }
        ],
            { allowDiskUse: true }).toArray();
        let allResult = [];
        for (let index = 0; index < data.length; index++) {
            let dayWiseData = await dayWiseCalc(data[index]);
            const query = { imei: dayWiseData.imei, date: dayWiseData.date };
            const update = { $set: dayWiseData };
            const options = { upsert: true };
            allResult.push(db.collection("rane_day_wise_data").updateOne(query, update, options));


        }
        await Promise.all(allResult);
        console.log("Completed");


    });

function getAvgTemp(data) {
    let avgTemp = 0;
    switch (data.numOfTempSens) {
        case 1:
            avgTemp = data.batTemp1
            break;
        case 2:
            avgTemp = (data.batTemp1 + data.batTemp2) / 2;
            break;
        case 3:
            avgTemp = (data.batTemp1 + data.batTemp2 + data.batTemp3) / 3;
            break;
        case 4:
            avgTemp = (data.batTemp1 + data.batTemp2 + data.batTemp3 + data.batTemp4) / 4;
            break;
        default:
            avgTemp = (data.batTemp1 + data.batTemp2 + data.batTemp3 + data.batTemp4) / 4;
            break;
    }
    return avgTemp;
}

async function dayWiseCalc(data) {
    let distance = 0;
    data.docs = data.docs.sort((a, b) => {
        return a.time > b.time ? 1 : -1;
    });
    let prevDoc = data.docs[0];
    let initTemp = getAvgTemp(prevDoc);
    let minTemp = initTemp;
    let maxTemp = initTemp;
    for (let index = 1; index < data.docs.length; index++) {
        let temp = getAvgTemp(data.docs[index]);
        if (temp < minTemp) {
            minTemp = temp;
        }
        if (temp > maxTemp) {
            maxTemp = temp;
        }
        if (data.docs[index].gpsValidity == "A") {
            let dist = geoDist.getDistanceFromLatLon({
                lat1: prevDoc.lat,
                lon1: prevDoc.long,
                lat2: data.docs[index].lat,
                lon2: data.docs[index].long,
            });
            prevDoc = data.docs[index];
            distance += dist;

        }
    }
    let ahIn = data.docs[data.docs.length - 1].cummulativeCharge - data.docs[0].cummulativeCharge;
    let ahout = data.docs[data.docs.length - 1].cummulativeDischarge - data.docs[0].cummulativeDischarge;
    let eneryIn = ahIn * 50.4;
    let eneryOut = ahout * 50.4;
    let mileage = 0;
    if (distance > 0 && eneryOut > 0) {
        mileage = distance / (eneryOut * 1000);
    }
    return {
        distance,
        ahIn,
        ahout,
        eneryIn,
        eneryOut,
        mileage,
        minTemp,
        maxTemp,
        date: data._id.date,
        imei: data._id.imei
    };

}