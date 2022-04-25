const MongoClient = require('mongodb').MongoClient;
const geoDist = require("./geoDist")
let collections = {
    raneLogsHistory: 'Log_Rane_History',
    raneLogsLive: 'Log_Rane_Live'
};
let dbCredentials = {
    localHost: "mongodb://baas_dev_pnc_user:kCxagTLm0xUnvSvXEYWQj44X7wYv7Wxn@api-dev.lime.ai:27017/",
    dbName: "baas_dev_pnc",
};
function insertSorted(arr, item, comparator) {
    if (comparator == null) {
        // emulate the default Array.sort() comparator
        comparator = function (a, b) {
            if (typeof a !== 'string') a = String(a);
            if (typeof b !== 'string') b = String(b);
            return (a > b ? 1 : (a < b ? -1 : 0));
        };
    }

    // get the index we need to insert the item at
    var min = 0;
    var max = arr.length;
    var index = Math.floor((min + max) / 2);
    while (max > min) {
        if (comparator(item, arr[index]) < 0) {
            max = index;
        } else {
            min = index + 1;
        }
        index = Math.floor((min + max) / 2);
    }

    // insert the item
    arr.splice(index, 0, item);
};

async function getBatData(db, options) {
    let data = await db.collection(collections.raneLogsHistory).aggregate(options,
        { allowDiskUse: true }).toArray();
    return data;
}

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
        let batteries = await db.collection(collections.raneLogsHistory).distinct("imei");
        let temp = [];
        for (let index = 0; index < batteries.length; index++) {
            let options = [
                { $project: projection },
                { $match: { imei: batteries[index] } },
                {
                    $group: {
                        _id: { imei: "$imei", date: "$date" },
                        docs: { $push: "$$ROOT" }
                    }
                }
            ]
            temp.push(getBatData(db, options));
            if (temp.length == 1000) {
                let data = await Promise.all(temp);
                for (let index = 0; index < data.length; index++) {
                    if (data[index].length == 1) {
                        let dayWiseData = await dayWiseCalc(data[index][0]);
                        const query = { imei: dayWiseData.imei, date: dayWiseData.date };
                        const update = { $set: dayWiseData };
                        const options = { upsert: true };
                        db.collection("rane_day_wise_data").updateOne(query, update, options);
                    }
                    else {
                        for (let inner = 0; inner < data[index].length; inner++) {
                            let dayWiseData = await dayWiseCalc(data[index][inner]);
                            const query = { imei: dayWiseData.imei, date: dayWiseData.date };
                            const update = { $set: dayWiseData };
                            const options = { upsert: true };
                            db.collection("rane_day_wise_data").updateOne(query, update, options);
                        }
                    }
                }
                temp = [];
            }
        }
        let data = await Promise.all(temp);
        for (let index = 0; index < data.length; index++) {
            if (data[index].length == 1) {
                let dayWiseData = await dayWiseCalc(data[index][0]);
                const query = { imei: dayWiseData.imei, date: dayWiseData.date };
                const update = { $set: dayWiseData };
                const options = { upsert: true };
                db.collection("rane_day_wise_data").updateOne(query, update, options);
            }
            else {
                for (let inner = 0; inner < data[index].length; inner++) {
                    let dayWiseData = await dayWiseCalc(data[index][inner]);
                    const query = { imei: dayWiseData.imei, date: dayWiseData.date };
                    const update = { $set: dayWiseData };
                    const options = { upsert: true };
                    db.collection("rane_day_wise_data").updateOne(query, update, options);
                }
            }
        }

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