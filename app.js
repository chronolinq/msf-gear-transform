const MongoClient = require('mongodb').MongoClient;
const assert = require('assert');

// Import the geat data fresh. This is from msf.gg
// mongoimport --db msf --collection raw_gear --file raw_data\gear.json

// Import the character gear data
// mongoimport --db msf --collection character_gear --type csv --headerline --file raw_data\M3GearTiers.csv

// Connection URL
const url = 'mongodb://localhost:27017/msf';

// Database Name
const dbName = 'msf';

// Use connect method to connect to the server
MongoClient.connect(url, { useNewUrlParser: true }, function (err, client)
{
    assert.equal(null, err);
    console.log("Connected successfully to server");

    const db = client.db(dbName);

    let initializePromise = initialize(db);

    initializePromise.then(function (result)
    {
        console.log("dropped \"gear\" collection");

        transformData(db, function ()
        {
            client.close();
            console.log("db connection closed");
            process.exit();
        });
    });
});

// Make sure that the gear table is cleared before we transforma and write into it
var initialize = function (db)
{
    return new Promise(function (resolve, reject)
    {
        let gearCollection = db.collection("gear");
        gearCollection.deleteMany({}).then(function ()
        {
            console.log("cleared \"gear\" collection");
            resolve();
        });
    });
}

var rawGear = {};
var currentParentTransformedGear = {};
var transformedDict = {};

const transformData = function (db, callback)
{
    // Get the documents collection
    const collection = db.collection('raw_gear');

    // Find some documents
    collection.find({}).toArray(function (err, docs)
    {
        assert.equal(err, null);

        // Return here if we don't have the data
        if (!docs || docs.length !== 1)
        {
            callback();
            return;
        }

        rawGear = docs[0];
        let transformedGear = [];

        // Loop through the raw gear collection and transform into the items that we need
        for (let rawGearKey in rawGear)
        {
            if (rawGear.hasOwnProperty(rawGearKey) && rawGearKey !== "_id")
            {
                let gearType = rawGear[rawGearKey];
                let applyPrefix = Object.keys(gearType).length > 1;

                for (let gearTypeKey in gearType)
                {
                    if (gearType.hasOwnProperty(gearTypeKey))
                    {
                        let gearItem = gearType[gearTypeKey];

                        let transformed = {
                            "gear_id": applyPrefix === true ? `T${gearTypeKey}_${rawGearKey}` : rawGearKey,
                            "msf_gg_id": rawGearKey,
                            "msf_gg_tier": gearTypeKey,
                            "display_name": gearItem.name,
                            "pic": gearItem.pic,
                            "stats": parseNumbers(gearItem.stats),
                            "slots": parseNumbers(gearItem.slots),
                            "cost": parseInt(gearItem.slots.cost, 10),
                            "total_cost": 0,
                            "gear_materials": [],
                            "is_material": gearItem.slots.p1_ID === "" && gearItem.slots.p2_ID === "" && gearItem.slots.p3_ID === "" && applyPrefix !== true,
                            "is_final": applyPrefix === true
                        };

                        transformedGear.push(transformed);
                        transformedDict[rawGearKey] = transformed;
                    }
                }
            }
        }

        for (let i = 0; i < transformedGear.length; i++)
        {
            let currentTransformedGear = transformedGear[i];
            currentParentTransformedGear = currentTransformedGear;

            traverseGearRequirements(currentTransformedGear);
        }

        const newCollection = db.collection('gear');

        newCollection.insertMany(transformedGear, function (err, result)
        {
            assert.equal(err, null);
            console.log(`Inserted ${result.ops.length} documents into the collection`);
            callback();
        });
    });
}

// Traverse the gear items to get the raw materials for them and record them onto the finished gear piece
const traverseGearRequirements = function (currentTransformedGear, currentGearItemCount = 1)
{
    // If this is material, then we don't need to traverse this
    if (currentTransformedGear.is_material === true)
    {
        return;
    }

    // Add the to the total cost
    currentParentTransformedGear.total_cost += currentTransformedGear.cost;

    let slots = [];

    // Get each of the slots
    let slot1 = { name: currentTransformedGear.slots["p1_ID"], count: currentTransformedGear.slots["p1_Count"] * currentGearItemCount };
    let slot2 = { name: currentTransformedGear.slots["p2_ID"], count: currentTransformedGear.slots["p2_Count"] * currentGearItemCount };
    let slot3 = { name: currentTransformedGear.slots["p3_ID"], count: currentTransformedGear.slots["p3_Count"] * currentGearItemCount };

    if (slot1.name !== "")
    {
        slots.push(slot1);
    }

    if (slot2.name !== "")
    {
        slots.push(slot2);
    }

    if (slot3.name !== "")
    {
        slots.push(slot3);
    }

    for (let i = 0; i < slots.length; i++)
    {
        let currentSlot = slots[i];
        let gearFromDict = transformedDict[currentSlot.name];

        if (gearFromDict.is_material !== true)
        {
            traverseGearRequirements(gearFromDict, currentSlot.count);
            continue;
        }

        let gearMats = currentParentTransformedGear.gear_materials;
        let found = false;
        
        for (let i = 0; i < gearMats.length; i++)
        {
            let currentGearMat = gearMats[i];

            if (currentGearMat.gear_id === currentSlot.name)
            {
                currentGearMat.count = currentGearMat.count + currentSlot.count;
                found = true;
            }
        }

        if (!found)
        {
            gearMats.push({
                gear_id: gearFromDict.gear_id,
                display_name: gearFromDict.display_name,
                count: currentSlot.count
            })
        }
    }
}

// Parse strings that look like numbers into numbers
const parseNumbers = function (item)
{
    let transformedItem = {};

    for (let key in item)
    {
        if (item.hasOwnProperty(key) && key !== "cost")
        {
            let itemValue = item[key];
            let parsedValue = parseInt(itemValue, 10);

            if (isNaN(parsedValue))
            {
                transformedItem[key] = itemValue;
            }
            else
            {
                transformedItem[key] = parsedValue;
            }
        }
    }

    return transformedItem;
}