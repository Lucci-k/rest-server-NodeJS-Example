const express = require('express');
const bodyParser = require('body-parser');
const MongoClient = require('mongodb').MongoClient;
require('custom-env').env()


/*
    ************* express, body-parser & mongodb setup *************
*/

const app = express();
app.use(bodyParser.urlencoded({extended: true}));
app.use(express.static('public'));
app.use(bodyParser.json());

const uri = "mongodb+srv://" + process.env.DB_USER + ":" + process.env.gitDB_PASS + "@cluster-mock-market.mpksa.mongodb.net/sample_airbnb?retryWrites=true&w=majority";
const client = new MongoClient(uri);

// Change database and collection here name here!
const dbName = "sample_airbnb";
const collectionName = "listingsAndReviews"

// Used to retrieve database and collection after client connection is made
// Created function just to refactor and make code more concise
function getCollection(dbName, collectionName) {
    const db = client.db(dbName);
    const collection = db.collection(collectionName);
    return collection
}



/*
    ************* CRUD OPERATIONS *************
*/

// http://localhost:3000/getListingByPropertyType/?property_type=House
app.get('/getListingByPropertyType/', async (req, res) => {
    try {
        const propertyType = req.query.property_type
        console.log(propertyType)
        await client.connect();
        console.log("Connected correctly to server");
        // Find one document
        const doc = await getCollection(dbName, collectionName).findOne( { 
            property_type: propertyType 
        }, { 
            projection : {
                name: 1,
                 _id: 0} 
                } );
        // Print to the console
        res.status(200).send(doc)
    } catch (err) {
        console.log(err)
        res.status(400).send('Document does not exist or check query string');
     } 
})

// http://localhost:3000/postListing/?property_type=House&name=My+Custom+Listing
app.post('/postListing/', async (req, res) => {
    try {
        const name = req.query.name
        const propertyType = req.query.property_type
        console.log(name, propertyType)
        await client.connect();
        console.log("Connected correctly to server");
        // Post document
        await getCollection(dbName, collectionName).insertOne( {
            name: name,
            property_type: propertyType
        })
        res.status(200).send('Successfully inserted document to database.')
    } catch (err) {
        console.log(err)
        res.status(400).send('There was an error, document could not be posted. Check query string')
    }
})

// http://localhost:3000/updateListingNotes/?name=My+Custom+Listing&notes=This+is+a+test+listing+please+delete
app.put('/updateListingNotes/', async (req, res) => {
    try {
        const name = req.query.name
        const notes = req.query.notes
        console.log(name, notes) 
        await client.connect();
        console.log("Connected correctly to the server")
        // Update document
        await getCollection(dbName, collectionName).updateOne({name: name}, {$set: {notes: notes}})
        res.status(200).send('Successfully updated document.')
    } catch(err) {
        console.log(err)
        res.status(400).send('There was an error, document could not be updated. Check query string')
    }
})

// http://localhost:3000/deleteListingByName/?name=My+Custom+Listing
app.delete('/deleteListingByName/', async (req, res) => {
    try {
        const name = req.query.name
        console.log(name) 
        await client.connect();
        console.log("Connected correctly to the server")
        // Delete document
        await getCollection(dbName, collectionName).deleteOne({name: name})
        res.status(200).send('Successfully deleted document.')
    } catch(err) {
        console.log(err)
        res.status(400).send('There was an error, document could not be deleted. Check query string')
    }
})


/*
    ************* Pipeline & Aggregation Operations *************
*/

// http://localhost:3000/getTopRatedListingsWithLimits/?limit=5
app.get('/getTopRatedListingsWithLimits/', async (req, res) => {
    try {
        const limit = Number(req.query.limit)
        const pipeline = [
            {
              '$match': {
                'review_scores.review_scores_rating': {
                  '$gt': 90
                }
              }
            }, {
              '$project': {
                '_id': 0, 
                'name': 1, 
                'description': 1, 
                'review_scores.review_scores_rating': 1
              }
            }, {
              '$sort': {
                'review_scores.review_scores_rating': -1
              }
            }, {
              '$limit': limit
            }
          ]
        await client.connect();
        let listingsArray = [];
        const aggCursor = getCollection(dbName, collectionName).aggregate(pipeline);
        // iterate through aggregate cursor object, push to listings array
        // not doing this cuased an cursor object reference error
        await aggCursor.forEach((listing) => {
            listingsArray.push(listing)
        })
        res.status(200).send(listingsArray);
    } catch(err) {
        console.log(err)
        res.status(400).send(err)
    }
})

// http://localhost:3000/getAvgPriceByCountryMarketWithLimits/?country=Australia&market=Sydney&limit=5
app.get('/getAvgPriceByCountryMarketWithLimits/', async (req, res) => {
    try {
        const country = req.query.country
        const market = req.query.market
        const limit = Number(req.query.limit)
        const pipeline = [
            {
              '$match': {
                'bedrooms': 1, 
                'address.country': country, 
                'address.market': market
              }
            }, {
              '$project': {
                '_id': 0, 
                'name': 1, 
                'description': 1, 
                'price': 1, 
                'address.country': 1, 
                'address.market': 1
              }
            }, {
                '$sort': {
                'averagePrice': -1
              }
            }, {
              '$limit': limit
            }
          ]
        await client.connect();
        let listingsArray = [];
        const aggCursor = getCollection(dbName, collectionName).aggregate(pipeline);
        // iterate through aggregate cursor object, push to listings array
        // not doing this cuased an cursor object reference error
        await aggCursor.forEach((listing) => {
            listingsArray.push(listing)
        })
        res.status(200).send(listingsArray);
    } catch(err) {
        console.log(err)
        res.status(400).send(err)
    }
})

app.listen(3000, () => {
    console.log('Server is running on port 3000')
})