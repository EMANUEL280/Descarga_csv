const MongoClient = require('mongodb');
const url = 'mongodb://ijardines:uJ7NJjCbvz9fS32SW4QC@10.3.18.93:27017/?authSource=Vsys_ms'

let aggregate = function (db, collection, pipeline) {
  return new Promise((resolve, reject) => {
    db.collection(collection).aggregate(pipeline).toArray((error, docs) => {
      if (error) {
        console.log(error)
      }
      resolve(docs);
    });
  });
}
let callback = async function (error, database) {
  if (error) { console.log(error) }
  console.log('Connection is okay');
  const db = database.db('Vsys_ms');//NOMBRE DE LA BASE 
  let collection = "IOM_Vsys";//COLECCIÓN
  //let expression = new RegExp ("Prueba","i") //Expresiones Regulares
  let pipeline = [
    {
      $match:
      {
          service: "CUAD-AVAYA",
          status: "Activo"
      }
  },
  {
      $unwind: "$services"
  },
  {    
       $project:
   {
       _id: 0,
       folio: 1,
       oa: 1,
       siteNumber: 1,
       siteName: 1,
       phase: 1,
       status: 1,
       clientName: 1,
       sku: "$services.sku",
       partId: "$services.partId",
       qty: "$services.qty"
   }
},
{
   $lookup:
   {
       from: "invoicedCUAD",
       localField: "partId",
       foreignField: "partId",
       as: "true"
}
},
{
$lookup:
{
    from: "invoicedCUAD",
    let: {partFac: "$partId"},
    pipeline: [
        {
            $match:
            {
                $and:[
                    {period: { $gte:new Date("2021-02-01T00:00:00.000-06:00")}},
                    {
                         $expr:{
                             $eq:["$partId", "$$partFac"]
                         }   
                    }
                    ]
            }
        },
        ],  
        as: "true2"
}
},
  {    
       $project:
   {
       _id: 0,
       folio: 1,
       status: 1,
       clientName: 1,
       sku: 1,
       partId: 1,
       qty: 1,
       qtyFac: { 
       $reduce: {
         input: "$true2.fac_qty",
         initialValue: [],
         in: { $sum : ["$$value", "$$this"] }
     }
 },
}
}
  ]
  let docs = await aggregate(db, collection, pipeline);

  let pipeline2 = [

  ];
  let users = await aggregate(db, "logsUsersBsft", pipeline2);//COLECCIÓN 

  //console.log(users);
  //findDocuments(db, () => {
  console.log('Successful query');

  const { Parser } = require('json2csv')
  var fs = require('fs');
  var fields = ['folio','clientName', 'status', 'sku', 'partId', 'qty', 'qtyFac']; //COLUMNAS
  const json2csvParser = new Parser({ fields });
  const csv = json2csvParser.parse(docs); //Nombre de la variable de los datos
  //console.log(csv);

  fs.writeFile('cuad_Vsys_Avaya.1.csv', csv, function (err) {//nombre del doc
    if (err) throw err;

  });
};

MongoClient.connect(url,{ useNewUrlParser: true }, callback);

/*/MongoClient.connect(url, (error, database) => {
  if (error) {console.log(error)}
  console.log('Connection is okay');
  const db = database.db('Vsys_ms');//NOMBRE DE LA BASE
  db.collection("groups").aggregate // NOMBRE DE LA COLECCIÓN
  (
    [
      {
        $project:
        {
            groupId:1,
            siteNumber:1,
            _id: 0
        }
      }
    ]
  ).toArray((error,docs)=>{
    if (error) {
      console.log (error)
    }
    console.log (docs)
  })
  //findDocuments(db, () => {
  console.log('Successful query');
  });/*/
