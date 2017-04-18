import Redis from 'ioredis'
import kue from 'kue'
import express from 'express'
import bodyParser from 'body-parser'
import MongoClient from 'mongodb'
import ConfRedis from './src/redis-cluster'

const app = express()
const limitPush = 3
var path = 'HOST:PORT'
var dbname = 'DBNAME'
var db

MongoClient.connect(`mongodb://${path}/${dbname}`, (err, database) => {
if (err)return console.log(err)
    db = database
})

kue.app.listen(8888); 
const queue = kue.createQueue( {
  prefix:'q', 
  redis: {
      createClientFactory:function () {
        return new Redis.Cluster(ConfRedis); 
      }
    }
});

app.use(bodyParser.json())
app.route('/push-queue')
    .post((req, res) =>  {
        const topicId = req.body.topic_id
        if(typeof topicId == "undefined"){
            res.status(500).send( {message:`topicId required`})
        }
        
        db.collection('collection').findOne( {'_id':topicId }, (err, doc) =>  {
            if (doc != null) {
                let arr = []
                let cnt = doc.followers.length
               
                doc.followers.forEach((v, k) =>  {
                    
                   arr.push(v)
                   
                   if (arr.length == limitPush) {
                        console.log( arr); 

                        queue.create('push-queue',  {
                                app_id : 1,
                                ids :arr,
                               
                        }).attempts(10).save()

                        arr = []
                   }

                   if(k+1 == cnt ){
                        console.log(arr)
                        queue.create('push-queue',  {
                                app_id : 1,
                                ids :arr,
                              
                        }).attempts(10).save()
                        arr = [];
                   }
                 
                })
                res.json({ status: true })

            }else {
                res.status(404).send( {message:`Not Found topic id ${topicId}`})
            }
        })
    })



const server = app.listen(7755, () =>  {

  var port = server.address().port
  console.log("app listening port ", port)

})

queue.process('push-queue', 10, function(job, done) {
    console.log(job.data)
    done();
})

var interval = setInterval(function(){ 
  kue.Job.rangeByState( 'complete', 0, 10000, 'asc', function( err, jobs ) {
    jobs.forEach( function( job ) {
      job.remove( function(){
        //console.log( 'removed ', job.id );
      });
    });
  });
}, 30000);

