import kue from 'kue'
import Redis from 'Redis'
import ConfRedis from './src/redis-cluster'

const queue = kue.createQueue( {
  prefix:'q', 
  redis: {
      createClientFactory:function () {
        return new Redis.Cluster(ConfRedis); 
      }
    }
}); 

kue.app.listen(9898); 