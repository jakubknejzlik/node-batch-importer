var assert = require('assert');
var BatchImporter = require('../index.js')

var bi = new BatchImporter()

bi.addConnection('source','mysql','mysql://root@localhost/source-table')
bi.addConnection('dest','mysql','mysql://root@localhost/destination-table')

//bi.logTransferProgress = true;

describe('test suite',function(){

    it('should truncate data',function(done){
        bi.fetchData('dest','TRUNCATE __test',done)
    })
    it('test',function(done){
        bi.fetchData('dest','INSERT INTO log (date) VALUES (-123);SHOW WARNINGS',function(err,results){
            assert(err != null);
            done();
        })
    })

    it('should import data',function(done){
        bi.transferData('source','dest.__test','SELECT * FROM __test',function(err){
            if(err)return done(err);
            done();
        });
    })

    it('should have equal row count',function(done){
        bi.fetchData('source','SELECT COUNT(*) FROM __test',function(err,results){
            if(err)return done(err);
            bi.fetchData('dest','SELECT COUNT(*) FROM __test',function(err,results2){
                if(err)return done(err);
                assert.equal(results[0][0],results2[0][0]);
                done();
            })
        })
    })
})