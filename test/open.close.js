let should = require('chai').should()
  , assert = require('chai').assert
  , [AsyncWaterfall, AsyncApply] = [require('async/waterfall'), require('async/apply')]
  , Datastore = require('../nedb/datastore');

let closeDb = 'workspace/close.db';

describe('Database', function () {
  it('Can open and close cleanly', function(done) {
    let db = new Datastore({filename: closeDb, autoload: true}, function() { });
    db.filename.should.equal(closeDb);
    
    db.inMemoryOnly.should.equal(false);
    
    db.insert({ somedata: 'ok' }, function(err) { 
      assert.isNull(err);
      
      db.closeDatabase(function(err) {
        db.insert({ somedata: 'ok' }, function(err) {
          err.message.should.equal("Attempting operation on closed database.");
        });
        
        try {
          db.insert({ somedata: 'ok' });
        } catch (e) {
          e.message.should.equal("Attempting operation on closed database.");
        }  
        done();
      });      
    });
  });
  
  it('Can reopen a closed database', function(done) {
    let db = new Datastore({filename: closeDb, autoload: true}, function() { });
    db.find({}, function(err, docs) {
      assert.isNull(err, 'There were no errors');
      assert.isNotNull(docs, 'A result was returned');
      assert.isAbove(docs.length, 0, 'Some results exist');
      assert.isDefined(docs[0].somedata, 'somedata has been defined');
      docs[0].somedata.should.equal('ok');  
      db.closeDatabase(function(err) {
        done();
      });
    });
  });
  
  it('Can open multiple databases, and then close them again', function(done) {
    let multiOne = new Datastore({filename: 'workspace/multiOne.db', autoload: true}, function() {});
    let multiTwo = new Datastore({filename: 'workspace/multiTwo.db', autoload: true}, function() {});
    let multiThree = new Datastore({filename: 'workspace/multiThree.db', autoload: true}, function() {});
    let multiFour = new Datastore({filename: 'workspace/multiFour.db', autoload: true}, function() {});
    let multiFive = new Datastore({filename: 'workspace/multiFive.db', autoload: true}, function() {});
    let multiSix = new Datastore({filename: 'workspace/multiSix.db', autoload: true}, function() {});
    let multiSeven = new Datastore({filename: 'workspace/multiSeven.db', autoload: true}, function() {});
    let multiEight = new Datastore({filename: 'workspace/multiEight.db', autoload: true}, function() {});
    let multiNine = new Datastore({filename: 'workspace/multiNine.db', autoload: true}, function() {});
    let multiTen = new Datastore({filename: 'workspace/multiTen.db', autoload: true}, function() {});
    
    multiOne.closeDatabase(function(err) {});
    multiTwo.closeDatabase(function(err) {});
    multiThree.closeDatabase(function(err) {});
    multiFour.closeDatabase(function(err) {});
    multiFive.closeDatabase(function(err) {});
    multiSix.closeDatabase(function(err) {});
    multiSeven.closeDatabase(function(err) {});
    multiEight.closeDatabase(function(err) {});
    multiNine.closeDatabase(function(err) {});
    multiTen.closeDatabase(function(err) {
      done();
    });
  });
});